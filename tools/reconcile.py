"""Сверить CSV -> STG -> ODS -> витрины -> ClickHouse за одну дату."""

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion import common
from publish_clickhouse import TABLE as CLICKHOUSE_TABLE
from publish_clickhouse import clickhouse_client


EVENTS_CSV = ROOT / "data" / "federal_transactions.csv"
REFERENCE_CSV = ROOT / "data" / "award_type_reference.csv"


def expected_source_counts(ds):
    with EVENTS_CSV.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required = {"award_id_piid", "action_date"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"В {EVENTS_CSV.name} отсутствуют нужные колонки.")
        events = [row for row in reader if row["action_date"][:10] == ds.isoformat()]
    award_ids = {
        row["award_id_piid"].strip()
        for row in events
        if row["award_id_piid"].strip()
    }

    with REFERENCE_CSV.open(encoding="utf-8-sig", newline="") as source:
        reference_count = sum(1 for _ in csv.DictReader(source))
    return len(events), reference_count, award_ids


def require_equal(label, expected, actual):
    if expected != actual:
        raise ValueError(f"{label}: ожидалось {expected}, получено {actual}.")
    print(f"OK {label}: {actual}")


def latest_log_status(conn, stg_schema, task_id, ds):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT status FROM {}.etl_load_log "
                "WHERE task_id = %s AND ds = %s "
                "ORDER BY id DESC LIMIT 1"
            ).format(sql.Identifier(stg_schema)),
            (task_id, ds),
        )
        row = cur.fetchone()
    return row[0] if row else None


def run(ds):
    login = common.project_login()
    stg_schema = f"{login}_stg"
    ods_schema = f"{login}_ods"
    datamart_schema = f"{login}_datamart"
    expected_events, expected_reference, award_ids = expected_source_counts(ds)

    conn = common.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.stg_federal_transactions WHERE _ds = %s"
                ).format(sql.Identifier(stg_schema)),
                (ds,),
            )
            stg_events = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.stg_award_type_reference WHERE _ds = %s"
                ).format(sql.Identifier(stg_schema)),
                (ds,),
            )
            stg_reference = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.stg_api_awards WHERE _ds = %s"
                ).format(sql.Identifier(stg_schema)),
                (ds,),
            )
            stg_api = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.ods_federal_transactions "
                    "WHERE action_date = %s"
                ).format(sql.Identifier(ods_schema)),
                (ds,),
            )
            ods_events = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT count(*) FROM {}.ods_awards WHERE award_id = ANY(%s)"
                ).format(sql.Identifier(ods_schema)),
                (list(award_ids),),
            )
            ods_awards = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT coalesce(sum(federal_action_obligation), 0) "
                    "FROM {}.ods_federal_transactions WHERE action_date = %s"
                ).format(sql.Identifier(ods_schema)),
                (ds,),
            )
            ods_total = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "SELECT count(*), coalesce(sum(net_action_obligation), 0) "
                    "FROM {}.dm_agency_daily_spending WHERE action_date = %s"
                ).format(sql.Identifier(datamart_schema)),
                (ds,),
            )
            mart_count, mart_total = cur.fetchone()

        require_equal("CSV → STG events", expected_events, stg_events)
        require_equal("CSV → STG reference", expected_reference, stg_reference)
        require_equal("CSV award IDs → STG API", len(award_ids), stg_api)
        require_equal("STG → ODS events", expected_events, ods_events)
        require_equal("API IDs → ODS awards", len(award_ids), ods_awards)
        require_equal("ODS → daily mart sum", ods_total, mart_total)
    finally:
        conn.close()

    client = clickhouse_client(login)
    try:
        target = f"{login}.{CLICKHOUSE_TABLE}"
        result = client.query(
            f"SELECT count(), coalesce(sum(net_action_obligation), 0) "
            f"FROM {target} WHERE action_date = toDate('{ds.isoformat()}')"
        ).result_set[0]
    finally:
        client.close()

    clickhouse_count, clickhouse_total = result
    require_equal("PostgreSQL → ClickHouse rows", mart_count, clickhouse_count)
    require_equal("PostgreSQL → ClickHouse sum", mart_total, clickhouse_total)

    required_tasks = (
        "load_federal_transactions",
        "load_award_type_reference",
        "load_api_awards",
        "ods_award_types",
        "ods_agencies",
        "ods_recipients",
        "ods_awards",
        "ods_federal_transactions",
        "dm_agency_daily_spending",
        "dm_recipient_analysis",
        "dm_monthly_awards_by_type",
        "copy_agency_daily_spending_to_clickhouse",
    )
    conn = common.connect()
    try:
        for task_id in required_tasks:
            status = latest_log_status(conn, stg_schema, task_id, ds)
            require_equal(f"ETL log {task_id}", "success", status)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ds", required=True, help="Дата запуска YYYY-MM-DD")
    args = parser.parse_args()

    try:
        run(date.fromisoformat(args.ds))
    except Exception as error:
        parser.exit(1, f"Сверка не пройдена: {error}\n")


if __name__ == "__main__":
    main()
