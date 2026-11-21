"""Скопировать дневную витрину PostgreSQL -> ClickHouse за один ds."""

import argparse
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import clickhouse_connect
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion import common


TABLE = "dm_agency_daily_spending"
COLUMNS = [
    "action_date",
    "agency_code",
    "agency_name",
    "transaction_count",
    "awards_count",
    "net_action_obligation",
    "built_at",
]


def clickhouse_client(login):
    required = ("CH_HOST", "CH_USER")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Не заданы параметры ClickHouse: " + ", ".join(missing))

    secure = os.environ.get("CH_SECURE", "false").lower() in {"1", "true", "yes"}
    port = int(os.environ.get("CH_PORT") or (8443 if secure else 8123))
    return clickhouse_connect.get_client(
        host=os.environ["CH_HOST"],
        port=port,
        username=os.environ["CH_USER"],
        password=os.environ.get("CH_PASSWORD", ""),
        secure=secure,
    )


def prepare_table(client, login):
    client.command(f"CREATE DATABASE IF NOT EXISTS {login}")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {login}.{TABLE} (
            action_date Date,
            agency_code String,
            agency_name String,
            transaction_count UInt64,
            awards_count UInt64,
            net_action_obligation Decimal(20, 2),
            built_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree
        ORDER BY (action_date, agency_code)
        """
    )


def load_rows(ds):
    login = common.project_login()
    ods_schema = f"{login}_ods"
    datamart_schema = f"{login}_datamart"
    conn = common.connect()
    try:
        with conn.cursor() as cur:
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
                    "SELECT action_date, agency_code, agency_name, "
                    "transaction_count, awards_count, net_action_obligation, "
                    "built_at FROM {}.{} WHERE action_date = %s "
                    "ORDER BY agency_code"
                ).format(
                    sql.Identifier(datamart_schema), sql.Identifier(TABLE)
                ),
                (ds,),
            )
            rows = cur.fetchall()

        mart_total = sum((row[5] for row in rows), Decimal("0.00"))
        if ods_total != mart_total:
            raise ValueError(
                f"Сумма витрины PostgreSQL ({mart_total}) не совпадает "
                f"с ODS ({ods_total}) за {ds}."
            )
        return rows, ods_total
    finally:
        conn.close()


def run(ds):
    login = common.project_login()
    stg_schema = f"{login}_stg"
    dag_id = f"final_{login}_datamart"
    target = f"{login}.{TABLE}"
    load_id = common.log_start(
        stg_schema,
        dag_id,
        "copy_agency_daily_spending_to_clickhouse",
        "postgresql",
        target,
        ds,
    )

    client = None
    try:
        rows, ods_total = load_rows(ds)
        client = clickhouse_client(login)
        prepare_table(client, login)
        client.command(
            f"ALTER TABLE {target} DELETE "
            f"WHERE action_date = toDate('{ds.isoformat()}') "
            "SETTINGS mutations_sync = 1"
        )
        if rows:
            client.insert(target, rows, column_names=COLUMNS)

        result = client.query(
            f"SELECT count(), coalesce(sum(net_action_obligation), 0) "
            f"FROM {target} WHERE action_date = toDate('{ds.isoformat()}')"
        ).result_set[0]
        clickhouse_count, clickhouse_total = result
        if clickhouse_count != len(rows) or clickhouse_total != ods_total:
            raise ValueError(
                "После копирования значения в PostgreSQL и ClickHouse "
                f"не совпали за {ds}: rows={clickhouse_count}/{len(rows)}, "
                f"sum={clickhouse_total}/{ods_total}."
            )
    except Exception as error:
        common.log_finish(stg_schema, load_id, "failed", 0, str(error))
        raise
    finally:
        if client is not None:
            client.close()

    common.log_finish(stg_schema, load_id, "success", len(rows))
    print(
        f"{target}, ds={ds}: строк {len(rows)}, "
        f"сумма {ods_total}; повторная загрузка заменяет эту дату."
    )
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ds", required=True, help="Дата запуска YYYY-MM-DD")
    args = parser.parse_args()

    try:
        run(date.fromisoformat(args.ds))
    except Exception as error:
        parser.exit(1, f"Ошибка копирования в ClickHouse: {error}\n")


if __name__ == "__main__":
    main()
