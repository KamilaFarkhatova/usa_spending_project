"""Запустить dbt-модель витрины и записать результат в журнал ETL."""

import argparse
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion import common


MODEL = "dm_monthly_awards_by_type"
TABLE = f"{common.project_login()}_datamart.{MODEL}"


def run(ds):
    login = common.project_login()
    stg_schema = f"{login}_stg"
    dag_id = f"final_{login}_datamart"
    load_id = common.log_start(
        stg_schema,
        dag_id,
        MODEL,
        "dbt",
        TABLE,
        ds,
    )

    env = os.environ.copy()
    env["PROJECT_LOGIN"] = login
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(ROOT / "dbt"),
        "--profiles-dir",
        str(ROOT / "dbt"),
        "--select",
        f"+{MODEL}",
    ]

    try:
        subprocess.run(command, cwd=ROOT, env=env, check=True)
        conn = common.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {TABLE}")
                rows_loaded = cur.fetchone()[0]
        finally:
            conn.close()
    except Exception as error:
        common.log_finish(stg_schema, load_id, "failed", 0, str(error))
        raise

    common.log_finish(stg_schema, load_id, "success", rows_loaded)
    print(f"{TABLE}: построено строк {rows_loaded}, ds={ds}")
    return rows_loaded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ds", required=True, help="Дата запуска YYYY-MM-DD")
    args = parser.parse_args()

    try:
        ds = date.fromisoformat(args.ds)
        run(ds)
    except Exception as error:
        parser.exit(1, f"Ошибка запуска dbt: {error}\n")


if __name__ == "__main__":
    main()
