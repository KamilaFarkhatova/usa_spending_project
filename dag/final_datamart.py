"""Построить аналитические витрины из ODS за logical date."""

import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from _lib.tenant import student_dag


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNNER = PROJECT_ROOT / "run" / "run_pipeline.sh"
sys.path.insert(0, str(PROJECT_ROOT))

LOGIN = "farkhatova"
CONN_ID = f"aiacademy_pg_farkhatova"
STG_SCHEMA = f"{LOGIN}_stg"
DEFAULT_ARGS = {
    "owner": LOGIN,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def connection_environment():
    connection = BaseHook.get_connection(CONN_ID)
    if not all((connection.host, connection.login, connection.schema)):
        raise ValueError(
            f"В Airflow Connection {CONN_ID} заполните host, login и database."
        )

    password = connection.password
    if not password:
        raise ValueError(
            f"В Airflow Connection {CONN_ID} не задан пароль PostgreSQL."
        )

    env = os.environ.copy()
    env.update(
        {
            "PROJECT_LOGIN": LOGIN,
            "PGHOST": connection.host,
            "PGPORT": str(connection.port or 5432),
            "PGUSER": connection.login,
            "PGPASSWORD": "farkhatova_pass123",
            "PGDATABASE": connection.schema,
        }
    )
    return env


def clickhouse_environment(env):
    connection = BaseHook.get_connection(f"aiacademy_ch_{LOGIN}")
    if not all((connection.host, connection.login)):
        raise ValueError(
            f"В Airflow Connection aiacademy_ch_{LOGIN} заполните host и login."
        )

    secure = connection.extra_dejson.get("secure", False)
    secure = str(secure).lower() in {"1", "true", "yes"}
    env.update(
        {
            "CH_HOST": connection.host,
            "CH_PORT": str(connection.port or (8443 if secure else 8123)),
            "CH_USER": connection.login,
            "CH_PASSWORD": connection.password or "",
            "CH_SECURE": "true" if secure else "false",
        }
    )
    return env


with student_dag(
    __file__,
    dag_id="final_datamart",
    schedule=None,
    start_date=datetime(2024, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=[LOGIN, "final", "datamart"],
) as dag:
    @task(execution_timeout=timedelta(minutes=5))
    def build_marts(ds=None):
        env = connection_environment()
        try:
            with tempfile.TemporaryDirectory(prefix=f"{LOGIN}-dbt-") as work_dir:
                dbt_env = {
                    **env,
                    "DBT_LOG_PATH": str(Path(work_dir) / "logs"),
                    "DBT_TARGET_PATH": str(Path(work_dir) / "target"),
                }
                result = subprocess.run(
                    ["bash", str(RUNNER), "marts", ds],
                    cwd=PROJECT_ROOT,
                    env=dbt_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            if result.stdout:
                logging.getLogger(__name__).info("Вывод сборки витрин:\n%s", result.stdout.rstrip())
            result.check_returncode()
        except Exception as error:
            try:
                from ingestion.common import log_fail_running

                with patch.dict(os.environ, env):
                    log_fail_running(STG_SCHEMA, dag.dag_id, ds, "dm_", error)
            except Exception:
                pass
            raise

    @task(execution_timeout=timedelta(minutes=5))
    def publish_clickhouse(ds=None):
        env = clickhouse_environment(connection_environment())
        subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "tools" / "publish_clickhouse.py"),
             "--ds", ds],
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
        )

    build_task = build_marts()
    clickhouse_task = publish_clickhouse()
    build_task >> clickhouse_task

