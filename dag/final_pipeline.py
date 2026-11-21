"""Загрузка данных USAspending за одну logical date."""

import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _lib.tenant import student_dag


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNNER = PROJECT_ROOT / "run" / "run_pipeline.sh"
sys.path.insert(0, str(PROJECT_ROOT))

LOGIN = "farkhatova"

CONN_ID = f"aiacademy_pg_{LOGIN}"
STG_SCHEMA = f"{LOGIN}_stg"
ODS_SCHEMA = f"{LOGIN}_ods"


def connection_environment():
    connection = BaseHook.get_connection(CONN_ID)
    if not all((connection.host, connection.login, connection.schema)):
        raise ValueError(
            f"В Airflow Connection {CONN_ID} заполните host, login и database."
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


DEFAULT_ARGS = {
    "owner": LOGIN,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with student_dag(
    __file__,
    dag_id="final_pipeline",
    start_date=datetime(2024, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["final", LOGIN],
) as dag:

    @task(execution_timeout=timedelta(minutes=3))
    def load_events(ds=None):
        from ingestion.load_events import load

        with patch.dict(os.environ, connection_environment()):
            return load(ds, schema=STG_SCHEMA)

    @task(execution_timeout=timedelta(minutes=3))
    def load_reference(ds=None):
        from ingestion.load_reference import load

        with patch.dict(os.environ, connection_environment()):
            return load(ds, schema=STG_SCHEMA)

    @task(execution_timeout=timedelta(minutes=5))
    def load_api_awards(ds=None):
        from ingestion.load_api import run

        with patch.dict(os.environ, connection_environment()):
            run(date.fromisoformat(ds))

    @task(execution_timeout=timedelta(minutes=5))
    def transform_ods(ds=None):
        env = connection_environment()
        try:
            subprocess.run(
                ["bash", str(RUNNER), "ods", ds],
                cwd=PROJECT_ROOT,
                env=env,
                check=True,
            )
        except Exception as error:
            try:
                from ingestion.common import log_fail_running

                with patch.dict(os.environ, env):
                    log_fail_running(STG_SCHEMA, dag.dag_id, ds, "ods_", error)
            except Exception:
                pass
            raise

    events_task = load_events()
    reference_task = load_reference()
    api_task = load_api_awards()
    ods_task = transform_ods()

    [events_task, reference_task, api_task] >> ods_task

    datamart_task = TriggerDagRunOperator(
        task_id="trigger_datamart",
        trigger_dag_id=f"{LOGIN}.final_datamart",
        logical_date="{{ ts }}",
        conf={"ds": "{{ ds }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    ods_task >> datamart_task

