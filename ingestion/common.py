"""Общие функции для загрузчиков проекта."""

import csv
import io
import os
import re
from datetime import date

import psycopg2
from psycopg2 import sql


SERVICE_COLUMNS = ("_ds", "_source_file", "_loaded_at")


def project_login():
    login = os.environ.get("PROJECT_LOGIN", "")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", login):
        raise ValueError(
            "Укажите PROJECT_LOGIN: строчные латинские буквы, цифры и _; "
            "первый символ — буква."
        )
    return login


def schema_name(schema=None):
    name = schema or f"{project_login()}_stg"
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name):
        raise ValueError(f"Некорректное имя схемы: {name!r}")
    return name


def pipeline_dag_id():
    return f"final_{project_login()}_pipeline"


def connect():
    """Подключиться через PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE."""
    return psycopg2.connect(connect_timeout=10)


def table_columns(cur, schema, table):
    schema = schema_name(schema)
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", table):
        raise ValueError(f"Некорректное имя таблицы: {table!r}")
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    columns = [row[0] for row in cur.fetchall()]
    return [column for column in columns if column not in SERVICE_COLUMNS]


def need_table(cur, schema, table):
    columns = table_columns(cur, schema, table)
    if not columns:
        raise RuntimeError(
            f"Таблица {schema}.{table} не найдена. "
            "Сначала выполните: bash run/run_pipeline.sh ddl"
        )
    return columns


def log_start(stg_schema, dag_id, task_id, source, target_table, ds):
    stg_schema = schema_name(stg_schema)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.etl_load_log "
                    "(dag_id, task_id, source, target_table, ds) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id"
                ).format(sql.Identifier(stg_schema)),
                (dag_id, task_id, source, target_table, ds),
            )
            load_id = cur.fetchone()[0]
        conn.commit()
        return load_id
    finally:
        conn.close()


def log_finish(stg_schema, load_id, status, rows=0, error=None):
    stg_schema = schema_name(stg_schema)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {}.etl_load_log "
                    "SET status = %s, finished_at = now(), rows_loaded = %s, "
                    "error_text = %s WHERE id = %s"
                ).format(sql.Identifier(stg_schema)),
                (status, rows, (error or "")[:500] or None, load_id),
            )
        conn.commit()
    finally:
        conn.close()


def log_fail_running(stg_schema, dag_id, ds, task_prefix, error):
    stg_schema = schema_name(stg_schema)
    if not isinstance(ds, date):
        ds = date.fromisoformat(str(ds))
    if not task_prefix:
        raise ValueError("task_prefix не может быть пустым.")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {}.etl_load_log "
                    "SET status = 'failed', finished_at = now(), error_text = %s "
                    "WHERE dag_id = %s AND ds = %s AND status = 'running' "
                    "AND left(task_id, %s) = %s"
                ).format(sql.Identifier(stg_schema)),
                (
                    (str(error) or "SQL step failed")[:500],
                    dag_id,
                    ds,
                    len(task_prefix),
                    task_prefix,
                ),
            )
            failed_count = cur.rowcount
        conn.commit()
        return failed_count
    finally:
        conn.close()


def write_rows(conn, schema, table, columns, rows, ds, source_file, mode):
    schema = schema_name(schema)
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", table):
        raise ValueError(f"Некорректное имя таблицы: {table!r}")
    if mode not in {"delete_ds", "truncate"}:
        raise ValueError(f"Неизвестный режим загрузки: {mode!r}")

    expected = len(columns)
    if any(len(row) != expected for row in rows):
        raise ValueError(f"В {table} число значений в строке не совпадает с числом колонок.")

    table_id = sql.SQL("{}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    with conn.cursor() as cur:
        if mode == "delete_ds":
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE _ds = %s").format(table_id),
                (ds,),
            )
        else:
            cur.execute(sql.SQL("DELETE FROM {}").format(table_id))
        deleted = cur.rowcount

        if rows:
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            for row in rows:
                values = ["" if value is None else str(value) for value in row]
                writer.writerow(values + [ds.isoformat(), source_file])
            buffer.seek(0)

            copy_columns = columns + ["_ds", "_source_file"]
            copy_sql = sql.SQL(
                "COPY {} ({}) FROM STDIN WITH (FORMAT CSV)"
            ).format(
                table_id,
                sql.SQL(", ").join(sql.Identifier(column) for column in copy_columns),
            )
            cur.copy_expert(copy_sql.as_string(conn), buffer)

    conn.commit()
    return deleted, len(rows)
