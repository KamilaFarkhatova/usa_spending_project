"""Проверить INSERT, UPDATE и DELETE источника в ODS не дольше 30 секунд."""

import sys
import time
import uuid
from pathlib import Path

from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ingestion import common


TIMEOUT_SECONDS = 30
POLL_SECONDS = 1


def wait_for_value(conn, target_schema, recipient_id, expected):
    started_at = time.monotonic()
    while time.monotonic() - started_at < TIMEOUT_SECONDS:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT recipient_name FROM {}.ods_recipients "
                    "WHERE recipient_id = %s"
                ).format(sql.Identifier(target_schema)),
                (recipient_id,),
            )
            row = cur.fetchone()
        actual = row[0] if row else None
        if actual == expected:
            return round(time.monotonic() - started_at, 2)
        time.sleep(POLL_SECONDS)
    raise TimeoutError(
        f"CDC не доставил ожидаемое состояние за {TIMEOUT_SECONDS} секунд: "
        f"expected={expected!r}, actual={actual!r}."
    )


def run():
    login = common.project_login()
    source_schema = login
    target_schema = f"{login}_ods"
    recipient_id = "CDC_TEST_" + uuid.uuid4().hex[:16]
    conn = common.connect()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.recipients (recipient_id, recipient_name) "
                    "VALUES (%s, %s)"
                ).format(sql.Identifier(source_schema)),
                (recipient_id, "CDC test recipient v1"),
            )
        insert_seconds = wait_for_value(
            conn, target_schema, recipient_id, "CDC test recipient v1"
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {}.recipients SET recipient_name = %s "
                    "WHERE recipient_id = %s"
                ).format(sql.Identifier(source_schema)),
                ("CDC test recipient v2", recipient_id),
            )
        update_seconds = wait_for_value(
            conn, target_schema, recipient_id, "CDC test recipient v2"
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {}.recipients WHERE recipient_id = %s").format(
                    sql.Identifier(source_schema)
                ),
                (recipient_id,),
            )
        delete_seconds = wait_for_value(conn, target_schema, recipient_id, None)

        print(
            "CDC passed: "
            f"insert={insert_seconds}s, update={update_seconds}s, "
            f"delete={delete_seconds}s (limit={TIMEOUT_SECONDS}s)."
        )
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {}.recipients WHERE recipient_id = %s").format(
                        sql.Identifier(source_schema)
                    ),
                    (recipient_id,),
                )
                cur.execute(
                    sql.SQL(
                        "DELETE FROM {}.ods_recipients WHERE recipient_id = %s"
                    ).format(sql.Identifier(target_schema)),
                    (recipient_id,),
                )
        finally:
            conn.close()


if __name__ == "__main__":
    run()
