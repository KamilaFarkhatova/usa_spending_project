"""Создать/обновить роль Debezium из секретов окружения без файла с паролем."""

import os
import re
import sys
from pathlib import Path

from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ingestion import common


def run():
    username = os.environ.get("CDC_USER", "")
    password = os.environ.get("CDC_PASSWORD", "")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", username):
        raise ValueError("CDC_USER должен быть строчным именем роли PostgreSQL.")
    if not password:
        raise ValueError("Передайте CDC_PASSWORD через секрет окружения.")

    conn = common.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
            exists = cur.fetchone() is not None
            action = "ALTER ROLE" if exists else "CREATE ROLE"
            cur.execute(
                sql.SQL(f"{action} {{}} WITH LOGIN REPLICATION PASSWORD {{}}").format(
                    sql.Identifier(username), sql.Literal(password)
                )
            )
        conn.commit()
    finally:
        conn.close()

    print(f"Роль {username} готова для логической репликации; пароль не выводился.")


if __name__ == "__main__":
    run()
