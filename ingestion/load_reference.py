"""Полностью загрузить CSV-справочник в stg за один ds."""

import argparse
import csv
from datetime import date
from pathlib import Path

try:
    import common
except ModuleNotFoundError:
    from ingestion import common


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FILE = ROOT / "data" / "award_type_reference.csv"
TABLE = "stg_award_type_reference"


def load(ds, file_path=DEFAULT_FILE, schema=None):
    if not isinstance(ds, date):
        ds = date.fromisoformat(str(ds))
    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Не найден CSV-справочник: {file_path}")

    stg_schema = common.schema_name(schema)
    dag_id = common.pipeline_dag_id()
    target_table = f"{stg_schema}.{TABLE}"
    conn = common.connect()
    load_id = None

    try:
        with conn.cursor() as cur:
            columns = common.need_table(cur, stg_schema, TABLE)
        conn.rollback()

        load_id = common.log_start(
            stg_schema,
            dag_id,
            "load_award_type_reference",
            file_path.name,
            target_table,
            ds,
        )

        rows = []
        with file_path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source)
            header = next(reader, None)
            if header != columns:
                raise ValueError(
                    "Колонки CSV и staging-таблицы не совпадают. "
                    f"CSV: {header}; таблица: {columns}"
                )
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise ValueError(
                        f"В строке {line_number} CSV неверное число колонок."
                    )
                rows.append(row)

        if not rows:
            raise ValueError("CSV-справочник пустой.")

        source_file = file_path.name
        deleted, inserted = common.write_rows(
            conn,
            stg_schema,
            TABLE,
            columns,
            rows,
            ds,
            source_file,
            "delete_ds",
        )
    except Exception as error:
        conn.rollback()
        conn.close()
        if load_id is not None:
            common.log_finish(stg_schema, load_id, "failed", 0, str(error))
        raise

    conn.close()
    common.log_finish(stg_schema, load_id, "success", inserted)
    print(
        f"{target_table}, ds={ds}: удалено {deleted}, вставлено {inserted}; "
        f"источник: {source_file}"
    )
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ds", required=True, help="Дата запуска YYYY-MM-DD")
    parser.add_argument("--file", default=str(DEFAULT_FILE), help="CSV-справочник")
    parser.add_argument("--schema", help="STG-схема; по умолчанию <PROJECT_LOGIN>_stg")
    args = parser.parse_args()

    try:
        load(date.fromisoformat(args.ds), args.file, args.schema)
    except Exception as error:
        parser.exit(1, f"Ошибка загрузки справочника: {error}\n")


if __name__ == "__main__":
    main()
