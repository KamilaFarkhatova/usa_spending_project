"""Загрузить сведения о наградах из USAspending за один ds в staging."""

import argparse
import csv
import json
import os
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

try:
    import common
except ModuleNotFoundError:
    from ingestion import common
import requests


ROOT = Path(__file__).resolve().parents[1]
EVENTS_CSV = ROOT / "data" / "federal_transactions.csv"
RAW_DIR = ROOT / "data" / "raw" / "api_awards"
API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
STG_TABLE = "stg_api_awards"
STG_COLUMNS = [
    "award_id",
    "recipient_id",
    "recipient_name",
    "awarding_agency_code",
    "awarding_agency_name",
    "start_date",
    "award_amount",
    "contract_award_type",
]
AWARD_TYPE_CODES = ["A", "B", "C", "D"]
PAGE_SIZE = 100
FIELDS = [
    "Award ID",
    "recipient_id",
    "Recipient Name",
    "Awarding Agency Code",
    "Awarding Agency",
    "Start Date",
    "Award Amount",
    "Contract Award Type",
]


def read_award_ids(ds):
    """Прочитать уникальные ID наград из событийного CSV за ds."""
    if not EVENTS_CSV.is_file():
        raise FileNotFoundError(f"Не найден файл событий: {EVENTS_CSV}")

    with EVENTS_CSV.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required = {"action_date", "award_id_piid"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"В CSV отсутствуют колонки: {sorted(missing)}")
        award_ids = set()
        for row in reader:
            action_date = (row.get("action_date") or "").strip()
            award_id = (row.get("award_id_piid") or "").strip()
            if action_date == ds.isoformat() and award_id:
                award_ids.add(award_id)
        return sorted(award_ids)


def request_page(payload, timeout=20, retries=3):
    """Отправить страницу запроса; повторять сетевые ошибки, 429 и 5xx."""
    last_error = "unknown error"
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(API_URL, json=payload, timeout=timeout)
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as error:
                    raise RuntimeError("USAspending вернул невалидный JSON.") from error
            if response.status_code != 429 and response.status_code < 500:
                raise RuntimeError(
                    f"USAspending ответил HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"

        if attempt < retries:
            time.sleep(min(2 ** attempt, 8))

    raise RuntimeError(
        f"Запрос к USAspending не удался после {retries} попыток: {last_error}"
    )


def fetch_awards(award_ids):
    """Запросить награды пачками и пройти все страницы ответа API."""
    raw_pages = []
    results = []

    for offset in range(0, len(award_ids), PAGE_SIZE):
        batch = award_ids[offset:offset + PAGE_SIZE]
        page = 1
        while True:
            payload = {
                "filters": {
                    "award_type_codes": AWARD_TYPE_CODES,
                    "award_ids": batch,
                },
                "fields": FIELDS,
                "limit": PAGE_SIZE,
                "page": page,
                "sort": "Award ID",
                "order": "asc",
            }
            response = request_page(payload)
            page_results = response.get("results")
            page_metadata = response.get("page_metadata")
            if not isinstance(page_results, list) or not isinstance(page_metadata, dict):
                raise RuntimeError(
                    "Ответ USAspending не содержит results или page_metadata."
                )
            has_next = page_metadata.get("hasNext")
            if not isinstance(has_next, bool):
                raise RuntimeError(
                    "В page_metadata отсутствует логическое поле hasNext."
                )

            raw_pages.append({"request": payload, "response": response})
            results.extend(page_results)
            if not has_next:
                break
            page += 1

    return raw_pages, results


def write_raw_response(ds, award_ids, raw_pages):
    """Сохранить исходные ответы API и параметры запросов в data/raw."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    target = RAW_DIR / f"{ds.isoformat()}.json"
    document = {
        "endpoint": API_URL,
        "ds": ds.isoformat(),
        "award_ids": award_ids,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "pages": raw_pages,
    }
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=RAW_DIR, delete=False
    ) as temp:
        json.dump(document, temp, ensure_ascii=False, indent=2)
        temp.write("\n")
        temp_path = Path(temp.name)
    os.replace(temp_path, target)
    return target


def text_or_none(value):
    return None if value is None else str(value)


def normalize_award(record):
    """Преобразовать поля ответа API в порядке колонок STG-таблицы."""
    award_id = record.get("Award ID")
    if not award_id:
        raise ValueError("В ответе API найдена награда без Award ID.")
    return (
        str(award_id),
        text_or_none(record.get("recipient_id")),
        text_or_none(record.get("Recipient Name")),
        text_or_none(record.get("Awarding Agency Code")),
        text_or_none(record.get("Awarding Agency")),
        text_or_none(record.get("Start Date")),
        text_or_none(record.get("Award Amount")),
        text_or_none(record.get("Contract Award Type")),
    )


def run(ds, schema=None):
    stg_schema = common.schema_name(schema)
    dag_id = common.pipeline_dag_id()
    target_table = f"{stg_schema}.{STG_TABLE}"
    conn = common.connect()
    try:
        with conn.cursor() as cur:
            columns = common.need_table(cur, stg_schema, STG_TABLE)
        conn.rollback()
        if columns != STG_COLUMNS:
            raise RuntimeError(
                f"Колонки {target_table} не совпадают с загрузчиком. "
                f"Ожидались {STG_COLUMNS}, найдены {columns}."
            )

        load_id = common.log_start(
            stg_schema, dag_id, "load_api_awards", API_URL, target_table, ds
        )
    except Exception:
        conn.close()
        raise

    try:
        award_ids = read_award_ids(ds)
        raw_pages, api_rows = fetch_awards(award_ids)
        raw_path = write_raw_response(ds, award_ids, raw_pages)

        requested = set(award_ids)
        records_by_id = {
            str(record.get("Award ID")): record
            for record in api_rows
            if record.get("Award ID") is not None
            and str(record.get("Award ID")) in requested
        }
        missing = sorted(requested - set(records_by_id))
        if missing:
            raise RuntimeError(
                "API не вернул часть наград из событийного CSV: "
                + ", ".join(missing[:10])
            )

        rows = [normalize_award(records_by_id[award_id]) for award_id in award_ids]
        source_file = f"{API_URL} (ds={ds.isoformat()})"
        deleted, inserted = common.write_rows(
            conn,
            stg_schema,
            STG_TABLE,
            columns,
            rows,
            ds,
            source_file,
            "delete_ds",
        )
    except Exception as error:
        conn.rollback()
        conn.close()
        common.log_finish(stg_schema, load_id, "failed", 0, str(error))
        raise

    conn.close()
    common.log_finish(stg_schema, load_id, "success", inserted)
    print(
        f"{target_table}, ds={ds}: удалено {deleted}, вставлено {inserted}; "
        f"сырой ответ сохранён в {raw_path.relative_to(ROOT)}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ds", required=True, help="Дата запуска YYYY-MM-DD")
    parser.add_argument(
        "--schema", help="STG-схема; по умолчанию <PROJECT_LOGIN>_stg"
    )
    args = parser.parse_args()

    try:
        ds = date.fromisoformat(args.ds)
        run(ds, args.schema)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
