from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
ARCHIVE = RAW_DIR / "federal_transactions_export.zip"
REFERENCE_JSON = RAW_DIR / "award_types.json"
TRANSACTIONS_CSV = ROOT / "data" / "federal_transactions.csv"
REFERENCE_CSV = ROOT / "data" / "award_type_reference.csv"
EXPECTED_ROWS = 1937
START_DATE = "2024-10-01"
END_DATE = "2024-10-31"
EVENT_FIELDS = [
    "award_id_piid",
    "modification_number",
    "action_date",
    "federal_action_obligation",
    "awarding_agency_code",
    "awarding_agency_name",
    "recipient_name",
    "award_type_code",
    "award_type",
]
REFERENCE_FIELDS = ["award_type_code", "award_type_group", "award_type_name"]


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as temp:
        temp.write(content)
        temp_path = Path(temp.name)
    os.replace(temp_path, path)


def to_csv(rows: list[dict], fields: list[str]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> None:
    if not ARCHIVE.is_file() or not REFERENCE_JSON.is_file():
        raise FileNotFoundError("The source archive or award_types.json is missing from data/raw")

    with zipfile.ZipFile(ARCHIVE) as archive:
        candidates = [name for name in archive.namelist() if name.lower().endswith(".csv") and "primetransactions" in name.lower()]
        if len(candidates) != 1:
            raise RuntimeError(f"Expected one PrimeTransactions CSV in archive, found {len(candidates)}")
        with archive.open(candidates[0]) as binary:
            text = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(text)
            source_fields = set(reader.fieldnames or [])
            missing_fields = sorted(set(EVENT_FIELDS) - source_fields)
            if missing_fields:
                raise RuntimeError(f"Required source fields are missing: {missing_fields}")
            records = [{field: row.get(field, "") or "" for field in EVENT_FIELDS} for row in reader]

    if len(records) != EXPECTED_ROWS:
        raise RuntimeError(f"Expected {EXPECTED_ROWS} rows from API metadata, found {len(records)}")

    event_keys = [(r["award_id_piid"], r["modification_number"]) for r in records]
    if any(not award or not mod for award, mod in event_keys):
        raise RuntimeError("Some rows do not have the award id and modification number key")
    if len(set(event_keys)) != len(event_keys):
        raise RuntimeError("The composite (award_id_piid, modification_number) key is not unique")
    if any(not (START_DATE <= r["action_date"] <= END_DATE) for r in records):
        raise RuntimeError("Some action dates are outside the requested period")

    with REFERENCE_JSON.open(encoding="utf-8-sig") as stream:
        type_map = json.load(stream)
    reference_rows = []
    for group, mapping in type_map.items():
        for code, name in mapping.items():
            reference_rows.append({
                "award_type_code": code,
                "award_type_group": group,
                "award_type_name": name,
            })
    reference_rows.sort(key=lambda r: (r["award_type_group"], r["award_type_code"]))
    known_codes = {r["award_type_code"] for r in reference_rows}
    used_codes = {r["award_type_code"] for r in records}
    if used_codes - known_codes:
        raise RuntimeError(f"Transaction codes missing from reference: {sorted(used_codes - known_codes)}")

    atomic_write(TRANSACTIONS_CSV, to_csv(records, EVENT_FIELDS))
    atomic_write(REFERENCE_CSV, to_csv(reference_rows, REFERENCE_FIELDS))

    print(f"Created {TRANSACTIONS_CSV.relative_to(ROOT)}: {len(records)} rows")
    print(f"Unique event keys (award_id_piid + modification_number): {len(set(event_keys))}")
    print(f"Date range: {min(r['action_date'] for r in records)} to {max(r['action_date'] for r in records)}")
    print(f"Agencies: {', '.join(sorted({r['awarding_agency_name'] for r in records}))}")
    print(f"Award type reference: {len(reference_rows)} rows; used codes {', '.join(sorted(used_codes))}")
    print(f"Created {REFERENCE_CSV.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
