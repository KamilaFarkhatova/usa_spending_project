#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

usage() {
  cat <<'EOF'
Использование: PROJECT_LOGIN=логин bash run/run_pipeline.sh КОМАНДА [ДАТА]

Команды:
  check       проверить инструменты и подключение к PostgreSQL
  ddl         создать схемы, журнал, staging- и ODS-таблицы
  stg ДАТА    загрузить оба CSV в STG за YYYY-MM-DD
  api ДАТА    запросить награды в USAspending API и загрузить их в STG
  ods ДАТА    выполнить SQL-преобразование stg → ODS за YYYY-MM-DD
  marts ДАТА  построить SQL-витрины и выполнить dbt за YYYY-MM-DD
  day ДАТА    выполнить stg, api, ods и marts за одну дату
  all ДАТА    создать таблицы и выполнить day за одну дату
  backfill ДАТА полностью пересчитать витрины из загруженного ODS
  reconcile ДАТА сверить CSV, PostgreSQL, витрины, ClickHouse и журнал
  reset       удалить схемы <логин>_datamart, <логин>_ods и <логин>_stg
EOF
}

q() {
  psql -v ON_ERROR_STOP=1 "$@"
}

if [[ "${1:-}" == "help" || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

LOGIN="${PROJECT_LOGIN:-}"
if [[ ! "$LOGIN" =~ ^[a-z][a-z0-9_]{0,40}$ ]]; then
  echo "Укажите PROJECT_LOGIN: строчные латинские буквы, цифры и _; первый символ — буква."
  exit 2
fi

STG_SCHEMA="${LOGIN}_stg"
ODS_SCHEMA="${LOGIN}_ods"
DATAMART_SCHEMA="${LOGIN}_datamart"
DAG_ID="final_${LOGIN}_pipeline"

case "${1:-}" in
  check)
    for name in PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE; do
      if [[ -z "${!name:-}" ]]; then
        echo "Не задана переменная подключения: $name"
        exit 2
      fi
    done
    command -v psql >/dev/null || {
      echo "Не найдена команда psql. Установите клиент PostgreSQL."
      exit 2
    }
    command -v python3 >/dev/null || {
      echo "Не найдена команда python3."
      exit 2
    }
    python3 -c 'import psycopg2' || {
      echo "Для Python не установлен psycopg2."
      exit 2
    }
    command -v dbt >/dev/null || {
      echo "Не найдена команда dbt. Установите dbt-postgres для проекта."
      exit 2
    }
    python3 -c 'import clickhouse_connect' || {
      echo "Для ClickHouse не установлен clickhouse-connect."
      exit 2
    }
    DATABASE="$(q -Atqc 'SELECT current_database()')"
    echo "Подключение к PostgreSQL работает: $DATABASE"
    ;;
  ddl)
    q -q \
      -v stg_schema="$STG_SCHEMA" \
      -v ods_schema="$ODS_SCHEMA" \
      -v datamart_schema="$DATAMART_SCHEMA" \
      -f "$ROOT/sql/ddl.sql"
    ;;
  stg)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/ingestion/load_events.py" \
      --ds "$DS" --schema "$STG_SCHEMA"
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/ingestion/load_reference.py" \
      --ds "$DS" --schema "$STG_SCHEMA"
    ;;
  api)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/ingestion/load_api.py" \
      --ds "$DS" --schema "$STG_SCHEMA"
    ;;
  ods)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    q \
      -v login="$LOGIN" \
      -v ds="$DS" \
      -v dag_id="$DAG_ID" \
      -v stg_schema="$STG_SCHEMA" \
      -v ods_schema="$ODS_SCHEMA" \
      -f "$ROOT/sql/transform_ods.sql"
    ;;
  marts)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    MART_FILES=("$ROOT"/sql/datamart__*.sql)
    if [[ ! -e "${MART_FILES[0]}" ]]; then
      echo "SQL-файлы витрин пока не добавлены; команда появится после шага 7 плана."
      exit 0
    fi
    for sql_file in "${MART_FILES[@]}"; do
      q \
        -v login="$LOGIN" \
        -v ds="$DS" \
        -v dag_id="final_${LOGIN}_datamart" \
        -v stg_schema="$STG_SCHEMA" \
        -v ods_schema="$ODS_SCHEMA" \
        -v datamart_schema="$DATAMART_SCHEMA" \
        -f "$sql_file"
    done
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/tools/run_dbt.py" --ds "$DS"
    ;;
  backfill)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату запуска в формате YYYY-MM-DD."
      exit 2
    fi
    q \
      -v login="$LOGIN" \
      -v dag_id="final_${LOGIN}_datamart" \
      -v stg_schema="$STG_SCHEMA" \
      -v ods_schema="$ODS_SCHEMA" \
      -v datamart_schema="$DATAMART_SCHEMA" \
      -f "$ROOT/sql/backfill_03_marts.sql"
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/tools/run_dbt.py" --ds "$DS"
    ;;
  reconcile)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату запуска в формате YYYY-MM-DD."
      exit 2
    fi
    PROJECT_LOGIN="$LOGIN" python3 "$ROOT/tools/reconcile.py" --ds "$DS"
    ;;
  day)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" stg "$DS"
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" api "$DS"
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" ods "$DS"
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" marts "$DS"
    ;;
  all)
    DS="${2:-}"
    if [[ ! "$DS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Укажите дату в формате YYYY-MM-DD."
      exit 2
    fi
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" check
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" ddl
    PROJECT_LOGIN="$LOGIN" bash "$ROOT/run/run_pipeline.sh" day "$DS"
    ;;
  reset)
    command -v psql >/dev/null || {
      echo "Не найдена команда psql. Установите клиент PostgreSQL."
      exit 2
    }
    read -r -p "Будут удалены только схемы $DATAMART_SCHEMA, $ODS_SCHEMA и $STG_SCHEMA. Для продолжения введите $LOGIN: " answer
    if [[ "$answer" != "$LOGIN" ]]; then
      echo "Очистка отменена."
      exit 1
    fi
    q \
      -c "DROP SCHEMA IF EXISTS \"$DATAMART_SCHEMA\" CASCADE; DROP SCHEMA IF EXISTS \"$ODS_SCHEMA\" CASCADE; DROP SCHEMA IF EXISTS \"$STG_SCHEMA\" CASCADE;"
    ;;
  *)
    usage
    exit 2
    ;;
esac
