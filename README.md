# Хранилище анализа федеральных расходов США

## Цель проекта

Построить воспроизводимый конвейер данных: загружать данные по дате `ds`, сохранять исходники, переносить CSV и ответ USAspending API в PostgreSQL, преобразовывать их в ODS, рассчитывать три аналитические витрины, копировать одну витрину в ClickHouse и настроить CDC для получателей.

## Структура проекта

- `dag/` — DAG-и Airflow для загрузки и витрин.
- `data/` — проектные CSV; `data/raw/` хранит неизменённые исходные ответы.
- `ingestion/` — загрузчики CSV и USAspending API в staging за дату `ds`.
- `logs/` — журналы запусков и диагностики.
- `sql/` — создание схем и таблиц, преобразования между слоями и SQL-витрины.
- `run/run_pipeline.sh` — ручной запуск SQL- и dbt-шагов по командам, как в примере ментора.
- `dbt/` — dbt-модели, описания колонок и проверки качества.
- `cdc/` — конфигурация Debezium/Kafka и SQL для настройки и проверки CDC.
- `docs/` — паспорт источников, схема данных и инструкции.
- `tools/` — сборка CSV, запуск dbt, ClickHouse-копирование и сверка слоёв.

## Как запускаются SQL-шаги

`run/run_pipeline.sh` выбирает нужный SQL-файл и запускает его через `psql`. Все схемы и таблицы создаются обычными запросами из одного файла `sql/ddl.sql`. Преобразование stg -> ODS записано обычными SQL-запросами в `sql/transform_ods.sql`.

## Подготовка ручного запуска

Скрипт написан для Bash. Запускайте его в Linux, WSL или Git Bash. Нужны Python, PostgreSQL-клиент `psql` и Python-зависимости проекта:

```bash
python3 -m pip install -r requirements.txt
```

Airflow устанавливается отдельно по инструкции учебного стенда. В окружении задайте `PROJECT_LOGIN`, `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` и `PGDATABASE`.

```bash
export PROJECT_LOGIN=your_login
export PGHOST=your_postgres_host
export PGPORT=5432
export PGUSER=your_postgres_user
export PGPASSWORD=your_password
export PGDATABASE=your_database

bash run/run_pipeline.sh check
bash run/run_pipeline.sh all 2024-10-01
```

`all ДАТА` проверяет подключение, создаёт схемы и таблицы, загружает CSV и API за день, переносит данные в ODS, строит SQL-витрины и запускает dbt. ClickHouse-копирование выполняется отдельным DAG. `backfill ДАТА` пересчитывает SQL-витрины и dbt из уже загруженного ODS. `reset` удаляет только три схемы проекта и требует подтверждения логином.

## Airflow

В окружении scheduler и worker задайте `PROJECT_LOGIN` фактическим логином на стенде. Создайте Connections:

- `aiacademy_pg_<login>` — PostgreSQL host, port, database в поле Schema, username и пароль.
- `aiacademy_ch_<login>` — ClickHouse host, port, username и пароль. Для защищённого подключения задайте `{"secure": true}` в Extra.

Основной DAG называется `final_<login>_pipeline`. Запустите его вручную за нужную дату, например `2024-10-01`. После успешных STG/ODS-задач он запускает `final_<login>_datamart` с той же logical date. DAG витрин строит SQL/dbt-модели, выполняет dbt-тесты, затем копирует дневную витрину `dm_agency_daily_spending` в ClickHouse.

## Сверка результата

После успешного запуска обоих DAG задайте `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD` и `CH_SECURE` в окружении и выполните:

```bash
bash run/run_pipeline.sh reconcile 2024-10-01
```

Скрипт сверяет количество строк CSV с STG/ODS, уникальные award ID с API/ODS, сумму действий ODS с дневной витриной, PostgreSQL с ClickHouse и последние статусы задач в ETL-журнале.

## CDC получателей

Источник — `<login>.recipients` (`recipient_id`, `recipient_name`), приёмник — `<login>_ods.ods_recipients`. Задайте `CDC_USER` и `CDC_PASSWORD` в окружении и подготовьте роль, таблицу, права, публикацию и исходное состояние:

```bash
psql -v login="$PROJECT_LOGIN" -f cdc/sql/01_source_recipients.sql
python3 cdc/scripts/prepare_roles.py
psql -v database_name="$PGDATABASE" -v cdc_user="$CDC_USER" \
  -v sink_user="$PGUSER" -v login="$PROJECT_LOGIN" \
  -v ods_schema="${PROJECT_LOGIN}_ods" -f cdc/sql/02_grant_cdc_access.sql
psql -v login="$PROJECT_LOGIN" \
  -v publication_name="${PROJECT_LOGIN}_recipients_pub" \
  -f cdc/sql/03_publication.sql
psql -v login="$PROJECT_LOGIN" -v ods_schema="${PROJECT_LOGIN}_ods" \
  -f cdc/sql/04_seed_recipients.sql
```

`prepare_roles.py` читает пароль из переменной окружения и не печатает его. Для JDBC sink задайте `SINK_USER` и `SINK_PASSWORD`; пользователю нужны права записи в `ods_recipients`. Для REST API задайте `CONNECT_URL` и при необходимости `CONNECT_USER`/`CONNECT_PASSWORD`:

```bash
python3 cdc/scripts/connectors.py apply source
python3 cdc/scripts/connectors.py apply sink
python3 cdc/scripts/connectors.py status source
python3 cdc/scripts/connectors.py status sink
python3 cdc/scripts/verify_cdc.py
```

Проверка создаёт временного получателя `CDC_TEST_...`, измеряет insert/update/delete и удаляет тестовую запись. Шаблоны коннекторов содержат только переменные окружения, секреты в файлах не хранятся.

## Поток данных

`CSV и API -> сохранённые исходники -> stg -> ods -> витрины -> ClickHouse`

Airflow будет запускать пакетную обработку за логическую дату и передавать ту же дату в DAG витрин. CDC будет работать отдельно: изменения из базы сайта проходят через Debezium и Kafka и обновляют текущее состояние одной сущности в ODS.



