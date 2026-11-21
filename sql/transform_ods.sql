-- Перенос типизированных данных из stg в ods за одну дату.

\set ON_ERROR_STOP on

\if :{?login}
\else
  \echo 'Передайте login через run/run_pipeline.sh.'
  \quit 2
\endif
\if :{?ds}
\else
  \echo 'Передайте дату ds в формате YYYY-MM-DD.'
  \quit 2
\endif

-- 1. Справочник типов наград из CSV.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'ods_award_types', 'stg_to_ods',
        :'ods_schema' || '.ods_award_types', :'ds')
RETURNING id AS load_id \gset

INSERT INTO :"ods_schema".ods_award_types
    (award_type_code, award_type_group, award_type_name)
SELECT DISTINCT btrim(award_type_code), btrim(award_type_group),
       btrim(award_type_name)
FROM :"stg_schema".stg_award_type_reference
WHERE _ds = :'ds'
  AND NULLIF(btrim(award_type_code), '') IS NOT NULL
  AND NULLIF(btrim(award_type_group), '') IS NOT NULL
  AND NULLIF(btrim(award_type_name), '') IS NOT NULL
ON CONFLICT (award_type_code) DO UPDATE SET
    award_type_group = EXCLUDED.award_type_group,
    award_type_name = EXCLUDED.award_type_name;

UPDATE :"stg_schema".etl_load_log
SET status = 'success', finished_at = now(),
    rows_loaded = (
        SELECT count(*)
        FROM :"stg_schema".stg_award_type_reference
        WHERE _ds = :'ds'
    )
WHERE id = :load_id;

-- 2. Справочник ведомств из событийного CSV и ответа API.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'ods_agencies', 'stg_to_ods',
        :'ods_schema' || '.ods_agencies', :'ds')
RETURNING id AS load_id \gset

WITH source_agencies AS (
    SELECT btrim(awarding_agency_code) AS agency_code,
           COALESCE(NULLIF(btrim(awarding_agency_name), ''),
                    btrim(awarding_agency_code)) AS agency_name
    FROM :"stg_schema".stg_federal_transactions
    WHERE _ds = :'ds'
      AND NULLIF(btrim(awarding_agency_code), '') IS NOT NULL
    UNION
    SELECT btrim(awarding_agency_code),
           COALESCE(NULLIF(btrim(awarding_agency_name), ''),
                    btrim(awarding_agency_code))
    FROM :"stg_schema".stg_api_awards
    WHERE _ds = :'ds'
      AND NULLIF(btrim(awarding_agency_code), '') IS NOT NULL
)
INSERT INTO :"ods_schema".ods_agencies (agency_code, agency_name)
SELECT agency_code, agency_name
FROM source_agencies
ON CONFLICT (agency_code) DO UPDATE SET
    agency_name = EXCLUDED.agency_name;

UPDATE :"stg_schema".etl_load_log
SET status = 'success', finished_at = now(),
    rows_loaded = (SELECT count(*) FROM :"ods_schema".ods_agencies)
WHERE id = :load_id;

-- 3. Справочник получателей из ответа API.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'ods_recipients', 'stg_to_ods',
        :'ods_schema' || '.ods_recipients', :'ds')
RETURNING id AS load_id \gset

INSERT INTO :"ods_schema".ods_recipients (recipient_id, recipient_name)
SELECT DISTINCT ON (btrim(recipient_id))
       btrim(recipient_id),
       COALESCE(NULLIF(btrim(recipient_name), ''), btrim(recipient_id))
FROM :"stg_schema".stg_api_awards
WHERE _ds = :'ds'
  AND NULLIF(btrim(recipient_id), '') IS NOT NULL
ORDER BY btrim(recipient_id), btrim(recipient_name)
ON CONFLICT (recipient_id) DO UPDATE SET
    recipient_name = EXCLUDED.recipient_name;

UPDATE :"stg_schema".etl_load_log
SET status = 'success', finished_at = now(),
    rows_loaded = (SELECT count(*) FROM :"ods_schema".ods_recipients)
WHERE id = :load_id;

-- 4. Проверка и загрузка основной записи о каждой награде из API.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'ods_awards', 'stg_to_ods',
        :'ods_schema' || '.ods_awards', :'ds')
RETURNING id AS load_id \gset

SELECT EXISTS (
    SELECT 1
    FROM :"stg_schema".stg_api_awards a
    LEFT JOIN :"ods_schema".ods_award_types t
      ON upper(regexp_replace(btrim(t.award_type_name), '\s+', ' ', 'g'))
       = upper(regexp_replace(btrim(a.contract_award_type), '\s+', ' ', 'g'))
    WHERE a._ds = :'ds'
      AND (NULLIF(btrim(a.award_id), '') IS NULL
           OR t.award_type_code IS NULL)
) AS has_unmapped_api_award \gset

\if :has_unmapped_api_award
  UPDATE :"stg_schema".etl_load_log
  SET status = 'failed', finished_at = now(),
      error_text = 'API award ID is empty or award type is not in the reference table'
  WHERE id = :load_id;
  \echo 'Ошибка: в API есть пустой award_id или неизвестный тип награды.'
  \quit 1
\endif

SELECT EXISTS (
    SELECT 1
    FROM :"stg_schema".stg_federal_transactions e
    LEFT JOIN :"stg_schema".stg_api_awards a
      ON btrim(a.award_id) = btrim(e.award_id_piid)
     AND a._ds = :'ds'
    WHERE e._ds = :'ds'
      AND a.award_id IS NULL
) AS has_missing_api_award \gset

\if :has_missing_api_award
  UPDATE :"stg_schema".etl_load_log
  SET status = 'failed', finished_at = now(),
      error_text = 'API response does not contain every award from event CSV'
  WHERE id = :load_id;
  \echo 'Ошибка: не все награды из событийного CSV найдены в API.'
  \quit 1
\endif

INSERT INTO :"ods_schema".ods_awards AS target
    (award_id, recipient_id, agency_code, award_type_code,
     start_date, award_amount)
SELECT DISTINCT ON (btrim(a.award_id))
       btrim(a.award_id),
       NULLIF(btrim(a.recipient_id), ''),
       btrim(a.awarding_agency_code),
       t.award_type_code,
       NULLIF(btrim(a.start_date), '')::date,
       NULLIF(btrim(a.award_amount), '')::numeric(20, 2)
FROM :"stg_schema".stg_api_awards a
JOIN :"ods_schema".ods_award_types t
  ON upper(regexp_replace(btrim(t.award_type_name), '\s+', ' ', 'g'))
   = upper(regexp_replace(btrim(a.contract_award_type), '\s+', ' ', 'g'))
WHERE a._ds = :'ds'
ORDER BY btrim(a.award_id), a._loaded_at DESC
ON CONFLICT (award_id) DO UPDATE SET
    recipient_id = COALESCE(EXCLUDED.recipient_id, target.recipient_id),
    agency_code = EXCLUDED.agency_code,
    award_type_code = EXCLUDED.award_type_code,
    start_date = EXCLUDED.start_date,
    award_amount = EXCLUDED.award_amount;

UPDATE :"stg_schema".etl_load_log
SET status = 'success', finished_at = now(),
    rows_loaded = (SELECT count(*) FROM :"ods_schema".ods_awards)
WHERE id = :load_id;

-- 5. Перезагрузка событий за ds из CSV в типизированную таблицу ODS.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'ods_federal_transactions', 'stg_to_ods',
        :'ods_schema' || '.ods_federal_transactions', :'ds')
RETURNING id AS load_id \gset

SELECT EXISTS (
    SELECT 1
    FROM :"stg_schema".stg_federal_transactions
    WHERE _ds = :'ds'
      AND NULLIF(btrim(action_date), '')::date IS DISTINCT FROM :'ds'::date
) AS has_wrong_action_date \gset

\if :has_wrong_action_date
  UPDATE :"stg_schema".etl_load_log
  SET status = 'failed', finished_at = now(),
      error_text = 'action_date differs from ds in staging rows'
  WHERE id = :load_id;
  \echo 'Ошибка: action_date в staging не совпадает с ds.'
  \quit 1
\endif

DELETE FROM :"ods_schema".ods_federal_transactions
WHERE action_date = :'ds'::date;

INSERT INTO :"ods_schema".ods_federal_transactions
    (award_id_piid, modification_number, action_date,
     federal_action_obligation, awarding_agency_code, award_type_code)
SELECT btrim(award_id_piid),
       btrim(modification_number),
       NULLIF(btrim(action_date), '')::date,
       NULLIF(btrim(federal_action_obligation), '')::numeric(20, 2),
       btrim(awarding_agency_code),
       btrim(award_type_code)
FROM :"stg_schema".stg_federal_transactions
WHERE _ds = :'ds';

UPDATE :"stg_schema".etl_load_log
SET status = 'success', finished_at = now(),
    rows_loaded = (
        SELECT count(*)
        FROM :"ods_schema".ods_federal_transactions
        WHERE action_date = :'ds'::date
    )
WHERE id = :load_id;

\echo 'Перенос stg → ODS завершён.'
