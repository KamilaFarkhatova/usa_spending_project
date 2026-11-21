-- Витрина: сумма финансовых действий по дням и ведомствам за ds.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'dm_agency_daily_spending', 'ods',
        :'datamart_schema' || '.dm_agency_daily_spending', :'ds');

DELETE FROM :"datamart_schema".dm_agency_daily_spending
WHERE action_date = :'ds'::date;

WITH daily_totals AS (
    SELECT action_date,
           awarding_agency_code AS agency_code,
           count(*) AS transaction_count,
           count(DISTINCT award_id_piid) AS awards_count,
           sum(federal_action_obligation) AS net_action_obligation
    FROM :"ods_schema".ods_federal_transactions
    WHERE action_date = :'ds'::date
    GROUP BY action_date, awarding_agency_code
)
INSERT INTO :"datamart_schema".dm_agency_daily_spending
    (action_date, agency_code, agency_name, transaction_count,
     awards_count, net_action_obligation)
SELECT :'ds'::date,
       agency.agency_code,
       agency.agency_name,
       coalesce(daily.transaction_count, 0),
       coalesce(daily.awards_count, 0),
       coalesce(daily.net_action_obligation, 0)
FROM :"ods_schema".ods_agencies AS agency
LEFT JOIN daily_totals AS daily
  ON daily.agency_code = agency.agency_code;

UPDATE :"stg_schema".etl_load_log
SET status = 'success',
    finished_at = now(),
    rows_loaded = (
        SELECT count(*)
        FROM :"datamart_schema".dm_agency_daily_spending
        WHERE action_date = :'ds'::date
    )
WHERE id = (
    SELECT id
    FROM :"stg_schema".etl_load_log
    WHERE dag_id = :'dag_id'
      AND task_id = 'dm_agency_daily_spending'
      AND ds = :'ds'::date
      AND status = 'running'
    ORDER BY id DESC
    LIMIT 1
);
