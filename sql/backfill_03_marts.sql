-- Полный повторный расчёт витрин после загрузки ODS за весь период.

INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
SELECT :'dag_id', 'backfill_03_marts', 'ods_full_refresh',
       :'datamart_schema' || '.all_marts',
       coalesce(max(action_date), current_date)
FROM :"ods_schema".ods_federal_transactions;

DELETE FROM :"datamart_schema".dm_agency_daily_spending;
DELETE FROM :"datamart_schema".dm_recipient_analysis;

WITH date_bounds AS (
    SELECT min(action_date) AS first_date, max(action_date) AS last_date
    FROM :"ods_schema".ods_federal_transactions
), calendar AS (
    SELECT generate_series(
               first_date::timestamp,
               last_date::timestamp,
               interval '1 day'
           )::date
           AS action_date
    FROM date_bounds
    WHERE first_date IS NOT NULL
), daily_totals AS (
    SELECT action_date,
           awarding_agency_code AS agency_code,
           count(*) AS transaction_count,
           count(DISTINCT award_id_piid) AS awards_count,
           sum(federal_action_obligation) AS net_action_obligation
    FROM :"ods_schema".ods_federal_transactions
    GROUP BY action_date, awarding_agency_code
)
INSERT INTO :"datamart_schema".dm_agency_daily_spending
    (action_date, agency_code, agency_name, transaction_count,
     awards_count, net_action_obligation)
SELECT calendar.action_date,
       agency.agency_code,
       agency.agency_name,
       coalesce(daily.transaction_count, 0),
       coalesce(daily.awards_count, 0),
       coalesce(daily.net_action_obligation, 0)
FROM calendar
CROSS JOIN :"ods_schema".ods_agencies AS agency
LEFT JOIN daily_totals AS daily
  ON daily.action_date = calendar.action_date
 AND daily.agency_code = agency.agency_code;

WITH recipient_metrics AS (
    SELECT date_trunc('month', tx.action_date)::date AS period_start,
           (date_trunc('month', tx.action_date)
             + interval '1 month' - interval '1 day')::date AS period_end,
           recipient.recipient_id,
           recipient.recipient_name,
           count(*) AS transaction_count,
           count(DISTINCT award.award_id) AS awards_count,
           sum(tx.federal_action_obligation) AS net_action_obligation
    FROM :"ods_schema".ods_federal_transactions AS tx
    JOIN :"ods_schema".ods_awards AS award
      ON award.award_id = tx.award_id_piid
    JOIN :"ods_schema".ods_recipients AS recipient
      ON recipient.recipient_id = award.recipient_id
    GROUP BY date_trunc('month', tx.action_date),
             recipient.recipient_id,
             recipient.recipient_name
), ranked_recipients AS (
    SELECT recipient_metrics.*,
           rank() OVER (
               PARTITION BY period_start
               ORDER BY net_action_obligation DESC
           ) AS rank_by_obligation,
           rank() OVER (
               PARTITION BY period_start
               ORDER BY awards_count DESC
           ) AS rank_by_awards
    FROM recipient_metrics
)
INSERT INTO :"datamart_schema".dm_recipient_analysis
    (period_start, period_end, recipient_id, recipient_name,
     transaction_count, awards_count, net_action_obligation,
     rank_by_obligation, rank_by_awards)
SELECT period_start, period_end, recipient_id, recipient_name,
       transaction_count, awards_count, net_action_obligation,
       rank_by_obligation, rank_by_awards
FROM ranked_recipients;

UPDATE :"stg_schema".etl_load_log
SET status = 'success',
    finished_at = now(),
    rows_loaded =
        (SELECT count(*) FROM :"datamart_schema".dm_agency_daily_spending)
        + (SELECT count(*) FROM :"datamart_schema".dm_recipient_analysis)
WHERE id = (
    SELECT id
    FROM :"stg_schema".etl_load_log
    WHERE dag_id = :'dag_id'
      AND task_id = 'backfill_03_marts'
      AND source = 'ods_full_refresh'
      AND status = 'running'
    ORDER BY id DESC
    LIMIT 1
);
