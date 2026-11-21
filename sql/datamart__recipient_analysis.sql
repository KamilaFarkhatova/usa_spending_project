-- Витрина: рейтинг получателей по действиям и числу наград за месяц ds.
INSERT INTO :"stg_schema".etl_load_log
    (dag_id, task_id, source, target_table, ds)
VALUES (:'dag_id', 'dm_recipient_analysis', 'ods',
        :'datamart_schema' || '.dm_recipient_analysis', :'ds');

DELETE FROM :"datamart_schema".dm_recipient_analysis
WHERE period_start = date_trunc('month', :'ds'::date)::date;

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
    WHERE tx.action_date >= date_trunc('month', :'ds'::date)::date
      AND tx.action_date <
          (date_trunc('month', :'ds'::date) + interval '1 month')::date
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
    rows_loaded = (
        SELECT count(*)
        FROM :"datamart_schema".dm_recipient_analysis
        WHERE period_start = date_trunc('month', :'ds'::date)::date
    )
WHERE id = (
    SELECT id
    FROM :"stg_schema".etl_load_log
    WHERE dag_id = :'dag_id'
      AND task_id = 'dm_recipient_analysis'
      AND ds = :'ds'::date
      AND status = 'running'
    ORDER BY id DESC
    LIMIT 1
);
