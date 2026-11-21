-- Сверка текущего состояния источника и приёмника после работы коннекторов.
SELECT count(*) AS source_rows
FROM :"login".recipients;

SELECT count(*) AS ods_rows
FROM :"ods_schema".ods_recipients;

SELECT source.recipient_id, source.recipient_name
FROM :"login".recipients AS source
LEFT JOIN :"ods_schema".ods_recipients AS target
  ON target.recipient_id = source.recipient_id
WHERE target.recipient_id IS NULL
   OR target.recipient_name IS DISTINCT FROM source.recipient_name;

SELECT target.recipient_id, target.recipient_name
FROM :"ods_schema".ods_recipients AS target
LEFT JOIN :"login".recipients AS source
  ON source.recipient_id = target.recipient_id
WHERE source.recipient_id IS NULL;
