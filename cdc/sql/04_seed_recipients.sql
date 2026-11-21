-- Начальная копия текущих ODS-получателей в источник CDC.
INSERT INTO :"login".recipients (recipient_id, recipient_name)
SELECT recipient_id, recipient_name
FROM :"ods_schema".ods_recipients
ON CONFLICT (recipient_id) DO UPDATE SET
    recipient_name = EXCLUDED.recipient_name;

