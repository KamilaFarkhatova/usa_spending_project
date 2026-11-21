-- Источник CDC: текущее состояние получателей приложения.
CREATE SCHEMA IF NOT EXISTS :"login";

CREATE TABLE IF NOT EXISTS :"login".recipients (
    recipient_id    text PRIMARY KEY,
    recipient_name  text NOT NULL
);

ALTER TABLE :"login".recipients REPLICA IDENTITY DEFAULT;

