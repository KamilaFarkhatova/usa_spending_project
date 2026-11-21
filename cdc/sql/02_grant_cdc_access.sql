GRANT CONNECT ON DATABASE :"database_name" TO :"cdc_user";
GRANT USAGE ON SCHEMA :"login" TO :"cdc_user";
GRANT SELECT ON TABLE :"login".recipients TO :"cdc_user";

GRANT CONNECT ON DATABASE :"database_name" TO :"sink_user";
GRANT USAGE ON SCHEMA :"ods_schema" TO :"sink_user";
GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE :"ods_schema".ods_recipients TO :"sink_user";
