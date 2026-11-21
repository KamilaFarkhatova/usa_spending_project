
  create view "aiacademy"."farkhatova_datamart"."int_awards__dbt_tmp"
    
    
  as (
    select
    award_id,
    recipient_id,
    award_type_code,
    start_date,
    award_amount
from "aiacademy"."farkhatova_ods"."ods_awards"
where start_date is not null
  );