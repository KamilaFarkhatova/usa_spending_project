
  create view "aiacademy"."farkhatova_datamart"."int_award_types__dbt_tmp"
    
    
  as (
    select
    award_type_code,
    award_type_group,
    award_type_name
from "aiacademy"."farkhatova_ods"."ods_award_types"
  );