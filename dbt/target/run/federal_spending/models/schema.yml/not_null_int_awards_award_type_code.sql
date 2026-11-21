select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select award_type_code
from "aiacademy"."farkhatova_datamart"."int_awards"
where award_type_code is null



      
    ) dbt_internal_test