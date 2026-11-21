select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select api_awards_count
from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
where api_awards_count is null



      
    ) dbt_internal_test