select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select month_start
from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
where month_start is null



      
    ) dbt_internal_test