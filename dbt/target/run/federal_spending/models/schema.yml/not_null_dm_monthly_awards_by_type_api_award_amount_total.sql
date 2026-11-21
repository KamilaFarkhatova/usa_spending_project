select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select api_award_amount_total
from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
where api_award_amount_total is null



      
    ) dbt_internal_test