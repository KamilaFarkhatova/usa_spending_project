select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select month_start, award_type_code
from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
group by month_start, award_type_code
having count(*) > 1
      
    ) dbt_internal_test