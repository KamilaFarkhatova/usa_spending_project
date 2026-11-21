select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select award_type_code as from_field
    from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
    where award_type_code is not null
),

parent as (
    select award_type_code as to_field
    from "aiacademy"."farkhatova_datamart"."int_award_types"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test