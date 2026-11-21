
    
    

with all_values as (

    select
        award_type_code as value_field,
        count(*) as n_records

    from "aiacademy"."farkhatova_datamart"."dm_monthly_awards_by_type"
    group by award_type_code

)

select *
from all_values
where value_field not in (
    'A','B','C','D'
)


