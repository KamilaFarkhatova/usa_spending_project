
    
    

with child as (
    select award_type_code as from_field
    from "aiacademy"."farkhatova_datamart"."int_awards"
    where award_type_code is not null
),

parent as (
    select award_type_code as to_field
    from "aiacademy"."farkhatova_ods"."ods_award_types"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


