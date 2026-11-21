select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    award_type_code as unique_field,
    count(*) as n_records

from "aiacademy"."farkhatova_datamart"."int_award_types"
where award_type_code is not null
group by award_type_code
having count(*) > 1



      
    ) dbt_internal_test