select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    award_id as unique_field,
    count(*) as n_records

from "aiacademy"."farkhatova_datamart"."int_awards"
where award_id is not null
group by award_id
having count(*) > 1



      
    ) dbt_internal_test