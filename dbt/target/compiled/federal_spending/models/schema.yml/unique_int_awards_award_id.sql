
    
    

select
    award_id as unique_field,
    count(*) as n_records

from "aiacademy"."farkhatova_datamart"."int_awards"
where award_id is not null
group by award_id
having count(*) > 1


