select
    date_trunc('month', awards.start_date)::date as month_start,
    awards.award_type_code,
    award_types.award_type_group,
    award_types.award_type_name,
    count(distinct awards.award_id)::bigint as api_awards_count,
    coalesce(sum(awards.award_amount), 0)::numeric(20, 2)
        as api_award_amount_total
from "aiacademy"."farkhatova_datamart"."int_awards" as awards
join "aiacademy"."farkhatova_datamart"."int_award_types" as award_types
  on award_types.award_type_code = awards.award_type_code
group by date_trunc('month', awards.start_date)::date,
         awards.award_type_code,
         award_types.award_type_group,
         award_types.award_type_name