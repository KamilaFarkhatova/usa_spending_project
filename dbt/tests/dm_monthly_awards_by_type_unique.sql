select month_start, award_type_code
from {{ ref('dm_monthly_awards_by_type') }}
group by month_start, award_type_code
having count(*) > 1
