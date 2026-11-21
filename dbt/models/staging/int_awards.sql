select
    award_id,
    recipient_id,
    award_type_code,
    start_date,
    award_amount
from {{ source('federal_spending', 'ods_awards') }}
where start_date is not null
