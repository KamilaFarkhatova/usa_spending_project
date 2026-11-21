select
    award_type_code,
    award_type_group,
    award_type_name
from {{ source('federal_spending', 'ods_award_types') }}
