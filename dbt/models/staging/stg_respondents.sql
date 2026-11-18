select
    respondent,
    respondent_name
from {{ source('core', 'dim_respondent') }}
