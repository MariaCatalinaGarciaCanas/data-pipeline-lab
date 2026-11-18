select
    type,
    type_name,
    category
from {{ source('core', 'dim_observation_type') }}
