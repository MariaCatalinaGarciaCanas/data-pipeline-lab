-- Staging: una fila por observacion, sin transformar el significado. Solo se anade una clave
-- sustituta (observation_id) para poder probar unicidad con un unico campo.
select
    md5(concat_ws('|', cast(period_utc as varchar), respondent, type)) as observation_id,
    period_utc,
    respondent,
    type,
    value,
    value_units,
    ingested_at
from {{ source('core', 'fct_observation') }}
