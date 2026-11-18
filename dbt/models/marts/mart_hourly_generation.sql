-- Mart: generacion neta horaria (tipo NG) por region. Una fila por hora UTC x region.
-- No todas las regiones reportan generacion: las que no lo hacen simplemente no aparecen.
select
    period_utc,
    date_utc,
    hour_utc,
    respondent,
    respondent_name,
    value as net_generation_mwh
from {{ ref('int_observations_enriched') }}
where category = 'generation'
