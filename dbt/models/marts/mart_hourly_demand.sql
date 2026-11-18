-- Mart: demanda horaria (tipo D) por region. Solo demanda: no se mezcla con pronostico,
-- generacion ni intercambio. Una fila por hora UTC x region.
select
    period_utc,
    date_utc,
    hour_utc,
    respondent,
    respondent_name,
    value as demand_mwh
from {{ ref('int_observations_enriched') }}
where category = 'demand'
