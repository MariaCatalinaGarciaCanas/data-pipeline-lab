-- Mart: intercambio total (tipo TI) por region y dia UTC.
-- Convencion de signo de EIA: valor negativo = la region EXPORTA (envia mas de lo que recibe);
-- positivo = importa. Que sea negativo es valido, no un error.
select
    date_utc,
    respondent,
    respondent_name,
    count(*)                                  as hours_reported,
    sum(value)                                as net_interchange_mwh,
    count(*) filter (where value < 0)         as hours_exporting,
    count(*) filter (where value > 0)         as hours_importing,
    min(value)                                as min_hourly_interchange_mwh,
    max(value)                                as max_hourly_interchange_mwh
from {{ ref('int_observations_enriched') }}
where category = 'interchange'
group by date_utc, respondent, respondent_name
