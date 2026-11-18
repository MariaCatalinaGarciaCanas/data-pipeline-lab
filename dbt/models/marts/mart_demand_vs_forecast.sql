{{ config(materialized='view') }}

-- Vista: demanda real frente al pronostico del dia anterior (DF), por hora y region.
-- INNER JOIN: solo hay fila cuando existen las DOS observaciones para esa hora y region
-- (no se asume que todas las regiones publican pronostico).
-- forecast_error_mwh > 0 significa que el pronostico se quedo ALTO frente a la demanda real.
with demand as (
    select period_utc, date_utc, respondent, respondent_name, demand_mwh
    from {{ ref('mart_hourly_demand') }}
),

forecast as (
    select period_utc, respondent, value as forecast_mwh
    from {{ ref('int_observations_enriched') }}
    where category = 'demand_forecast'
)

select
    d.period_utc,
    d.date_utc,
    d.respondent,
    d.respondent_name,
    d.demand_mwh,
    f.forecast_mwh,
    f.forecast_mwh - d.demand_mwh                                  as forecast_error_mwh,
    abs(f.forecast_mwh - d.demand_mwh) / nullif(d.demand_mwh, 0)   as abs_pct_error
from demand as d
inner join forecast as f
    on f.period_utc = d.period_utc
   and f.respondent = d.respondent
