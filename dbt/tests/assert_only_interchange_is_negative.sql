{{ config(severity='warn') }}

-- Test de negocio (aviso, no error): solo el intercambio (TI) deberia tener valores negativos.
-- Una demanda, un pronostico o una generacion negativos son sospechosos. Se deja como `warn`
-- porque en los datos reales de EIA-930 pueden aparecer valores atipicos que conviene revisar,
-- pero que no deben impedir la carga.
select category, respondent, period_utc, value
from {{ ref('int_observations_enriched') }}
where value < 0
  and category <> 'interchange'
