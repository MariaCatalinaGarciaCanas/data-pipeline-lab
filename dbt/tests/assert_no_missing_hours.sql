{{ config(severity='warn') }}

-- Test de completitud (aviso, no error): para cada respondent y type, genera todas las horas
-- entre su primer y su ultimo period_utc y devuelve las que no existen en los datos.
-- Es `warn` porque la fuente (EIA) puede omitir horas, y eso no debe impedir la carga.
with bounds as (
    select
        respondent,
        type,
        min(period_utc) as first_hour,
        max(period_utc) as last_hour
    from {{ ref('stg_observations') }}
    group by respondent, type
),

expected as (
    select
        respondent,
        type,
        unnest(generate_series(first_hour, last_hour, interval 1 hour)) as missing_hour
    from bounds
)

select e.respondent, e.type, e.missing_hour
from expected as e
left join {{ ref('stg_observations') }} as o
    on o.respondent = e.respondent
   and o.type = e.type
   and o.period_utc = e.missing_hour
where o.period_utc is null
