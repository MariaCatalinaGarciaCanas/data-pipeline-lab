-- Intermediate: observaciones con los nombres de region y tipo y columnas de tiempo derivadas.
-- Se usa LEFT JOIN a proposito: si faltara una dimension la fila NO desaparece (los tests de
-- relaciones y not_null de las dimensiones lo detectarian, en lugar de perder datos sin avisar).
select
    o.observation_id,
    o.period_utc,
    cast(o.period_utc as date)  as date_utc,
    hour(o.period_utc)          as hour_utc,
    isodow(o.period_utc)        as iso_day_of_week,   -- 1 = lunes ... 7 = domingo
    o.respondent,
    r.respondent_name,
    o.type,
    t.type_name,
    t.category,
    o.value,
    o.value_units,
    (o.value < 0)               as is_negative_value,
    o.ingested_at
from {{ ref('stg_observations') }} as o
left join {{ ref('stg_respondents') }} as r on r.respondent = o.respondent
left join {{ ref('stg_observation_types') }} as t on t.type = o.type
