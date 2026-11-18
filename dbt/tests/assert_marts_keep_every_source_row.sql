-- Test de negocio: ningun mart pierde ni inventa observaciones respecto a su categoria de origen.
-- Devuelve las categorias en las que el numero de filas del mart no coincide con el de origen.
with expected as (
    select category, count(*) as expected_rows
    from {{ ref('int_observations_enriched') }}
    group by category
),

actual as (
    select 'demand' as category, count(*) as actual_rows from {{ ref('mart_hourly_demand') }}
    union all
    select 'generation', count(*) from {{ ref('mart_hourly_generation') }}
    union all
    select 'interchange', sum(hours_reported) from {{ ref('mart_interchange_by_region') }}
)

select e.category, e.expected_rows, a.actual_rows
from expected as e
join actual as a using (category)
where e.expected_rows <> a.actual_rows
