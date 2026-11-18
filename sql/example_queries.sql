-- Consultas de ejemplo sobre el almacen DuckDB (data/warehouse/analytics.duckdb).
--
-- Como ejecutarlas (desde la raiz del proyecto):
--   uv run python -c "import duckdb; con = duckdb.connect('data/warehouse/analytics.duckdb', read_only=True); print(con.sql(open('sql/example_queries.sql').read().split(';')[1]))"
-- o, mas comodo, con el cliente de DuckDB (`duckdb data/warehouse/analytics.duckdb`) y pegando una.
--
-- Recordatorio del modelo: core.fct_observation tiene UNA FILA por hora (UTC) x region x tipo.
-- Tipos: D = demanda, DF = pronostico de demanda del dia anterior, NG = generacion neta,
-- TI = intercambio total (negativo = la region exporta). No se deben sumar tipos distintos.
-- Todas las horas estan en UTC.

-- 1. Cobertura: cuantas observaciones hay por region y tipo (no todas las regiones tienen todo).
SELECT r.respondent, r.respondent_name, f.type, count(*) AS observaciones
FROM core.fct_observation AS f
JOIN core.dim_respondent AS r USING (respondent)
GROUP BY ALL
ORDER BY r.respondent, f.type;

-- 2. Demanda media por hora del dia (UTC) y region.
SELECT f.respondent, t.hour_utc, round(avg(f.value)) AS demanda_media_mwh
FROM core.fct_observation AS f
JOIN core.dim_time AS t USING (period_utc)
WHERE f.type = 'D'
GROUP BY ALL
ORDER BY f.respondent, t.hour_utc;

-- 3. Hora de maxima demanda de cada region.
SELECT respondent, max_by(period_utc, value) AS hora_pico_utc, max(value) AS demanda_pico_mwh
FROM core.fct_observation
WHERE type = 'D'
GROUP BY respondent
ORDER BY demanda_pico_mwh DESC;

-- 4. Calidad del pronostico: error porcentual absoluto medio (MAPE) por region.
SELECT d.respondent,
       round(100 * avg(abs(f.value - d.value) / nullif(d.value, 0)), 2) AS mape_pct
FROM core.fct_observation AS d
JOIN core.fct_observation AS f
  ON f.period_utc = d.period_utc AND f.respondent = d.respondent AND f.type = 'DF'
WHERE d.type = 'D'
GROUP BY d.respondent
ORDER BY mape_pct;

-- 5. Regiones exportadoras: porcentaje de horas con intercambio negativo.
SELECT respondent,
       round(100.0 * count(*) FILTER (WHERE value < 0) / count(*), 1) AS horas_exportando_pct
FROM core.fct_observation
WHERE type = 'TI'
GROUP BY respondent
ORDER BY horas_exportando_pct DESC;

-- 6. Generacion neta frente a demanda por dia y region (comparar, no sumar).
SELECT CAST(period_utc AS DATE) AS dia_utc,
       respondent,
       sum(value) FILTER (WHERE type = 'D')  AS demanda_mwh,
       sum(value) FILTER (WHERE type = 'NG') AS generacion_mwh
FROM core.fct_observation
GROUP BY ALL
ORDER BY dia_utc, respondent;
