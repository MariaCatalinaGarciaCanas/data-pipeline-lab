# data-pipeline-lab

Pipeline de ingeniería de datos *local-first* e **inspirado en producción** (*production-inspired*,
no listo para producción) que toma datos horarios de la red eléctrica de EE. UU. (EIA-930), los
guarda, limpia, carga en un almacén analítico y los modela con dbt.

Es un proyecto de aprendizaje: el código prioriza ser **legible y explicado** sobre ser mínimo.

```
EIA API → Python (httpx) → Raw Parquet → Polars → Clean Parquet → DuckDB → dbt → Análisis
          [cliente +        [texto,       [tipos,    [validado,     [hechos y   [staging →
           extractor]        por día UTC]  validación] + rechazadas]  dimensiones] marts]
```

## Objetivos

- Pipelines **reproducibles e idempotentes**: repetir una ejecución no duplica ni corrompe nada.
- **Calidad de datos** explícita: nada se descarta sin dejar rastro y los tests de dbt vigilan el modelo.
- Código Python **mantenible** (fuera de notebooks), con pruebas, lint y CI.
- **Seguridad de secretos** tratada en serio (ver `docs/adr/0001-eia-api-key-handling.md`).
- Decisiones **documentadas** (`docs/adr/`).

## Tecnologías

| Herramienta | Uso |
|---|---|
| Python 3.12 + [uv](https://docs.astral.sh/uv/) | Lenguaje y gestión de dependencias (`uv.lock` fija las versiones) |
| httpx | Cliente HTTP síncrono, con reintentos y protección de la API key |
| Polars | Limpieza y tipado de los datos (lectura perezosa de Parquet) |
| DuckDB | Almacén analítico local (un solo archivo) |
| dbt Core + dbt-duckdb | Modelado SQL por capas, con tests de calidad |
| pytest + respx | Pruebas; `respx` simula HTTP (nunca se llama a EIA en los tests) |
| Ruff | Lint y formato |
| Docker | Imagen reproducible del pipeline y de dbt |
| GitHub Actions | CI: lint, tests, pipeline sintético y dbt |

## Estructura

```
data-pipeline-lab/
├── src/pipeline_lab/
│   ├── config.py, logging_config.py     configuración y logs que no exponen secretos
│   ├── eia/
│   │   ├── client.py, exceptions.py     UNA petición validada, reintentos, errores saneados
│   │   ├── _sanitize.py                 saneado de mensajes no confiables del servidor
│   │   └── extractor.py                 recorre TODAS las páginas de una ventana
│   ├── storage/                         capa Raw: Parquet por día UTC, escritura atómica
│   ├── transform/clean.py               Raw → Clean con Polars (+ filas rechazadas)
│   ├── warehouse/duckdb_loader.py       Clean → DuckDB (hechos y dimensiones, idempotente)
│   ├── synthetic.py                     fuente de datos inventados con la forma de EIA-930
│   ├── pipeline.py                      los tres pasos: ingest, transform, load
│   └── cli.py, __main__.py              `python -m pipeline_lab`
├── dbt/                                 proyecto dbt (staging → intermediate → marts)
├── sql/example_queries.sql              consultas de ejemplo sobre el almacén
├── tests/{unit,integration}/            pruebas
├── docs/adr/                            decisiones de arquitectura
├── Dockerfile, .dockerignore
└── .github/workflows/ci.yml
```

## Instalación

Requisitos: [uv](https://docs.astral.sh/uv/getting-started/installation/) y Git (Python 3.12 lo
gestiona uv a partir de `.python-version`).

```powershell
git clone https://github.com/MariaCatalinaGarciaCanas/data-pipeline-lab.git
cd data-pipeline-lab
uv sync            # instala exactamente lo de uv.lock (incluye dbt)
```

## Configuración

Copia `.env.example` a `.env` (está en `.gitignore`; **nunca** lo subas a Git):

| Variable | Para qué | Obligatoria |
|---|---|---|
| `EIA_API_KEY` | Clave de EIA ([registro](https://www.eia.gov/opendata/register.php)) | Solo con `--live` |
| `DATA_DIR` | Carpeta de datos (por defecto `data/`) | No |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`... (por defecto `INFO`) | No |
| `DBT_DUCKDB_PATH` | Ruta del almacén que lee dbt | No |

## Uso: el pipeline

```powershell
# Todo con datos INVENTADOS (sin red ni API key). --anomalies añade filas problemáticas a propósito.
# Usa una carpeta aparte para no mezclarlos con datos reales.
uv run python -m pipeline_lab run --synthetic --anomalies --output-dir data/synthetic

# Por pasos
uv run python -m pipeline_lab ingest --synthetic --output-dir data/synthetic
uv run python -m pipeline_lab transform --output-dir data/synthetic
uv run python -m pipeline_lab load --output-dir data/synthetic

# Datos REALES de EIA (necesita EIA_API_KEY; descarga de verdad) en la carpeta por defecto, data/
uv run python -m pipeline_lab run --live --start 2026-11-04 --end 2026-11-10
```

`ingest` y `run` **exigen elegir** `--synthetic` o `--live`: nada descarga datos reales por
accidente. Otras opciones: `--types D DF NG TI`, `--respondents CISO PJM`, `--page-size 5000`,
`--output-dir`. La ventana por defecto es del 2026-11-04 al 2026-11-10 (días UTC, ambos incluidos).

**Datos sintéticos y reales no se mezclan:** `--synthetic` deja una marca `.synthetic` en la carpeta;
se niega a escribir donde ya hay datos Raw sin esa marca (reemplazaría particiones reales) y `--live`
se niega a escribir en una carpeta marcada.

## Capas de datos

| Capa | Ruta | Contenido |
|---|---|---|
| Raw | `data/raw/eia/rto_region_data/date=YYYY-MM-DD/data.parquet` | Los 7 campos de EIA **como texto** + `_ingested_at` |
| Clean | `data/clean/eia/rto_region_data/observations.parquet` | Tipado, validado, deduplicado |
| Rechazadas | `data/clean/eia/rto_region_data/rejected.parquet` | Filas descartadas **con su motivo** |
| Almacén | `data/warehouse/analytics.duckdb` | Esquema `core` (hechos y dimensiones) y, tras dbt, `staging`, `intermediate`, `marts` |

**Idempotencia:** una partición Raw se reemplaza entera y de forma atómica (archivo temporal +
renombrado); la carga en DuckDB hace `INSERT OR REPLACE` por clave primaria. Repetir cualquier paso
con los mismos datos no cambia el resultado (hay pruebas que lo verifican).

## Transformaciones (Polars)

`period` se interpreta como **UTC**. Una fila se rechaza (a `rejected.parquet`, con `reject_reason`)
si tiene `invalid_period`, `missing_respondent`, `unknown_type`, `invalid_value`
(nulo, no numérico, NaN o infinito), `unexpected_units` (distinto de `megawatthours`) o es un
`duplicate_superseded` (misma clave; se conserva la ingerida más recientemente).
**Los valores negativos no se rechazan:** un *interchange* negativo significa que la región exporta
y es válido; se marca con `is_negative_value`.

## DuckDB: modelo y granularidad

`core.fct_observation` tiene **una fila por hora (UTC) × región × tipo de observación**
(clave primaria `(period_utc, respondent, type)`). Dimensiones: `core.dim_respondent`,
`core.dim_observation_type` y la vista `core.dim_time`. Las marcas de tiempo son `TIMESTAMP` sin zona
que **significan UTC** (ver `docs/adr/0002-data-layers-and-warehouse-design.md`).

Ejemplos (más en [`sql/example_queries.sql`](sql/example_queries.sql)):

```sql
-- Hora de máxima demanda de cada región
SELECT respondent, max_by(period_utc, value) AS hora_pico_utc, max(value) AS demanda_pico_mwh
FROM core.fct_observation WHERE type = 'D' GROUP BY respondent ORDER BY demanda_pico_mwh DESC;

-- Regiones exportadoras: % de horas con intercambio negativo
SELECT respondent, round(100.0 * count(*) FILTER (WHERE value < 0) / count(*), 1) AS horas_exportando_pct
FROM core.fct_observation WHERE type = 'TI' GROUP BY respondent ORDER BY 2 DESC;
```

```powershell
uv run python -c "import duckdb; print(duckdb.connect('data/warehouse/analytics.duckdb', read_only=True).sql('SELECT type, count(*) FROM core.fct_observation GROUP BY type'))"
```

## dbt

Proyecto en `dbt/`, sobre el almacén DuckDB (que debe existir: ejecuta antes el pipeline).

```powershell
cd dbt
uv run dbt build --profiles-dir .      # modelos + tests, sobre ../data/warehouse/analytics.duckdb

# Para el almacén sintético:
$env:DBT_DUCKDB_PATH = "../data/synthetic/warehouse/analytics.duckdb"; uv run dbt build --profiles-dir .
```

| Capa | Modelos |
|---|---|
| staging (vistas) | `stg_observations`, `stg_respondents`, `stg_observation_types` |
| intermediate (vista) | `int_observations_enriched` |
| marts | `mart_hourly_demand`, `mart_hourly_generation`, `mart_interchange_by_region` (tablas), `mart_demand_vs_forecast` (vista) |

Tests: `not_null`, `unique`, `relationships`, `accepted_values`, un test genérico propio
(`unique_combination`) y tests de negocio (los marts no pierden filas; solo el intercambio debería
ser negativo, como aviso). **No se mezclan categorías**: demanda, generación e intercambio van a
marts distintos y el único cruce (demanda frente a pronóstico) es un `INNER JOIN` explícito.

## Pruebas y calidad

```powershell
uv run pytest                  # toda la suite (incluye dbt; tarda ~2 min por el arranque de dbt)
uv run pytest tests/unit       # solo unitarias (rápidas)
uv run ruff check
uv run ruff format --check
```

Ninguna prueba llama a EIA: se usan `respx` (HTTP simulado) y la fuente sintética. `tests/unit/`
cubre cliente, extractor, almacenamiento, limpieza y carga; `tests/integration/` recorre
Fuente → Parquet → Polars → DuckDB → dbt y la línea de comandos. Las pruebas de seguridad de la API
key se describen en el ADR 0001.

## Docker

```powershell
docker build -t data-pipeline-lab .

# Pipeline sintético; los datos quedan en ./data/synthetic (volumen montado)
docker run --rm -v "${PWD}/data/synthetic:/data" data-pipeline-lab run --synthetic --anomalies

# dbt dentro de la imagen, sobre el mismo volumen
docker run --rm -v "${PWD}/data/synthetic:/data" --entrypoint dbt data-pipeline-lab `
    build --project-dir /app/dbt --profiles-dir /app/dbt

# Datos reales: la clave se pasa al ejecutar, NUNCA va dentro de la imagen
docker run --rm --env-file .env -v "${PWD}/data:/data" data-pipeline-lab run --live
```

La imagen corre como usuario sin privilegios, y `.dockerignore` excluye `.env` y `data/`.

## CI (GitHub Actions)

`.github/workflows/ci.yml` se ejecuta en cada PR y en cada push a `main`: instala con
`uv sync --locked`, pasa Ruff (lint y formato), `pytest`, ejecuta el pipeline sintético y
`dbt parse` + `dbt build`. No usa ningún secreto.

## Limitaciones (lo que NO está hecho o NO se ha comprobado)

- **La descarga real de EIA se ha probado solo a pequeña escala.** El 2026-11-17 se descargó
  un día (2026-11-10) de una sola región (CISO): 95 filas, 0 rechazadas, y recorrieron todo el
  pipeline hasta DuckDB. Aún no se ha probado con ventanas largas, con varias regiones a la vez
  ni con volúmenes grandes. El tipo `TI` tuvo 23 horas en lugar de 24 (falta 2026-11-10 18:00 UTC):
  la fuente entregó una fila menos. El test de dbt `assert_no_missing_hours` la detecta como aviso
  (WARN), pero el pipeline de Python no la rechaza ni la corrige (ver Roadmap). Las otras 62
  comprobaciones de dbt se ejecutaron sobre estos datos reales y pasaron, pero ninguna detectó la hora
  faltante de `TI`; la paginación en varias páginas sigue probada solo con
  respuestas simuladas.

- **El CI se ejecutó en GitHub** (PR #3) y pasó en verde.
- **Sin orquestación ni programación**: se ejecuta a mano. No hay aún una ventana móvil automática de
  "últimos N días" para absorber el retraso de publicación de `NG` y `TI` ni las revisiones de EIA.
- **Duplicados:** dos filas con la misma clave se resuelven quedándose con la más reciente. Si un
  servidor ignorase `offset` y repitiera páginas, el extractor lo vería como duplicados y lo avisaría
  en el resumen, pero no puede distinguirlo de duplicados reales.
- `transform` relee toda la capa Raw en cada ejecución. Es barato con unos millones de filas; para
  volúmenes mucho mayores habría que hacerlo incremental.
- DuckDB admite un único proceso escritor: no ejecutes `load` y `dbt` a la vez sobre el mismo archivo.
- La protección de la API key **reduce** las vías de fuga pero no las elimina (red, memoria,
  instrumentación de httpx...). Ver las limitaciones del ADR 0001.
- No se ha medido rendimiento con datos de gran volumen.

## Roadmap

- [x] Fase 0: configuración del proyecto
- [x] Fase 1: ingesta (config, cliente HTTP, extractor, Raw Parquet, CLI)
- [x] Fase 2: Polars y DuckDB
- [x] Fase 3: pruebas y calidad de datos
- [x] Fase 4: dbt
- [x] Fase 5: CI (ejecutado en GitHub en el PR #3, en verde)
- [x] Fase 6: Docker (imagen construida y probada en local)
- [x] Fase 7: documentación
- [X] Primera descarga real de EIA y revisión de resultados
- [ ] Comprobación de completitud: detectar horas faltantes por región y tipo
- [ ] Ventana móvil de reingesta y *freshness checks*
- [ ] Transformación incremental
- [ ] Orquestación (Dagster o Prefect) y observabilidad

## Flujo de trabajo Git

Una rama por tarea, commits pequeños con mensajes descriptivos (*Conventional Commits*) y Pull
Request hacia `main` con CI en verde.
