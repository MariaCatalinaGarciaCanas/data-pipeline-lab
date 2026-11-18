# ADR 0002: Capas de datos, limpieza y diseño del almacén

- **Estado:** Propuesto (primera versión integral del pipeline).
- **Fecha:** 2026-11-17
- **Ámbito:** `src/pipeline_lab/storage`, `transform`, `warehouse`, `dbt/`.

## Contexto

Los datos de EIA-930 llegan por API como texto JSON. Hay que guardarlos, limpiarlos, cargarlos en
un almacén analítico local y modelarlos para análisis, de forma **reproducible** (repetir una
ejecución no debe duplicar ni corromper nada) y **auditable** (poder saber qué se descartó y por qué).

## Decisiones

### 1. Cuatro capas, cada una reproducible desde la anterior

| Capa | Dónde | Qué contiene | Herramienta |
|---|---|---|---|
| Raw | `data/raw/eia/rto_region_data/date=YYYY-MM-DD/data.parquet` | Los 7 campos de EIA **como texto** + `_ingested_at` | Python + Polars |
| Clean | `data/clean/eia/rto_region_data/observations.parquet` y `rejected.parquet` | Datos tipados, validados y deduplicados; filas descartadas con su motivo | Polars |
| Warehouse | `data/warehouse/analytics.duckdb`, esquema `core` | Tabla de hechos y dimensiones | DuckDB |
| Marts | mismo `.duckdb`, esquemas `staging`, `intermediate`, `marts` | Modelos de análisis | dbt |

Raw guarda el dato **sin interpretar** (incluso `value` es texto). Si un criterio de limpieza resulta
erróneo, se corrige y se vuelve a ejecutar `transform` sin tener que descargar de nuevo.

### 2. Nada se descarta sin dejar rastro

Una fila que no pasa la validación va a `rejected.parquet` con su `reject_reason`
(`invalid_period`, `missing_respondent`, `unknown_type`, `invalid_value`, `unexpected_units`,
`duplicate_superseded`) y sus valores originales. **Los valores negativos no se rechazan**: un
`total interchange` negativo significa que la región exporta y es un dato legítimo; se marcan con
`is_negative_value`.

### 3. Granularidad explícita y clave primaria

`core.fct_observation` tiene **una fila por hora (UTC) × región × tipo de observación**, con clave
primaria `(period_utc, respondent, type)`. Esa clave hace imposible tener dos filas para la misma
observación y es la base de la idempotencia.

### 4. Idempotencia en dos niveles

- **Raw:** una partición diaria se reemplaza entera y de forma atómica (archivo temporal +
  `os.replace`). Repetir una ventana deja los mismos datos; una ventana vacía no borra nada.
- **Warehouse:** `INSERT OR REPLACE` sobre la clave primaria dentro de una transacción. Cargar dos
  veces lo mismo no duplica; cargar una corrección sobrescribe la observación.

### 5. Tiempo en UTC, sin zona en el almacén

`period` está en UTC. En Clean y DuckDB se guarda como `TIMESTAMP` **sin zona** que, por convenio,
significa UTC. Así ninguna herramienta lo reinterpreta con la zona horaria del equipo (un error muy
común al extraer la fecha de una marca con zona).

### 6. No mezclar categorías

Demanda (`D`), pronóstico (`DF`), generación (`NG`) e intercambio (`TI`) son magnitudes distintas.
Cada mart trabaja con **una** categoría y el único cruce (demanda frente a pronóstico) es un
`INNER JOIN` explícito por hora y región. No se asume que todas las regiones publican todos los tipos.

### 7. Dependencias mínimas

Polars escribe y lee Parquet sin `pyarrow`, y DuckDB lee Parquet directamente: por eso
`pyarrow` no es una dependencia. Se podrá añadir si algún día hace falta intercambiar objetos Arrow.

## Consecuencias

- Más archivos intermedios en disco (Raw y Clean) a cambio de poder reejecutar cualquier paso.
- `transform` relee toda la capa Raw en cada ejecución (es barato con este volumen; ver "Limitaciones"
  del README para el caso de datos mucho mayores).
- Los duplicados con la misma clave se resuelven quedándose con la fila ingerida más recientemente;
  esto supone que EIA corrige datos hacia adelante (algo habitual en EIA-930, **no verificado** aquí).

## Alternativas descartadas

- **Borrar filas inválidas:** se pierde la trazabilidad y no se puede revisar un criterio dudoso.
- **`TIMESTAMPTZ` en DuckDB:** depende de la zona de la sesión; obliga a fijarla en cada conexión
  (también en dbt) para no cambiar de día al extraer la fecha.
- **Carga con `DELETE` + `INSERT`:** más pasos y más riesgo de dejar la tabla a medias que un
  upsert transaccional por clave primaria.
