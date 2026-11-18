"""Limpieza Raw -> Clean con Polars.

Criterio general: NO se descarta nada sin dejar rastro. Cada fila que no pasa la validacion va a
`rejected` con su motivo, conservando los valores originales; asi se puede auditar y, si el
criterio era demasiado estricto, cambiarlo sin haber perdido datos.

Motivos de rechazo (`reject_reason`), evaluados en este orden:
  invalid_period        `period` no es `YYYY-MM-DDTHH` o no es una fecha real.
  missing_respondent    `respondent` vacio o nulo.
  unknown_type          `type` no es D, DF, NG ni TI.
  invalid_value         `value` nulo, no numerico o no finito (NaN, inf).
  unexpected_units      `value-units` distinto de `megawatthours` (mezclar unidades falsearia
                        las sumas; se rechaza hasta decidir como convertir).
  duplicate_superseded  hay otra fila con la misma clave (period, respondent, type); se conserva
                        la mas reciente segun `_ingested_at`.

Que NO se rechaza: los valores negativos. Un `total interchange` negativo es legitimo (la region
exporta). Se marcan con `is_negative_value` para poder estudiarlos.

Convenio de tiempo: `period` esta en UTC. En Clean (y despues en DuckDB) las marcas de tiempo se
guardan como TIMESTAMP SIN zona, que por convenio SIEMPRE significa UTC; asi ninguna herramienta
posterior las reinterpreta con la zona horaria de la maquina.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from pipeline_lab.storage.parquet_io import atomic_write_parquet
from pipeline_lab.storage.raw import RAW_COLUMNS

logger = logging.getLogger(__name__)

EXPECTED_UNITS = "megawatthours"
TYPE_NAMES = {
    "D": "Demand",
    "DF": "Day-ahead demand forecast",
    "NG": "Net generation",
    "TI": "Total interchange",
}
# `category` agrupa los tipos por significado: no mezclar demanda, generacion e intercambio.
TYPE_CATEGORY = {
    "D": "demand",
    "DF": "demand_forecast",
    "NG": "generation",
    "TI": "interchange",
}
CLEAN_COLUMNS = (
    "period_utc",
    "observation_date",
    "hour_utc",
    "respondent",
    "respondent_name",
    "type",
    "type_name",
    "category",
    "value",
    "value_units",
    "is_negative_value",
    "ingested_at",
)
KEY = ["period_utc", "respondent", "type"]  # granularidad: una fila por hora x region x tipo
OBLIGATORY_RAW = (*RAW_COLUMNS, "_ingested_at")

OUTPUT_CLEAN = "observations.parquet"
OUTPUT_REJECTED = "rejected.parquet"


class SchemaError(Exception):
    """El Parquet de entrada no tiene las columnas esperadas."""


@dataclass(frozen=True, slots=True)
class CleaningStats:
    raw_rows: int
    clean_rows: int
    rejected_rows: int
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    null_counts: dict[str, int] = field(default_factory=dict)  # nulos por columna Raw


@dataclass(frozen=True, slots=True)
class CleanResult:
    clean: pl.DataFrame
    rejected: pl.DataFrame
    stats: CleaningStats


def _validate_columns(lf: pl.LazyFrame) -> None:
    present = set(lf.collect_schema().names())
    missing = [c for c in OBLIGATORY_RAW if c not in present]
    if missing:
        raise SchemaError(f"Faltan columnas en los datos Raw: {missing}")


def clean_observations(raw: pl.LazyFrame | pl.DataFrame) -> CleanResult:
    """Valida, tipa, normaliza y deduplica los datos Raw.

    Devuelve las filas limpias (esquema `CLEAN_COLUMNS`), las rechazadas (columnas Raw originales
    mas `reject_reason`) y estadisticas. No lee ni escribe disco.
    """
    lf = raw.lazy()
    _validate_columns(lf)
    original = lf.select(list(OBLIGATORY_RAW))

    period_text = pl.col("period").str.strip_chars()
    typed = original.with_columns(
        # El patron exige exactamente YYYY-MM-DDTHH; se completa con :00:00 para parsear la hora.
        pl.when(period_text.str.contains(r"^\d{4}-\d{2}-\d{2}T\d{2}$"))
        .then(
            (period_text + pl.lit(":00:00")).str.to_datetime(
                "%Y-%m-%dT%H:%M:%S", time_zone="UTC", strict=False
            )
        )
        .otherwise(None)
        .alias("_period_utc"),
        pl.col("respondent").str.strip_chars().str.to_uppercase().alias("_respondent"),
        pl.col("type").str.strip_chars().str.to_uppercase().alias("_type"),
        pl.col("value").str.strip_chars().cast(pl.Float64, strict=False).alias("_value"),
        pl.col("value-units").str.strip_chars().str.to_lowercase().alias("_units"),
    )

    reason = (
        pl.when(pl.col("_period_utc").is_null())
        .then(pl.lit("invalid_period"))
        .when(pl.col("_respondent").is_null() | (pl.col("_respondent") == ""))
        .then(pl.lit("missing_respondent"))
        .when(~pl.col("_type").is_in(list(TYPE_CATEGORY)).fill_null(False))
        .then(pl.lit("unknown_type"))
        .when(pl.col("_value").is_null() | ~pl.col("_value").is_finite())
        .then(pl.lit("invalid_value"))
        # `null != x` da null (no True): sin fill_null, unas unidades ausentes pasarian.
        .when((pl.col("_units") == EXPECTED_UNITS).fill_null(False).not_())
        .then(pl.lit("unexpected_units"))
        .otherwise(None)
    )
    frame = typed.with_columns(reason.alias("reject_reason")).collect()

    valid = frame.filter(pl.col("reject_reason").is_null())
    invalid = frame.filter(pl.col("reject_reason").is_not_null())

    # Duplicados: misma clave -> gana la fila ingerida mas tarde; el resto se rechaza.
    group = ["_period_utc", "_respondent", "_type"]
    ordered = valid.sort([*group, "_ingested_at"], maintain_order=True).with_columns(
        pl.int_range(pl.len()).over(group).alias("_position"),
        pl.len().over(group).alias("_group_size"),
    )
    is_winner = pl.col("_position") == pl.col("_group_size") - 1
    winners = ordered.filter(is_winner)
    superseded = ordered.filter(~is_winner).with_columns(
        pl.lit("duplicate_superseded").alias("reject_reason")
    )

    respondent = pl.col("_respondent")
    name = pl.col("respondent-name").str.strip_chars()
    clean = winners.select(
        pl.col("_period_utc").dt.replace_time_zone(None).alias("period_utc"),
        pl.col("_period_utc").dt.date().alias("observation_date"),
        pl.col("_period_utc").dt.hour().cast(pl.Int8).alias("hour_utc"),
        respondent.alias("respondent"),
        pl.when(name.is_null() | (name == ""))
        .then(respondent)
        .otherwise(name)
        .alias("respondent_name"),
        pl.col("_type").alias("type"),
        pl.col("_type").replace_strict(TYPE_NAMES).alias("type_name"),
        pl.col("_type").replace_strict(TYPE_CATEGORY).alias("category"),
        pl.col("_value").alias("value"),
        pl.col("_units").alias("value_units"),
        (pl.col("_value") < 0).alias("is_negative_value"),
        pl.col("_ingested_at").dt.replace_time_zone(None).alias("ingested_at"),
    ).sort(KEY)

    rejected_columns = [*OBLIGATORY_RAW, "reject_reason"]
    rejected = pl.concat(
        [invalid.select(rejected_columns), superseded.select(rejected_columns)]
    ).sort(["period", "respondent", "type", "_ingested_at"], nulls_last=True)

    by_reason = {
        row["reject_reason"]: row["len"]
        for row in rejected.group_by("reject_reason").len().sort("reject_reason").to_dicts()
    }
    stats = CleaningStats(
        raw_rows=frame.height,
        clean_rows=clean.height,
        rejected_rows=rejected.height,
        rejected_by_reason=by_reason,
        null_counts={
            c: int(original.select(pl.col(c).null_count()).collect().item()) for c in RAW_COLUMNS
        },
    )
    logger.info(
        "clean_done raw=%d clean=%d rejected=%d reasons=%s",
        stats.raw_rows,
        stats.clean_rows,
        stats.rejected_rows,
        stats.rejected_by_reason,
    )
    return CleanResult(clean=clean, rejected=rejected, stats=stats)


def write_clean(result: CleanResult, clean_dir: Path) -> tuple[Path, Path]:
    """Escribe `observations.parquet` y `rejected.parquet` (reemplazo atomico de cada archivo)."""
    observations = clean_dir / OUTPUT_CLEAN
    rejected = clean_dir / OUTPUT_REJECTED
    atomic_write_parquet(result.clean, observations)
    atomic_write_parquet(result.rejected, rejected)
    return observations, rejected
