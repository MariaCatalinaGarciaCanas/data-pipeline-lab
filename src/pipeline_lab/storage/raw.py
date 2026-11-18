"""Capa Raw (Bronze): los datos de EIA tal como llegan, particionados por dia UTC.

Reglas:
* Se conservan los 7 campos originales como TEXTO (incluido `value`): aqui no se interpreta ni se
  corrige nada. La limpieza y los tipos son de la capa siguiente (`pipeline_lab.transform`).
* Se anade `_ingested_at` (UTC): cuando se descargo la fila. No contiene credenciales.
* Una particion es `date=YYYY-MM-DD/data.parquet`, con el dia UTC de `period`. Las filas cuyo
  `period` no se pueda interpretar van a `date=unknown/` (no se pierden).
* Reescribir una particion la REEMPLAZA entera de forma atomica. Es lo que hace idempotente la
  ingesta: repetir la misma ventana deja exactamente los mismos datos.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from pipeline_lab.storage.parquet_io import atomic_write_parquet

logger = logging.getLogger(__name__)

RAW_COLUMNS = (
    "period",
    "respondent",
    "respondent-name",
    "type",
    "type-name",
    "value",
    "value-units",
)
RAW_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    **{column: pl.String for column in RAW_COLUMNS},
    "_ingested_at": pl.Datetime("us", "UTC"),
}
UNKNOWN_PARTITION = "unknown"
PARTITION_FILE = "data.parquet"
_PERIOD = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{2}")


class RawStorageError(Exception):
    """Error al escribir la capa Raw."""


@dataclass(frozen=True, slots=True)
class WriteSummary:
    rows: int = 0
    partitions: dict[str, int] = field(default_factory=dict)  # "date=YYYY-MM-DD" -> filas
    replaced: tuple[str, ...] = ()  # particiones que ya existian y se reemplazaron


def partition_date(period: str | None) -> str:
    """Dia UTC (`YYYY-MM-DD`) de un `period` como `2026-11-10T05`, o `unknown` si no es valido."""
    match = _PERIOD.fullmatch(period or "")
    if match is None:
        return UNKNOWN_PARTITION
    try:
        date.fromisoformat(match.group(1))
    except ValueError:  # p. ej. 2026-02-31
        return UNKNOWN_PARTITION
    return match.group(1)


def rows_to_frame(rows: Iterable[Mapping[str, object]], ingested_at: datetime) -> pl.DataFrame:
    """Convierte filas de EIA en un DataFrame con el esquema Raw (todo texto + `_ingested_at`).

    Los campos que no estan en `RAW_COLUMNS` se ignoran; los que faltan quedan nulos.
    """
    if ingested_at.tzinfo is None:
        raise ValueError("ingested_at debe tener zona horaria (UTC)")
    records = [
        {column: (None if row.get(column) is None else str(row[column])) for column in RAW_COLUMNS}
        for row in rows
    ]
    frame = pl.DataFrame(records, schema={c: pl.String for c in RAW_COLUMNS})
    return frame.with_columns(
        pl.lit(ingested_at.astimezone(UTC)).cast(pl.Datetime("us", "UTC")).alias("_ingested_at")
    )


def write_raw_partitions(
    frame: pl.DataFrame, raw_dir: Path, *, overwrite: bool = True
) -> WriteSummary:
    """Escribe `frame` en `raw_dir/date=YYYY-MM-DD/data.parquet`, una particion por dia UTC.

    Args:
        frame: filas con el esquema Raw.
        raw_dir: carpeta base de la capa Raw.
        overwrite: si es True (por defecto) una particion existente se reemplaza entera y de
            forma atomica. Si es False y ya existe, se lanza `RawStorageError` SIN escribir nada.

    Un `frame` vacio no escribe ni borra nada: asi una extraccion fallida o vacia no puede
    vaciar datos buenos que ya existian.
    """
    missing = [c for c in RAW_SCHEMA if c not in frame.columns]
    if missing:
        raise RawStorageError(f"Faltan columnas Raw: {missing}")
    if frame.height == 0:
        logger.warning("raw_write_skipped reason=empty_frame")
        return WriteSummary()

    # Mismo criterio que `partition_date`, pero vectorizado (sin recorrer fila a fila en Python).
    day = pl.col("period").str.extract(r"^(\d{4}-\d{2}-\d{2})T\d{2}$", 1)
    is_real_date = day.str.to_date("%Y-%m-%d", strict=False).is_not_null()
    keyed = frame.select(list(RAW_SCHEMA)).with_columns(
        pl.when(is_real_date).then(day).otherwise(pl.lit(UNKNOWN_PARTITION)).alias("_partition")
    )
    partitions = {
        f"date={name}": part.drop("_partition").sort(["period", "respondent", "type"])
        for (name,), part in keyed.partition_by("_partition", as_dict=True).items()
    }

    existing = [name for name in partitions if (raw_dir / name / PARTITION_FILE).exists()]
    if existing and not overwrite:
        raise RawStorageError(
            f"Ya existen particiones y overwrite=False: {sorted(existing)}. No se escribio nada."
        )

    root = raw_dir.resolve()
    for name, part in partitions.items():
        target = (raw_dir / name / PARTITION_FILE).resolve()
        if root not in target.parents:  # defensa: nunca escribir fuera de la carpeta Raw
            raise RawStorageError("Ruta de particion fuera de la carpeta Raw")
        atomic_write_parquet(part, target)

    summary = WriteSummary(
        rows=frame.height,
        partitions={name: part.height for name, part in sorted(partitions.items())},
        replaced=tuple(sorted(existing)),
    )
    logger.info(
        "raw_write_done rows=%d partitions=%d replaced=%d",
        summary.rows,
        len(summary.partitions),
        len(summary.replaced),
    )
    return summary


def scan_raw(raw_dir: Path) -> pl.LazyFrame:
    """Lee toda la capa Raw de forma perezosa (LazyFrame). Falla si no hay ninguna particion."""
    files = sorted(raw_dir.glob(f"date=*/{PARTITION_FILE}"))
    if not files:
        raise RawStorageError(f"No hay datos Raw en {raw_dir}. Ejecuta antes `ingest`.")
    return pl.scan_parquet(files)
