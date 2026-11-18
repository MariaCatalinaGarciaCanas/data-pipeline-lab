"""Los tres pasos del pipeline, cada uno independiente y con su resumen.

    ingest     fuente (EIA o sintetica) -> Parquet Raw      [datos tal como llegan]
    transform  Parquet Raw -> Parquet Clean (+ rechazadas)  [Polars: tipos, validacion]
    load       Parquet Clean -> DuckDB                      [tablas de hechos y dimensiones]

Cada paso lee lo que dejo el anterior en disco, asi que se pueden ejecutar por separado, repetir
(son idempotentes) o encadenar. El orquestador de la linea de comandos esta en `cli.py`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline_lab.eia.extractor import DataSource, ExtractionRequest, extract_region_data
from pipeline_lab.layout import clean_dir, raw_dir, warehouse_path
from pipeline_lab.storage.raw import WriteSummary, rows_to_frame, scan_raw, write_raw_partitions
from pipeline_lab.transform.clean import CleaningStats, clean_observations, write_clean
from pipeline_lab.warehouse.duckdb_loader import LoadSummary, connect, load_clean

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestSummary:
    rows: int
    pages: int
    total_announced: int
    duplicates_dropped: int
    write: WriteSummary


@dataclass(frozen=True, slots=True)
class TransformSummary:
    stats: CleaningStats
    observations_path: Path
    rejected_path: Path


def ingest(
    source: DataSource,
    request: ExtractionRequest,
    data_dir: Path,
    *,
    now: datetime | None = None,
) -> IngestSummary:
    """Extrae la ventana pedida y la guarda en la capa Raw (reemplazando esas particiones)."""
    ingested_at = now or datetime.now(UTC)
    result = extract_region_data(source, request)
    frame = rows_to_frame(result.rows, ingested_at)
    write = write_raw_partitions(frame, raw_dir(data_dir), overwrite=True)
    return IngestSummary(
        rows=len(result.rows),
        pages=result.pages,
        total_announced=result.total,
        duplicates_dropped=result.duplicates_dropped,
        write=write,
    )


def transform(data_dir: Path) -> TransformSummary:
    """Lee toda la capa Raw, la limpia con Polars y escribe la capa Clean."""
    result = clean_observations(scan_raw(raw_dir(data_dir)))
    observations, rejected = write_clean(result, clean_dir(data_dir))
    return TransformSummary(
        stats=result.stats, observations_path=observations, rejected_path=rejected
    )


def load(data_dir: Path) -> LoadSummary:
    """Carga la capa Clean en DuckDB (`data/warehouse/analytics.duckdb`)."""
    con = connect(warehouse_path(data_dir))
    try:
        return load_clean(con, clean_dir(data_dir) / "observations.parquet")
    finally:
        con.close()
