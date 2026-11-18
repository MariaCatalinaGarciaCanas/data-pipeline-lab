"""Donde vive cada cosa dentro de la carpeta de datos (`DATA_DIR`, por defecto `data/`).

    data/
    ├── raw/eia/rto_region_data/date=YYYY-MM-DD/data.parquet   <- datos tal como llegan (Bronze)
    ├── clean/eia/rto_region_data/observations.parquet         <- limpios y tipados (Silver)
    ├── clean/eia/rto_region_data/rejected.parquet             <- filas descartadas, con motivo
    └── warehouse/analytics.duckdb                             <- almacen analitico (Gold + dbt)

Toda la carpeta `data/` esta en .gitignore: son datos generados y reproducibles.
"""

from __future__ import annotations

from pathlib import Path

DATASET = "rto_region_data"


def raw_dir(data_dir: Path) -> Path:
    return data_dir / "raw" / "eia" / DATASET


def clean_dir(data_dir: Path) -> Path:
    return data_dir / "clean" / "eia" / DATASET


def warehouse_path(data_dir: Path) -> Path:
    return data_dir / "warehouse" / "analytics.duckdb"
