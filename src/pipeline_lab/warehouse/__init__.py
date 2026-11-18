"""Almacen analitico local en DuckDB."""

from pipeline_lab.warehouse.duckdb_loader import (
    SCHEMA,
    LoadSummary,
    connect,
    init_schema,
    load_clean,
)

__all__ = ["SCHEMA", "LoadSummary", "connect", "init_schema", "load_clean"]
