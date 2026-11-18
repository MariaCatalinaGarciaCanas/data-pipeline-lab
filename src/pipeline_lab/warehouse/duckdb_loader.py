"""Carga de los datos limpios en DuckDB (esquema `core`).

Modelo (estrella pequena)
-------------------------
    core.fct_observation        HECHOS. GRANULARIDAD: una fila por
                                hora (UTC) x region (respondent) x tipo de observacion.
                                Clave primaria: (period_utc, respondent, type).
    core.dim_respondent         una fila por region / autoridad de balance.
    core.dim_observation_type   una fila por tipo (D, DF, NG, TI) con su categoria.
    core.dim_time               VISTA: una fila por hora presente en los hechos.

Idempotencia: la carga usa `INSERT OR REPLACE` sobre la clave primaria. Cargar dos veces los
mismos datos deja las mismas filas (no duplica); cargar una version corregida de una
observacion la reemplaza. La carga entera ocurre dentro de una transaccion: o entra todo o nada.

Las columnas de tiempo son TIMESTAMP SIN zona y significan UTC por convenio.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SCHEMA = "core"

DDL = (
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_respondent (
        respondent       VARCHAR PRIMARY KEY,
        respondent_name  VARCHAR NOT NULL
    )""",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_observation_type (
        type       VARCHAR PRIMARY KEY,
        type_name  VARCHAR NOT NULL,
        category   VARCHAR NOT NULL
    )""",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fct_observation (
        period_utc   TIMESTAMP NOT NULL,   -- hora de la observacion, en UTC
        respondent   VARCHAR   NOT NULL,   -- region / autoridad de balance
        type         VARCHAR   NOT NULL,   -- D, DF, NG o TI
        value        DOUBLE    NOT NULL,   -- megavatios-hora; TI puede ser negativo
        value_units  VARCHAR   NOT NULL,
        ingested_at  TIMESTAMP NOT NULL,   -- cuando se descargo la fila (UTC)
        PRIMARY KEY (period_utc, respondent, type)
    )""",
    f"""
    CREATE OR REPLACE VIEW {SCHEMA}.dim_time AS
    SELECT DISTINCT
        period_utc,
        CAST(period_utc AS DATE)       AS date_utc,
        hour(period_utc)               AS hour_utc,
        isodow(period_utc)             AS iso_day_of_week,   -- 1 = lunes ... 7 = domingo
        strftime(period_utc, '%Y-%m')  AS year_month
    FROM {SCHEMA}.fct_observation""",
)


@dataclass(frozen=True, slots=True)
class LoadSummary:
    rows_in_file: int  # filas del Parquet limpio
    inserted: int  # observaciones nuevas
    replaced: int  # observaciones que ya existian y se sobrescribieron
    fact_rows_after: int  # total de hechos tras la carga
    respondents: int
    types: int


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Abre (o crea) la base de datos DuckDB, creando su carpeta si hace falta."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Crea esquema, tablas y vista si no existen. Es idempotente."""
    for statement in DDL:
        con.execute(statement)


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"SELECT count(*) FROM {SCHEMA}.{table}").fetchone()
    assert row is not None
    return int(row[0])


def load_clean(con: duckdb.DuckDBPyConnection, observations_parquet: Path) -> LoadSummary:
    """Carga `observations.parquet` en las dimensiones y la tabla de hechos. Idempotente."""
    if not observations_parquet.is_file():
        raise FileNotFoundError(f"No existe {observations_parquet}. Ejecuta antes `transform`.")
    init_schema(con)
    source = str(observations_parquet)

    row = con.execute("SELECT count(*) FROM read_parquet(?)", [source]).fetchone()
    assert row is not None
    rows_in_file = int(row[0])
    before = _count(con, "fct_observation")

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"""
            INSERT OR REPLACE INTO {SCHEMA}.dim_respondent
            SELECT respondent, arg_max(respondent_name, ingested_at)
            FROM read_parquet(?)
            GROUP BY respondent""",
            [source],
        )
        con.execute(
            f"""
            INSERT OR REPLACE INTO {SCHEMA}.dim_observation_type
            SELECT type, arg_max(type_name, ingested_at), arg_max(category, ingested_at)
            FROM read_parquet(?)
            GROUP BY type""",
            [source],
        )
        con.execute(
            f"""
            INSERT OR REPLACE INTO {SCHEMA}.fct_observation
            SELECT period_utc, respondent, type, value, value_units, ingested_at
            FROM read_parquet(?)""",
            [source],
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise

    after = _count(con, "fct_observation")
    inserted = after - before
    summary = LoadSummary(
        rows_in_file=rows_in_file,
        inserted=inserted,
        replaced=rows_in_file - inserted,
        fact_rows_after=after,
        respondents=_count(con, "dim_respondent"),
        types=_count(con, "dim_observation_type"),
    )
    logger.info(
        "warehouse_load_done rows_in_file=%d inserted=%d replaced=%d fact_rows=%d",
        summary.rows_in_file,
        summary.inserted,
        summary.replaced,
        summary.fact_rows_after,
    )
    return summary
