"""Integracion con dbt: los modelos y tests de dbt se ejecutan sobre un almacen sintetico.

Cada prueba trabaja en una carpeta temporal (almacen, `target/` y logs propios): no toca
`data/` ni `dbt/target/` del proyecto. No hay red ni API key.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from pipeline_lab import pipeline
from pipeline_lab.eia import ExtractionRequest
from pipeline_lab.layout import warehouse_path
from pipeline_lab.synthetic import SyntheticSource

pytest.importorskip("dbt.cli.main", reason="dbt no esta instalado (grupo de dependencias 'dbt')")

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
WEEK = ExtractionRequest(start=date(2026, 11, 4), end=date(2026, 11, 10))
NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def dbt(command: str, warehouse: Path, work: Path) -> subprocess.CompletedProcess[str]:
    executable = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    env = {**os.environ, "DBT_DUCKDB_PATH": str(warehouse)}
    return subprocess.run(
        [
            str(executable),
            command,
            "--profiles-dir",
            str(DBT_DIR),
            "--target-path",
            str(work / "target"),
            "--log-path",
            str(work / "logs"),
            "--no-use-colors",
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    pipeline.ingest(SyntheticSource(), WEEK, data_dir, now=NOW)
    pipeline.transform(data_dir)
    pipeline.load(data_dir)
    return warehouse_path(data_dir)


def query(warehouse: Path, sql: str):
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_dbt_build_runs_every_model_and_test(warehouse: Path, tmp_path: Path) -> None:
    result = dbt("build", warehouse, tmp_path)

    assert result.returncode == 0, result.stdout[-3000:]
    assert "ERROR=0" in result.stdout
    assert "FAIL" not in result.stdout


def test_marts_contain_what_they_promise(warehouse: Path, tmp_path: Path) -> None:
    assert dbt("build", warehouse, tmp_path).returncode == 0

    per_region_hours = 7 * 24
    counts = dict(
        query(
            warehouse,
            """
        SELECT 'demand', count(*) FROM marts.mart_hourly_demand
        UNION ALL SELECT 'generation', count(*) FROM marts.mart_hourly_generation
        UNION ALL SELECT 'interchange_days', count(*) FROM marts.mart_interchange_by_region
        UNION ALL SELECT 'demand_vs_forecast', count(*) FROM marts.mart_demand_vs_forecast
        """,
        )
    )
    assert counts["demand"] == 4 * per_region_hours
    assert counts["generation"] == 4 * per_region_hours
    assert counts["interchange_days"] == 4 * 7  # regiones x dias
    assert counts["demand_vs_forecast"] == 4 * per_region_hours  # todas tienen ambos tipos


def test_the_marts_do_not_mix_categories(warehouse: Path, tmp_path: Path) -> None:
    assert dbt("build", warehouse, tmp_path).returncode == 0

    # La demanda del mart debe coincidir EXACTAMENTE con los hechos de tipo D, y nada mas.
    mismatches = query(
        warehouse,
        """
        SELECT count(*) FROM marts.mart_hourly_demand AS m
        LEFT JOIN core.fct_observation AS f
          ON f.period_utc = m.period_utc AND f.respondent = m.respondent AND f.type = 'D'
        WHERE f.value IS DISTINCT FROM m.demand_mwh
        """,
    )
    assert mismatches == [(0,)]


def test_negative_interchange_reaches_the_mart(warehouse: Path, tmp_path: Path) -> None:
    assert dbt("build", warehouse, tmp_path).returncode == 0

    exporting_hours, min_value = query(
        warehouse,
        "SELECT sum(hours_exporting), min(min_hourly_interchange_mwh) "
        "FROM marts.mart_interchange_by_region",
    )[0]
    assert exporting_hours > 0
    assert min_value < 0


def test_forecast_error_has_the_documented_sign(warehouse: Path, tmp_path: Path) -> None:
    assert dbt("build", warehouse, tmp_path).returncode == 0

    wrong_sign = query(
        warehouse,
        "SELECT count(*) FROM marts.mart_demand_vs_forecast "
        "WHERE forecast_error_mwh <> forecast_mwh - demand_mwh",
    )
    assert wrong_sign == [(0,)]


def test_dbt_tests_detect_an_orphan_row(warehouse: Path, tmp_path: Path) -> None:
    """Los tests de dbt tienen dientes: una region que no esta en la dimension los hace fallar."""
    assert dbt("build", warehouse, tmp_path).returncode == 0
    con = duckdb.connect(str(warehouse))
    try:
        con.execute(
            "INSERT INTO core.fct_observation VALUES "
            "('2026-12-01 00:00', 'HUERFANA', 'D', 10, 'megawatthours', now())"
        )
    finally:
        con.close()

    result = dbt("test", warehouse, tmp_path)

    assert result.returncode != 0
    assert "relationships_stg_observations_respondent" in result.stdout


def test_dbt_tests_detect_a_duplicate_key(warehouse: Path, tmp_path: Path) -> None:
    """Si el almacen perdiera su clave primaria y entrara un duplicado, dbt lo detecta."""
    con = duckdb.connect(str(warehouse))
    try:
        # Se reconstruye la tabla SIN clave primaria para poder forzar un duplicado.
        con.execute("CREATE TABLE core.tmp AS SELECT * FROM core.fct_observation")
        con.execute("DROP VIEW core.dim_time")
        con.execute("DROP TABLE core.fct_observation")
        con.execute("ALTER TABLE core.tmp RENAME TO fct_observation")
        con.execute("INSERT INTO core.fct_observation SELECT * FROM core.fct_observation LIMIT 1")
    finally:
        con.close()

    result = dbt("test", warehouse, tmp_path)

    assert result.returncode != 0
    assert "unique_combination" in result.stdout or "unique_stg_observations" in result.stdout
