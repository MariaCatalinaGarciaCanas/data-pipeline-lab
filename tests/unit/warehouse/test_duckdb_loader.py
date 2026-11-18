"""Pruebas de la carga en DuckDB: esquema, granularidad, idempotencia y transacciones."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pipeline_lab.storage import rows_to_frame
from pipeline_lab.transform import clean_observations
from pipeline_lab.warehouse import SCHEMA, connect, init_schema, load_clean

T1 = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 11, 16, 11, 0, tzinfo=UTC)


def row(period="2026-11-10T05", respondent="CISO", type_="D", value="100", name="California ISO"):
    return {
        "period": period,
        "respondent": respondent,
        "respondent-name": name,
        "type": type_,
        "type-name": "x",
        "value": value,
        "value-units": "megawatthours",
    }


def clean_file(tmp_path: Path, rows: list[dict], name: str, at: datetime = T1) -> Path:
    clean = clean_observations(rows_to_frame(rows, at)).clean
    path = tmp_path / name
    clean.write_parquet(path)
    return path


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


# ================================================================================= esquema
def test_init_schema_creates_tables_and_view_and_is_idempotent(con) -> None:
    init_schema(con)
    init_schema(con)  # repetir no falla

    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [SCHEMA]
        ).fetchall()
    }
    assert tables == {"dim_respondent", "dim_observation_type", "fct_observation", "dim_time"}


def test_the_fact_table_primary_key_enforces_the_declared_granularity(con) -> None:
    init_schema(con)
    insert = (
        "INSERT INTO core.fct_observation VALUES ('2026-11-10 05:00', 'CISO', 'D', 1, 'mwh', now())"
    )
    con.execute(insert)

    with pytest.raises(duckdb.ConstraintException):
        con.execute(insert)  # misma hora x region x tipo: no se admite


def test_fact_columns_reject_nulls(con) -> None:
    init_schema(con)

    with pytest.raises(duckdb.ConstraintException):
        con.execute(
            "INSERT INTO core.fct_observation VALUES ('2026-11-10', 'A', 'D', NULL, 'u', now())"
        )


# ================================================================================== carga
def test_load_populates_facts_and_dimensions(con, tmp_path: Path) -> None:
    rows = [row(type_=t, respondent=c) for t in ("D", "DF", "NG", "TI") for c in ("CISO", "PJM")]
    path = clean_file(tmp_path, rows, "obs.parquet")

    summary = load_clean(con, path)

    assert (summary.rows_in_file, summary.inserted, summary.replaced) == (8, 8, 0)
    assert summary.fact_rows_after == 8
    assert (summary.respondents, summary.types) == (2, 4)
    categories = dict(
        con.execute("SELECT type, category FROM core.dim_observation_type").fetchall()
    )
    assert categories == {
        "D": "demand",
        "DF": "demand_forecast",
        "NG": "generation",
        "TI": "interchange",
    }


def test_loading_the_same_file_twice_does_not_duplicate_anything(con, tmp_path: Path) -> None:
    path = clean_file(tmp_path, [row(), row(period="2026-11-10T06")], "obs.parquet")

    first = load_clean(con, path)
    second = load_clean(con, path)

    assert (first.inserted, first.replaced) == (2, 0)
    assert (second.inserted, second.replaced) == (0, 2)
    assert scalar(con, "SELECT count(*) FROM core.fct_observation") == 2
    assert scalar(con, "SELECT count(*) FROM core.dim_respondent") == 1


def test_loading_new_data_adds_rows_and_keeps_the_old_ones(con, tmp_path: Path) -> None:
    load_clean(con, clean_file(tmp_path, [row()], "day1.parquet"))

    summary = load_clean(
        con, clean_file(tmp_path, [row(), row(period="2026-11-11T05")], "both.parquet")
    )

    assert (summary.inserted, summary.replaced) == (1, 1)
    assert scalar(con, "SELECT count(*) FROM core.fct_observation") == 2


def test_a_corrected_value_replaces_the_old_one(con, tmp_path: Path) -> None:
    load_clean(con, clean_file(tmp_path, [row(value="100")], "v1.parquet", T1))

    load_clean(con, clean_file(tmp_path, [row(value="175")], "v2.parquet", T2))

    assert scalar(con, "SELECT value FROM core.fct_observation") == 175.0
    assert scalar(con, "SELECT count(*) FROM core.fct_observation") == 1


def test_the_respondent_name_is_updated_to_the_latest(con, tmp_path: Path) -> None:
    load_clean(con, clean_file(tmp_path, [row(name="Nombre viejo")], "a.parquet", T1))
    load_clean(con, clean_file(tmp_path, [row(name="Nombre nuevo")], "b.parquet", T2))

    assert scalar(con, "SELECT respondent_name FROM core.dim_respondent") == "Nombre nuevo"


def test_negative_interchange_is_loaded_unchanged(con, tmp_path: Path) -> None:
    load_clean(con, clean_file(tmp_path, [row(type_="TI", value="-504")], "ti.parquet"))

    assert scalar(con, "SELECT value FROM core.fct_observation") == -504.0


def test_timestamps_are_stored_as_naive_utc_without_shifting(con, tmp_path: Path) -> None:
    load_clean(con, clean_file(tmp_path, [row(period="2026-11-10T23")], "t.parquet"))

    stored = scalar(con, "SELECT period_utc FROM core.fct_observation")
    assert stored == datetime(2026, 11, 10, 23, 0)
    assert stored.tzinfo is None


def test_dim_time_has_one_row_per_distinct_hour(con, tmp_path: Path) -> None:
    rows = [
        row(period="2026-11-10T05"),
        row(period="2026-11-10T05", respondent="PJM"),
        row(period="2026-11-11T00"),
    ]
    load_clean(con, clean_file(tmp_path, rows, "t.parquet"))

    result = con.execute(
        "SELECT date_utc, hour_utc FROM core.dim_time ORDER BY period_utc"
    ).fetchall()

    assert [(str(d), h) for d, h in result] == [("2026-11-10", 5), ("2026-11-11", 0)]


def test_a_missing_file_gives_a_helpful_error(con, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="transform"):
        load_clean(con, tmp_path / "nope.parquet")


def test_a_failed_load_rolls_back_everything(con, tmp_path: Path) -> None:
    # Un Parquet con un valor nulo viola NOT NULL en la tabla de hechos DESPUES de haber
    # insertado las dimensiones: la transaccion debe deshacerlo todo.
    bad = pl.DataFrame(
        {
            "period_utc": [datetime(2026, 11, 10, 5)],
            "respondent": ["CISO"],
            "respondent_name": ["California ISO"],
            "type": ["D"],
            "type_name": ["Demand"],
            "category": ["demand"],
            "value": [None],
            "value_units": ["megawatthours"],
            "ingested_at": [datetime(2026, 11, 16, 10)],
        },
        schema_overrides={"value": pl.Float64},
    )
    path = tmp_path / "bad.parquet"
    bad.write_parquet(path)

    with pytest.raises(duckdb.ConstraintException):
        load_clean(con, path)

    assert scalar(con, "SELECT count(*) FROM core.fct_observation") == 0
    assert scalar(con, "SELECT count(*) FROM core.dim_respondent") == 0
    assert scalar(con, "SELECT count(*) FROM core.dim_observation_type") == 0


def test_connect_creates_the_database_folder(tmp_path: Path) -> None:
    db = tmp_path / "nested" / "warehouse" / "analytics.duckdb"

    connection = connect(db)
    init_schema(connection)
    connection.close()

    assert db.is_file()
