"""Pruebas de la capa Raw: particionado por dia UTC, escritura atomica y reemplazo controlado."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline_lab.storage import (
    RAW_COLUMNS,
    RawStorageError,
    atomic_write_parquet,
    partition_date,
    rows_to_frame,
    scan_raw,
    write_raw_partitions,
)

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def make_row(period: str, respondent: str = "CISO", type_: str = "D", value: str | None = "100"):
    return {
        "period": period,
        "respondent": respondent,
        "respondent-name": "Nombre",
        "type": type_,
        "type-name": "Demand",
        "value": value,
        "value-units": "megawatthours",
    }


def frame(*periods: str) -> pl.DataFrame:
    return rows_to_frame([make_row(p) for p in periods], NOW)


def files(root: Path) -> list[str]:
    return sorted(
        str(p.relative_to(root)).replace("\\", "/") for p in root.rglob("*") if p.is_file()
    )


# ================================================================ interpretacion del periodo
@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("2026-11-10T05", "2026-11-10"),
        ("2026-11-10T00", "2026-11-10"),
        ("2024-02-29T23", "2024-02-29"),  # ano bisiesto
        ("2026-02-31T05", "unknown"),  # fecha imposible
        ("2026-11-10", "unknown"),
        ("2026-11-10T5", "unknown"),
        ("2026-11-10T05:00", "unknown"),
        ("garbage", "unknown"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_partition_date(period: str | None, expected: str) -> None:
    assert partition_date(period) == expected


# ====================================================================== filas -> DataFrame
def test_rows_to_frame_keeps_the_original_text_and_adds_ingestion_time() -> None:
    df = rows_to_frame([make_row("2026-11-10T05", value="27053")], NOW)

    assert df.columns == [*RAW_COLUMNS, "_ingested_at"]
    assert df.schema["value"] == pl.String  # NO se interpreta como numero en Raw
    assert df["value"].to_list() == ["27053"]
    assert df.schema["_ingested_at"] == pl.Datetime("us", "UTC")
    assert df["_ingested_at"].to_list() == [NOW]


def test_rows_to_frame_preserves_nulls_ignores_extra_fields_and_fills_missing_ones() -> None:
    rows = [{"period": "2026-11-10T05", "value": None, "campo-extra": "x"}]

    df = rows_to_frame(rows, NOW)

    assert "campo-extra" not in df.columns
    assert df["value"].to_list() == [None]
    assert df["respondent"].to_list() == [None]  # faltaba en la fila


def test_rows_to_frame_converts_to_utc() -> None:
    from datetime import timedelta, timezone

    local = datetime(2026, 11, 16, 14, 0, tzinfo=timezone(timedelta(hours=2)))

    assert rows_to_frame([make_row("2026-11-10T05")], local)["_ingested_at"].to_list() == [NOW]


def test_rows_to_frame_requires_an_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="zona horaria"):
        rows_to_frame([make_row("2026-11-10T05")], datetime(2026, 11, 16, 12, 0))


def test_rows_to_frame_accepts_no_rows() -> None:
    assert rows_to_frame([], NOW).height == 0


# ============================================================================ escritura
def test_writes_one_partition_per_utc_day(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    summary = write_raw_partitions(frame("2026-11-10T23", "2026-11-11T00", "2026-11-11T05"), raw)

    assert files(raw) == [
        "date=2026-11-10/data.parquet",
        "date=2026-11-11/data.parquet",
    ]
    assert summary.rows == 3
    assert summary.partitions == {"date=2026-11-10": 1, "date=2026-11-11": 2}
    assert summary.replaced == ()


def test_creates_missing_directories_automatically(tmp_path: Path) -> None:
    raw = tmp_path / "a" / "b" / "raw"

    write_raw_partitions(frame("2026-11-10T05"), raw)

    assert (raw / "date=2026-11-10" / "data.parquet").is_file()


def test_partition_contents_match_the_input_and_are_sorted(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    rows = [make_row("2026-11-10T07", "PJM"), make_row("2026-11-10T05", "CISO")]
    write_raw_partitions(rows_to_frame(rows, NOW), raw)

    stored = pl.read_parquet(raw / "date=2026-11-10" / "data.parquet")

    assert stored["period"].to_list() == ["2026-11-10T05", "2026-11-10T07"]
    assert stored.schema["value"] == pl.String
    assert stored.columns == [*RAW_COLUMNS, "_ingested_at"]  # y nada mas: sin credenciales


def test_rows_with_an_uninterpretable_period_go_to_the_unknown_partition(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    summary = write_raw_partitions(frame("2026-11-10T05", "garbage", "2026-02-31T01"), raw)

    assert summary.partitions == {"date=2026-11-10": 1, "date=unknown": 2}
    assert (raw / "date=unknown" / "data.parquet").is_file()


def test_rewriting_replaces_the_partition_and_is_idempotent(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    data = frame("2026-11-10T05", "2026-11-10T06")
    write_raw_partitions(data, raw)
    first = (raw / "date=2026-11-10" / "data.parquet").read_bytes()

    summary = write_raw_partitions(data, raw)

    assert summary.replaced == ("date=2026-11-10",)
    assert (raw / "date=2026-11-10" / "data.parquet").read_bytes() == first
    assert pl.read_parquet(raw / "date=2026-11-10" / "data.parquet").height == 2  # no se duplica


def test_a_new_window_replaces_only_its_own_partitions(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_raw_partitions(frame("2026-11-10T05", "2026-11-11T05"), raw)
    untouched = (raw / "date=2026-11-10" / "data.parquet").read_bytes()

    write_raw_partitions(frame("2026-11-11T05", "2026-11-11T06"), raw)

    assert (raw / "date=2026-11-10" / "data.parquet").read_bytes() == untouched
    assert pl.read_parquet(raw / "date=2026-11-11" / "data.parquet").height == 2


def test_overwrite_false_refuses_to_replace_and_writes_nothing(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_raw_partitions(frame("2026-11-10T05"), raw)
    before = (raw / "date=2026-11-10" / "data.parquet").read_bytes()

    with pytest.raises(RawStorageError, match="overwrite=False"):
        write_raw_partitions(frame("2026-11-10T09", "2026-11-12T05"), raw, overwrite=False)

    assert (raw / "date=2026-11-10" / "data.parquet").read_bytes() == before
    assert not (raw / "date=2026-11-12").exists()  # ni siquiera la particion nueva


def test_an_empty_frame_writes_and_deletes_nothing(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_raw_partitions(frame("2026-11-10T05"), raw)
    before = files(raw)

    summary = write_raw_partitions(rows_to_frame([], NOW), raw)

    assert summary.rows == 0
    assert files(raw) == before


def test_missing_columns_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(RawStorageError, match="Faltan columnas"):
        write_raw_partitions(frame("2026-11-10T05").drop("value"), tmp_path / "raw")


# ============================================================== sin archivos parciales
def test_no_temporary_files_remain_after_a_successful_write(tmp_path: Path) -> None:
    raw = tmp_path / "raw"

    write_raw_partitions(frame("2026-11-10T05", "2026-11-11T05"), raw)

    assert all(".tmp" not in name for name in files(raw))


def test_a_failed_write_leaves_the_previous_file_intact_and_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    write_raw_partitions(frame("2026-11-10T05"), raw)
    target = raw / "date=2026-11-10" / "data.parquet"
    before = target.read_bytes()

    def exploding_write(self: pl.DataFrame, file: Path, *args: object, **kwargs: object) -> None:
        Path(file).write_bytes(b"PARCIAL")  # simula un archivo a medio escribir
        raise OSError("disco lleno")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", exploding_write)

    with pytest.raises(OSError, match="disco lleno"):
        write_raw_partitions(frame("2026-11-10T05", "2026-11-10T06"), raw)

    assert target.read_bytes() == before  # el dato anterior sigue completo
    assert files(raw) == ["date=2026-11-10/data.parquet"]  # y no queda ningun temporal


def test_atomic_write_creates_parent_directories(tmp_path: Path) -> None:
    path = tmp_path / "x" / "y" / "out.parquet"

    atomic_write_parquet(pl.DataFrame({"a": [1, 2]}), path)

    assert pl.read_parquet(path)["a"].to_list() == [1, 2]


def test_a_partition_path_cannot_escape_the_raw_directory(tmp_path: Path) -> None:
    # La fecha de la particion se valida con una expresion regular estricta: ".." nunca llega
    # a formar parte de una ruta; se trata como "unknown".
    raw = tmp_path / "raw"

    summary = write_raw_partitions(frame("../../evil"), raw)

    assert summary.partitions == {"date=unknown": 1}
    assert all(raw in p.parents for p in tmp_path.rglob("*.parquet"))


# ==================================================================================== lectura
def test_scan_raw_reads_every_partition_lazily(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_raw_partitions(frame("2026-11-10T05", "2026-11-11T05", "2026-11-12T05"), raw)

    lazy = scan_raw(raw)

    assert isinstance(lazy, pl.LazyFrame)
    assert lazy.collect().height == 3


def test_scan_raw_without_data_explains_what_to_do(tmp_path: Path) -> None:
    with pytest.raises(RawStorageError, match="ingest"):
        scan_raw(tmp_path / "raw")
