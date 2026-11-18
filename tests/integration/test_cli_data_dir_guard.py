"""La CLI impide mezclar datos sinteticos con datos reales en la misma carpeta."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline_lab.cli import SYNTHETIC_MARKER, main
from pipeline_lab.layout import raw_dir
from pipeline_lab.storage import rows_to_frame, write_raw_partitions

DAY = ["--start", "2026-11-10", "--end", "2026-11-10"]


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)  # sin .env
    monkeypatch.delenv("EIA_API_KEY", raising=False)


def put_real_data(data_dir: Path) -> Path:
    """Simula datos 'reales' ya presentes (sin marca sintetica) en la capa Raw."""
    row = {
        "period": "2026-11-10T05",
        "respondent": "REAL",
        "respondent-name": "Region real",
        "type": "D",
        "type-name": "Demand",
        "value": "1",
        "value-units": "megawatthours",
    }
    write_raw_partitions(
        rows_to_frame([row], datetime(2026, 11, 16, tzinfo=UTC)), raw_dir(data_dir)
    )
    return raw_dir(data_dir) / "date=2026-11-10" / "data.parquet"


def test_synthetic_ingest_marks_the_directory(tmp_path: Path) -> None:
    out = tmp_path / "data"

    assert main(["ingest", "--synthetic", *DAY, "--output-dir", str(out)]) == 0

    assert (out / SYNTHETIC_MARKER).is_file()


def test_synthetic_refuses_to_overwrite_real_partitions(tmp_path: Path, capsys) -> None:
    out = tmp_path / "data"
    real = put_real_data(out)
    before = real.read_bytes()

    code = main(["ingest", "--synthetic", *DAY, "--output-dir", str(out)])

    assert code == 1
    assert "no son sinteticos" in capsys.readouterr().err
    assert real.read_bytes() == before  # el dato real sigue intacto
    assert pl.read_parquet(real)["respondent"].to_list() == ["REAL"]
    assert not (out / SYNTHETIC_MARKER).exists()


def test_live_refuses_to_write_into_a_synthetic_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    out = tmp_path / "data"
    assert main(["ingest", "--synthetic", *DAY, "--output-dir", str(out)]) == 0
    monkeypatch.setenv("EIA_API_KEY", "fake-key-for-the-guard-test-0123456789")

    code = main(["ingest", "--live", *DAY, "--output-dir", str(out)])

    assert code == 1
    assert "SINTETICOS" in capsys.readouterr().err


def test_repeating_a_synthetic_run_in_the_same_directory_is_allowed(tmp_path: Path) -> None:
    out = tmp_path / "data"

    assert main(["run", "--synthetic", *DAY, "--output-dir", str(out)]) == 0
    assert main(["run", "--synthetic", *DAY, "--output-dir", str(out)]) == 0


def test_synthetic_into_a_fresh_or_empty_directory_is_allowed(tmp_path: Path) -> None:
    assert main(["ingest", "--synthetic", *DAY, "--output-dir", str(tmp_path / "nuevo")]) == 0
