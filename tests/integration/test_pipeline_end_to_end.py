"""Integracion: Fuente -> extractor -> Parquet Raw -> Polars -> Parquet Clean -> DuckDB.

Todo con datos sinteticos o respuestas HTTP simuladas con respx. Nunca se llama a EIA.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx
import polars as pl
import pytest
import respx

from pipeline_lab import pipeline
from pipeline_lab.cli import main
from pipeline_lab.config import Secret
from pipeline_lab.eia import EIAClient, ExtractionRequest
from pipeline_lab.layout import clean_dir, raw_dir, warehouse_path
from pipeline_lab.synthetic import SyntheticSource

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
WEEK = ExtractionRequest(start=date(2026, 11, 4), end=date(2026, 11, 10))
ROWS_PER_WEEK = 7 * 24 * 4 * 4  # dias x horas x regiones x tipos
REPO_ROOT = Path(__file__).resolve().parents[2]


def fact_count(data_dir: Path) -> int:
    con = duckdb.connect(str(warehouse_path(data_dir)), read_only=True)
    try:
        return con.execute("SELECT count(*) FROM core.fct_observation").fetchone()[0]
    finally:
        con.close()


def run_all(source, request, data_dir: Path):
    ingest = pipeline.ingest(source, request, data_dir, now=NOW)
    transform = pipeline.transform(data_dir)
    load = pipeline.load(data_dir)
    return ingest, transform, load


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ======================================================================== flujo completo
def test_full_pipeline_with_synthetic_data(tmp_path: Path) -> None:
    ingest, transform, load = run_all(SyntheticSource(), WEEK, tmp_path)

    assert ingest.rows == ROWS_PER_WEEK
    assert len(ingest.write.partitions) == 7  # una particion por dia UTC
    assert transform.stats.clean_rows == ROWS_PER_WEEK
    assert transform.stats.rejected_rows == 0
    assert load.fact_rows_after == ROWS_PER_WEEK
    assert (load.respondents, load.types) == (4, 4)
    assert sorted(p.name for p in raw_dir(tmp_path).iterdir()) == [
        f"date=2026-11-{d:02d}" for d in range(4, 11)
    ]


def test_anomalies_are_quarantined_not_loaded_and_not_lost(tmp_path: Path) -> None:
    ingest, transform, load = run_all(SyntheticSource(anomalies=True), WEEK, tmp_path)

    assert ingest.duplicates_dropped == 1
    assert transform.stats.rejected_by_reason == {"invalid_value": 2}
    assert load.fact_rows_after == ROWS_PER_WEEK - 2
    rejected = pl.read_parquet(transform.rejected_path)
    assert sorted(rejected["value"].fill_null("<nulo>").to_list()) == ["<nulo>", "n/a"]


def test_negative_interchange_survives_the_whole_pipeline(tmp_path: Path) -> None:
    run_all(SyntheticSource(), WEEK, tmp_path)

    con = duckdb.connect(str(warehouse_path(tmp_path)), read_only=True)
    try:
        negatives = con.execute(
            "SELECT count(*) FROM core.fct_observation WHERE type = 'TI' AND value < 0"
        ).fetchone()[0]
        other_negatives = con.execute(
            "SELECT count(*) FROM core.fct_observation WHERE type <> 'TI' AND value < 0"
        ).fetchone()[0]
    finally:
        con.close()
    assert negatives > 0
    assert other_negatives == 0


# ==================================================================== idempotencia
def test_running_the_pipeline_twice_changes_nothing(tmp_path: Path) -> None:
    run_all(SyntheticSource(), WEEK, tmp_path)
    raw_before = {p.name: digest(p / "data.parquet") for p in raw_dir(tmp_path).iterdir()}
    clean_before = digest(clean_dir(tmp_path) / "observations.parquet")

    ingest, _, load = run_all(SyntheticSource(), WEEK, tmp_path)

    assert {p.name: digest(p / "data.parquet") for p in raw_dir(tmp_path).iterdir()} == raw_before
    assert digest(clean_dir(tmp_path) / "observations.parquet") == clean_before
    assert len(ingest.write.replaced) == 7
    assert (load.inserted, load.replaced) == (0, ROWS_PER_WEEK)
    assert fact_count(tmp_path) == ROWS_PER_WEEK


def test_overlapping_windows_do_not_duplicate_rows(tmp_path: Path) -> None:
    first = ExtractionRequest(start=date(2026, 11, 4), end=date(2026, 11, 7))
    second = ExtractionRequest(start=date(2026, 11, 6), end=date(2026, 11, 10))

    run_all(SyntheticSource(), first, tmp_path)
    run_all(SyntheticSource(), second, tmp_path)

    # La union de [4..7] y [6..10] son 7 dias distintos (4..10), sin repetir el 6 y el 7.
    assert fact_count(tmp_path) == ROWS_PER_WEEK


def test_reingesting_a_window_replaces_only_its_partitions(tmp_path: Path) -> None:
    run_all(SyntheticSource(), WEEK, tmp_path)
    untouched = digest(raw_dir(tmp_path) / "date=2026-11-04" / "data.parquet")

    one_day = ExtractionRequest(start=date(2026, 11, 8), end=date(2026, 11, 8))
    pipeline.ingest(SyntheticSource(), one_day, tmp_path, now=datetime(2026, 11, 17, tzinfo=UTC))

    assert digest(raw_dir(tmp_path) / "date=2026-11-04" / "data.parquet") == untouched


def test_an_empty_extraction_does_not_wipe_existing_data(tmp_path: Path) -> None:
    run_all(SyntheticSource(), WEEK, tmp_path)
    nobody = ExtractionRequest(
        start=date(2026, 11, 4), end=date(2026, 11, 10), respondents=("NOPE",)
    )

    ingest = pipeline.ingest(SyntheticSource(), nobody, tmp_path, now=NOW)

    assert ingest.rows == 0
    assert len(list(raw_dir(tmp_path).glob("date=*/data.parquet"))) == 7


# ============================================================ HTTP real (simulado con respx)
def _serve_synthetic(request: httpx.Request) -> httpx.Response:
    """Servidor falso de EIA: traduce los parametros HTTP a la fuente sintetica y pagina."""
    params = request.url.params
    page = SyntheticSource().get_data(
        frequency=params["frequency"],
        facets={"type": params.get_list("facets[type][]")},
        start=params["start"],
        end=params["end"],
        offset=int(params["offset"]),
        length=int(params["length"]),
    )
    body = {
        "response": {
            "total": str(page.total),  # EIA devuelve el total como texto
            "frequency": "hourly",
            "data": [dict(r) for r in page.rows],
        },
        "request": {"params": {}},
    }
    return httpx.Response(200, json=body)


def test_full_pipeline_through_the_real_http_client_with_mocked_responses(tmp_path: Path) -> None:
    key = "fake-key-for-integration-0123456789"
    request = ExtractionRequest(start=date(2026, 11, 10), end=date(2026, 11, 10), page_size=100)
    with respx.mock(base_url="https://api.eia.gov", assert_all_called=False) as router:
        route = router.get("/v2/electricity/rto/region-data/data").mock(
            side_effect=_serve_synthetic
        )
        with EIAClient(Secret(key), sleep=lambda s: None) as client:
            ingest = pipeline.ingest(client, request, tmp_path, now=NOW)
    pipeline.transform(tmp_path)
    load = pipeline.load(tmp_path)

    assert ingest.rows == 24 * 4 * 4
    assert ingest.pages == 4
    assert route.call_count == 5  # 4 paginas + la comprobacion final del total
    assert load.fact_rows_after == 24 * 4 * 4
    # La API key viajo en la URL, pero NO debe estar en ningun archivo de datos.
    for parquet in tmp_path.rglob("*.parquet"):
        assert key.encode() not in parquet.read_bytes()
    assert key.encode() not in warehouse_path(tmp_path).read_bytes()


# ================================================================= SQL de ejemplo
def test_every_example_query_runs_and_returns_rows(tmp_path: Path) -> None:
    run_all(SyntheticSource(), WEEK, tmp_path)
    text = (REPO_ROOT / "sql" / "example_queries.sql").read_text(encoding="utf-8")
    code_lines = [line for line in text.splitlines() if not line.startswith("--")]
    statements = [s.strip() for s in "\n".join(code_lines).split(";") if s.strip()]
    assert len(statements) >= 5

    con = duckdb.connect(str(warehouse_path(tmp_path)), read_only=True)
    try:
        for statement in statements:
            assert con.execute(statement).fetchall(), f"sin resultados: {statement[:60]}"
    finally:
        con.close()


# ====================================================================== linea de comandos
def test_cli_run_synthetic_end_to_end(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)  # sin .env a la vista
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    out = tmp_path / "data"

    code = main(["run", "--synthetic", "--anomalies", "--output-dir", str(out)])

    assert code == 0
    printed = capsys.readouterr().out
    assert "[ingest]" in printed and "[transform]" in printed and "[load]" in printed
    assert fact_count(out) == ROWS_PER_WEEK - 2


def test_cli_stages_can_run_separately(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    out = str(tmp_path / "data")

    assert (
        main(
            [
                "ingest",
                "--synthetic",
                "--start",
                "2026-11-10",
                "--end",
                "2026-11-10",
                "--output-dir",
                out,
            ]
        )
        == 0
    )
    assert main(["transform", "--output-dir", out]) == 0
    assert main(["load", "--output-dir", out]) == 0
    assert fact_count(Path(out)) == 24 * 4 * 4


def test_cli_filters_types_and_respondents(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    out = tmp_path / "data"

    code = main(
        [
            "run",
            "--synthetic",
            "--start",
            "2026-11-10",
            "--end",
            "2026-11-10",
            "--types",
            "D",
            "TI",
            "--respondents",
            "CISO",
            "--output-dir",
            str(out),
        ]
    )

    assert code == 0
    assert fact_count(out) == 24 * 2 * 1


def test_cli_requires_choosing_a_source_so_nothing_downloads_by_accident(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit):
        main(["ingest", "--output-dir", str(tmp_path / "data")])

    assert not (tmp_path / "data").exists()


def test_cli_live_without_an_api_key_fails_with_a_clear_message(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)  # sin .env
    monkeypatch.delenv("EIA_API_KEY", raising=False)

    code = main(["ingest", "--live", "--output-dir", str(tmp_path / "data")])

    assert code == 2
    assert "EIA_API_KEY" in capsys.readouterr().err


def test_cli_invalid_arguments(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EIA_API_KEY", raising=False)

    reversed_dates = main(
        [
            "ingest",
            "--synthetic",
            "--start",
            "2026-11-10",
            "--end",
            "2026-11-04",
            "--output-dir",
            str(tmp_path / "d"),
        ]
    )
    with pytest.raises(SystemExit):
        main(["ingest", "--synthetic", "--start", "not-a-date"])

    assert reversed_dates == 2
    assert "Argumentos invalidos" in capsys.readouterr().err


def test_cli_transform_without_raw_data_fails_cleanly(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EIA_API_KEY", raising=False)

    code = main(["transform", "--output-dir", str(tmp_path / "data")])

    assert code == 1
    assert "ingest" in capsys.readouterr().err
