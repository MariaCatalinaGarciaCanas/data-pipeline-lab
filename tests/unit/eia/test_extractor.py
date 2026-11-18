"""Pruebas del extractor paginado. Usan la fuente sintetica o fuentes falsas: sin red."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from types import MappingProxyType
from typing import Any

import pytest

from pipeline_lab.eia import (
    EIAExtractionError,
    EIAPage,
    EIAServerError,
    ExtractionRequest,
    extract_region_data,
)
from pipeline_lab.synthetic import SyntheticSource

DAY = date(2026, 11, 10)


def make_page(rows: list[dict[str, Any]], total: int) -> EIAPage:
    return EIAPage(total=total, rows=tuple(MappingProxyType(r) for r in rows), frequency="hourly")


def row(i: int, **extra: Any) -> dict[str, Any]:
    base = {
        "period": f"2026-11-10T{i % 24:02d}",
        "respondent": f"R{i // 24}",
        "type": "D",
        "value": str(i),
    }
    return {**base, **extra}


class FakeSource:
    """Fuente falsa: `handler(kwargs)` decide la pagina; guarda cada llamada para inspeccionarla."""

    def __init__(self, handler: Callable[[dict[str, Any]], EIAPage]) -> None:
        self.handler = handler
        self.calls: list[dict[str, Any]] = []

    def get_data(self, **kwargs: Any) -> EIAPage:
        self.calls.append(kwargs)
        return self.handler(kwargs)


def serving(rows: list[dict[str, Any]], total: int | None = None) -> FakeSource:
    """Sirve `rows` paginando bien por offset/length."""
    announced = len(rows) if total is None else total
    return FakeSource(
        lambda kw: make_page(rows[kw["offset"] : kw["offset"] + kw["length"]], announced)
    )


# ============================================================================ la peticion
def test_request_builds_utc_period_bounds_and_facets() -> None:
    request = ExtractionRequest(start=date(2026, 11, 4), end=date(2026, 11, 10))

    assert request.start_period == "2026-11-04T00"
    assert request.end_period == "2026-11-10T23"
    assert request.facets == {"type": ("D", "DF", "NG", "TI")}


def test_request_can_restrict_respondents() -> None:
    request = ExtractionRequest(start=DAY, end=DAY, types=("D",), respondents=("CISO", "PJM"))

    assert request.facets == {"type": ("D",), "respondent": ("CISO", "PJM")}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start": date(2026, 11, 11), "end": date(2026, 11, 10)},
        {"types": ()},
        {"types": ("D", "bad type")},
        {"respondents": ("A/B",)},
        {"page_size": 0},
        {"page_size": 5001},
        {"max_consistency_retries": -1},
    ],
)
def test_invalid_requests_are_rejected(kwargs: dict[str, Any]) -> None:
    arguments: dict[str, Any] = {"start": DAY, "end": DAY, **kwargs}

    with pytest.raises(ValueError):
        ExtractionRequest(**arguments)


# ======================================================================== paginacion
def test_retrieves_every_page_of_a_synthetic_day() -> None:
    source = SyntheticSource()
    request = ExtractionRequest(start=DAY, end=DAY, page_size=100)

    result = extract_region_data(source, request)

    expected = 24 * 4 * 4  # horas x regiones x tipos
    assert result.total == expected
    assert len(result.rows) == expected
    assert result.pages == 4  # ceil(384 / 100)
    assert result.duplicates_dropped == 0
    assert len({(r["period"], r["respondent"], r["type"]) for r in result.rows}) == expected


def test_one_page_when_page_size_exceeds_the_total() -> None:
    result = extract_region_data(SyntheticSource(), ExtractionRequest(start=DAY, end=DAY))

    assert result.pages == 1
    assert len(result.rows) == result.total


def test_requests_offsets_advance_by_the_rows_received() -> None:
    source = serving([row(i) for i in range(250)])

    result = extract_region_data(source, ExtractionRequest(start=DAY, end=DAY, page_size=100))

    page_offsets = [call["offset"] for call in source.calls[:-1]]  # la ultima llamada es el control
    assert page_offsets == [0, 100, 200]
    assert result.pages == 3
    assert len(result.rows) == 250


def test_sends_window_types_sort_and_length_to_the_source() -> None:
    source = serving([row(0)])
    request = ExtractionRequest(
        start=date(2026, 11, 4), end=date(2026, 11, 10), types=("D", "TI"), page_size=50
    )

    extract_region_data(source, request)

    first = source.calls[0]
    assert first["frequency"] == "hourly"
    assert first["start"] == "2026-11-04T00"
    assert first["end"] == "2026-11-10T23"
    assert first["facets"] == {"type": ("D", "TI")}
    assert first["length"] == 50
    assert list(first["sort"]) == [("period", "asc"), ("respondent", "asc"), ("type", "asc")]


def test_filters_by_type_and_respondent() -> None:
    request = ExtractionRequest(start=DAY, end=DAY, types=("D",), respondents=("CISO",))

    result = extract_region_data(SyntheticSource(), request)

    assert len(result.rows) == 24
    assert {r["type"] for r in result.rows} == {"D"}
    assert {r["respondent"] for r in result.rows} == {"CISO"}


def test_values_are_kept_as_text_and_missing_values_stay_none() -> None:
    source = serving([{"period": "2026-11-10T00", "respondent": "A", "type": "D", "value": None}])

    result = extract_region_data(source, ExtractionRequest(start=DAY, end=DAY))

    assert result.rows[0]["value"] is None


def test_numeric_values_are_converted_to_text() -> None:
    source = serving([{"period": "2026-11-10T00", "respondent": "A", "type": "D", "value": 5}])

    result = extract_region_data(source, ExtractionRequest(start=DAY, end=DAY))

    assert result.rows[0]["value"] == "5"


# ============================================================ duplicados e incoherencias
def test_identical_keys_are_dropped_and_counted() -> None:
    source = SyntheticSource(anomalies=True)  # incluye un duplicado exacto

    result = extract_region_data(source, ExtractionRequest(start=DAY, end=DAY))

    assert result.duplicates_dropped == 1
    assert len(result.rows) == result.total - 1


def test_incomplete_response_is_an_error() -> None:
    # La API anuncia 10 filas pero la segunda pagina llega vacia.
    rows = [row(i) for i in range(5)]
    source = FakeSource(
        lambda kw: make_page(rows[kw["offset"] : kw["offset"] + kw["length"]], total=10)
    )

    with pytest.raises(EIAExtractionError, match="incompleta"):
        extract_region_data(source, ExtractionRequest(start=DAY, end=DAY, page_size=5))


def test_a_source_that_never_finishes_cannot_loop_forever() -> None:
    # Cada pagina devuelve 1 fila distinta aunque se pidan 500: nunca alcanza el total.
    counter = iter(range(10_000))
    source = FakeSource(lambda kw: make_page([row(next(counter))], total=1000))

    with pytest.raises(EIAExtractionError, match="maximo de paginas"):
        extract_region_data(source, ExtractionRequest(start=DAY, end=DAY, page_size=500))

    assert len(source.calls) <= 5  # acotado por ceil(1000/500) + 2


def test_total_changing_between_pages_is_retried_then_succeeds() -> None:
    rows = [row(i) for i in range(6)]
    seen = {"calls": 0}

    def handler(kw: dict[str, Any]) -> EIAPage:
        seen["calls"] += 1
        total = 7 if seen["calls"] == 2 else 6  # el total cambia solo en la 2.a llamada
        return make_page(rows[kw["offset"] : kw["offset"] + kw["length"]], total)

    result = extract_region_data(
        FakeSource(handler), ExtractionRequest(start=DAY, end=DAY, page_size=3)
    )

    assert len(result.rows) == 6


def test_total_that_keeps_changing_gives_up() -> None:
    state = {"total": 10}

    def handler(kw: dict[str, Any]) -> EIAPage:
        state["total"] += 1  # cada llamada anuncia un total distinto
        return make_page([row(i) for i in range(3)], state["total"])

    request = ExtractionRequest(start=DAY, end=DAY, page_size=3, max_consistency_retries=1)

    with pytest.raises(EIAExtractionError, match="siguio cambiando"):
        extract_region_data(FakeSource(handler), request)


def test_total_changing_right_at_the_end_is_detected_by_the_final_check() -> None:
    rows = [row(i) for i in range(4)]
    calls = {"n": 0}

    def handler(kw: dict[str, Any]) -> EIAPage:
        calls["n"] += 1
        # intento 1: 1 pagina + control con total distinto; intento 2: todo estable
        if calls["n"] == 2:
            return make_page(rows[:1], total=5)  # control final del primer intento
        return make_page(rows[kw["offset"] : kw["offset"] + kw["length"]], total=4)

    result = extract_region_data(FakeSource(handler), ExtractionRequest(start=DAY, end=DAY))

    assert len(result.rows) == 4
    assert calls["n"] == 4  # (pagina + control) x 2 intentos


def test_client_errors_propagate_unchanged() -> None:
    def handler(kw: dict[str, Any]) -> EIAPage:
        raise EIAServerError("EIA respondio con HTTP 503", status_code=503, attempts=3)

    with pytest.raises(EIAServerError) as info:
        extract_region_data(FakeSource(handler), ExtractionRequest(start=DAY, end=DAY))

    assert info.value.status_code == 503
