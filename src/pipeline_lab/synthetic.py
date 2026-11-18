"""Fuente de datos SINTETICA con la misma forma que EIA-930 (`electricity/rto/region-data`).

Sirve para ejecutar y probar el pipeline de principio a fin sin red y sin API key. Implementa el
mismo metodo `get_data` que `EIAClient` (incluida la paginacion con `offset`/`length`), asi que el
extractor no distingue una fuente de otra.

Los datos son inventados pero plausibles: demanda con ciclo diario, pronostico cercano a la
demanda, generacion y un intercambio que es la diferencia (negativo cuando la region exporta;
un valor negativo de interchange es VALIDO). Con `anomalies=True` se cuelan, a proposito, filas
problematicas para ejercitar la limpieza: un valor nulo, un valor no numerico y un duplicado.
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from types import MappingProxyType
from typing import Any

from pipeline_lab.eia.client import MAX_LENGTH, EIAPage

RESPONDENTS: dict[str, str] = {
    "CISO": "California Independent System Operator",
    "ERCO": "Electric Reliability Council of Texas, Inc.",
    "NYIS": "New York Independent System Operator",
    "PJM": "PJM Interconnection, LLC",
}
TYPE_NAMES: dict[str, str] = {
    "D": "Demand",
    "DF": "Day-ahead demand forecast",
    "NG": "Net generation",
    "TI": "Total interchange",
}
_BASE_MWH = {"CISO": 26_000, "ERCO": 48_000, "NYIS": 18_000, "PJM": 95_000}

Row = dict[str, str | None]


def _hours(start_period: str, end_period: str) -> list[datetime]:
    fmt = "%Y-%m-%dT%H"
    current, last = datetime.strptime(start_period, fmt), datetime.strptime(end_period, fmt)
    hours = []
    while current <= last:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


class SyntheticSource:
    """Genera y sirve datos sinteticos con paginacion. Determinista para una semilla dada."""

    def __init__(self, *, seed: int = 2026, anomalies: bool = False) -> None:
        self._seed = seed
        self._anomalies = anomalies
        self._cache: dict[tuple[Any, ...], list[Row]] = {}
        self.calls = 0  # numero de peticiones servidas (util en las pruebas)

    def _build(
        self, start: str, end: str, types: Sequence[str], respondents: Sequence[str]
    ) -> list[Row]:
        rng = random.Random(f"{self._seed}|{start}|{end}")
        rows: list[Row] = []
        for hour in _hours(start, end):
            for respondent in sorted(respondents):
                base = _BASE_MWH[respondent]
                daily = 1 + 0.25 * math.sin(2 * math.pi * (hour.hour - 14) / 24)
                demand = base * daily * (1 + rng.uniform(-0.02, 0.02))
                generation = demand * rng.uniform(0.9, 1.1)
                values = {
                    "D": demand,
                    "DF": demand * (1 + rng.uniform(-0.04, 0.04)),
                    "NG": generation,
                    "TI": demand - generation,  # negativo si la region genera mas de lo que consume
                }
                for type_code in sorted(types):
                    rows.append(
                        {
                            "period": f"{hour:%Y-%m-%dT%H}",
                            "respondent": respondent,
                            "respondent-name": RESPONDENTS[respondent],
                            "type": type_code,
                            "type-name": TYPE_NAMES[type_code],
                            "value": str(round(values[type_code])),
                            "value-units": "megawatthours",
                        }
                    )
        if self._anomalies and len(rows) > 12:
            rows[0]["value"] = None  # valor ausente
            rows[7]["value"] = "n/a"  # valor no numerico
            rows.append(dict(rows[11]))  # duplicado exacto (misma clave)
        return rows

    def get_data(
        self,
        *,
        frequency: str = "hourly",
        data: Sequence[str] = ("value",),
        facets: Mapping[str, Sequence[str]] | None = None,
        start: str | None = None,
        end: str | None = None,
        sort: Sequence[tuple[str, str]] = (),
        offset: int = 0,
        length: int = MAX_LENGTH,
    ) -> EIAPage:
        if frequency != "hourly" or start is None or end is None:
            raise ValueError("SyntheticSource solo admite frequency='hourly' con start y end")
        if offset < 0 or not 1 <= length <= MAX_LENGTH:
            raise ValueError("offset o length fuera de rango")
        facets = facets or {}
        types = tuple(facets.get("type", TYPE_NAMES))
        respondents = tuple(facets.get("respondent", RESPONDENTS))
        unknown = (set(types) - set(TYPE_NAMES)) | (set(respondents) - set(RESPONDENTS))
        key = (start, end, tuple(sorted(types)), tuple(sorted(respondents)))
        if unknown:
            rows: list[Row] = []  # como EIA: un facet inexistente no da error, da 0 filas
        else:
            if key not in self._cache:
                self._cache[key] = self._build(start, end, types, respondents)
            rows = self._cache[key]
        self.calls += 1
        window = rows[offset : offset + length]
        return EIAPage(
            total=len(rows),
            rows=tuple(MappingProxyType(dict(row)) for row in window),
            frequency="hourly",
            warnings=(),
        )


def window_for(start: date, end: date) -> tuple[str, str]:
    """(start_period, end_period) en el formato de EIA para una ventana de dias completos."""
    return f"{start:%Y-%m-%d}T00", f"{end:%Y-%m-%d}T23"
