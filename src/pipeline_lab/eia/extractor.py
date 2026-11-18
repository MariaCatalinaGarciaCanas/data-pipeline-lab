"""Extractor: recorre TODAS las paginas de una ventana temporal de EIA-930.

Separacion de responsabilidades:
* `EIAClient` (client.py) hace UNA peticion validada y se ocupa de HTTP, reintentos y secretos.
* Este modulo decide QUE pedir (ventana, tipos, orden) y COMO recorrerlo (paginacion), y
  comprueba que lo recibido es coherente con lo anunciado por la API (`total`).

El extractor no conoce la API key ni la URL: depende solo de un `DataSource` (cualquier objeto
con un metodo `get_data` como el de `EIAClient`). Asi se prueba con datos sinteticos.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from pipeline_lab.eia.client import MAX_LENGTH, EIAPage
from pipeline_lab.eia.exceptions import EIAExtractionError

logger = logging.getLogger(__name__)

DEFAULT_TYPES = ("D", "DF", "NG", "TI")
# Orden determinista: sin un criterio total, la paginacion podria repetir o saltar filas.
SORT_ORDER = (("period", "asc"), ("respondent", "asc"), ("type", "asc"))

_NAME = re.compile(r"[A-Za-z0-9_\-]+")
Row = dict[str, str | None]


class DataSource(Protocol):
    """Lo unico que el extractor necesita de un cliente."""

    def get_data(
        self,
        *,
        frequency: str = ...,
        data: Sequence[str] = ...,
        facets: Mapping[str, Sequence[str]] | None = ...,
        start: str | None = ...,
        end: str | None = ...,
        sort: Sequence[tuple[str, str]] = ...,
        offset: int = ...,
        length: int = ...,
    ) -> EIAPage: ...


@dataclass(frozen=True, slots=True)
class ExtractionRequest:
    """Que extraer: ventana de dias (UTC, ambos inclusive), tipos y, opcionalmente, regiones."""

    start: date
    end: date
    types: tuple[str, ...] = DEFAULT_TYPES
    respondents: tuple[str, ...] = ()
    page_size: int = MAX_LENGTH
    max_consistency_retries: int = 2

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start no puede ser posterior a end")
        if not self.types:
            raise ValueError("types no puede estar vacio")
        for name in (*self.types, *self.respondents):
            if not _NAME.fullmatch(name):
                raise ValueError("types y respondents solo admiten letras, digitos, '_' y '-'")
        if isinstance(self.page_size, bool) or not 1 <= self.page_size <= MAX_LENGTH:
            raise ValueError(f"page_size debe estar entre 1 y {MAX_LENGTH}")
        if self.max_consistency_retries < 0:
            raise ValueError("max_consistency_retries no puede ser negativo")

    @property
    def start_period(self) -> str:
        return f"{self.start:%Y-%m-%d}T00"

    @property
    def end_period(self) -> str:
        return f"{self.end:%Y-%m-%d}T23"

    @property
    def facets(self) -> dict[str, tuple[str, ...]]:
        facets: dict[str, tuple[str, ...]] = {"type": self.types}
        if self.respondents:
            facets["respondent"] = self.respondents
        return facets


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    rows: tuple[Row, ...]
    total: int  # filas anunciadas por la API (response.total)
    pages: int
    duplicates_dropped: int  # filas repetidas (misma clave) descartadas


class _TotalChangedError(Exception):
    """El total cambio mientras se paginaba: los datos se estan actualizando. Se reintenta."""


def _key(row: Row) -> tuple[str | None, str | None, str | None]:
    return row.get("period"), row.get("respondent"), row.get("type")


def _fetch_page(
    source: DataSource, request: ExtractionRequest, offset: int, length: int
) -> EIAPage:
    return source.get_data(
        frequency="hourly",
        data=("value",),
        facets=request.facets,
        start=request.start_period,
        end=request.end_period,
        sort=SORT_ORDER,
        offset=offset,
        length=length,
    )


def _extract_once(source: DataSource, request: ExtractionRequest) -> ExtractionResult:
    unique: dict[tuple[str | None, str | None, str | None], Row] = {}
    duplicates = 0
    received = 0
    pages = 0
    offset = 0
    total: int | None = None
    max_pages = 1  # se recalcula al conocer el total

    while True:
        page = _fetch_page(source, request, offset, request.page_size)
        pages += 1
        if total is None:
            total = page.total
            # Un margen de 2 paginas absorbe respuestas algo cortas; mas alla se considera un bucle.
            max_pages = math.ceil(total / request.page_size) + 2
        elif page.total != total:
            raise _TotalChangedError

        if not page.rows:
            break  # pagina vacia: no hay mas datos (la coherencia se comprueba abajo)

        received += len(page.rows)
        for raw in page.rows:
            row: Row = {k: (None if v is None else str(v)) for k, v in raw.items()}
            key = _key(row)
            if key in unique:
                duplicates += 1
            else:
                unique[key] = row
        offset += len(page.rows)

        if offset >= total:
            break
        if pages >= max_pages:
            raise EIAExtractionError(
                f"Se supero el maximo de paginas esperado ({max_pages}) sin completar los "
                f"{total} registros anunciados; se aborta para evitar un bucle"
            )

    assert total is not None
    if received != total:
        raise EIAExtractionError(
            f"Extraccion incompleta: se recibieron {received} filas y la API anuncio {total}"
        )
    return ExtractionResult(
        rows=tuple(unique.values()), total=total, pages=pages, duplicates_dropped=duplicates
    )


def extract_region_data(source: DataSource, request: ExtractionRequest) -> ExtractionResult:
    """Extrae todas las filas de la ventana pedida, paginando.

    Garantias: sin filas repetidas, sin paginas infinitas, y `filas recibidas == total`. Si el
    `total` cambia durante la extraccion (la API esta actualizando datos) se repite entera, hasta
    `max_consistency_retries` veces.

    Raises:
        EIAExtractionError: respuesta incompleta, inconsistente o demasiadas paginas.
        EIAError (y subclases): errores del cliente HTTP, ya saneados.
    """
    attempts = request.max_consistency_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            result = _extract_once(source, request)
            # Comprobacion final barata: si el total cambio justo al terminar, los datos no son
            # una foto consistente.
            check = _fetch_page(source, request, offset=0, length=1)
            if check.total != result.total:
                raise _TotalChangedError
        except _TotalChangedError:
            logger.warning("eia_extract_total_changed attempt=%d/%d", attempt, attempts)
            continue
        logger.info(
            "eia_extract_done start=%s end=%s pages=%d rows=%d total=%d duplicates_dropped=%d",
            request.start,
            request.end,
            result.pages,
            len(result.rows),
            result.total,
            result.duplicates_dropped,
        )
        return result
    raise EIAExtractionError(
        f"El total de registros siguio cambiando durante {attempts} intentos de extraccion"
    )
