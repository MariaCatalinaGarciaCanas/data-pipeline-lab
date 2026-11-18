"""Cliente HTTP sincrono para la API v2 de EIA (endpoint `electricity/rto/region-data/data`).

Alcance: UNA peticion validada por llamada (`get_data`). La paginacion automatica, la escritura
a Parquet y las transformaciones son de PRs posteriores.

Seguridad de la API key (resumen; detalle y LIMITES en docs/adr/0001-eia-api-key-handling.md)
-----------------------------------------------------------------------------------------
EIA solo acepta la clave como parametro `api_key` de la URL, asi que la URL de cada peticion la
contiene y `httpx` la incluye en varios sitios (excepciones, `request.url`, logs, hooks).
Para que no llegue a una excepcion propia ni a un traceback se separan dos responsabilidades:

* `_transport()` es el UNICO sitio donde existen objetos de httpx. Captura cualquier fallo de la
  llamada y devuelve DATOS ya saneados (`_Reply` / `_Failure`). No lanza nada.
* `_execute()` decide reintentos y lanza las excepciones propias. No contiene ningun `except`,
  asi que se lanzan FUERA de un bloque de captura: `__context__` y `__cause__` son `None` y
  ningun objeto de httpx esta enlazado a la excepcion ni a sus frames.

Esto NO protege frente a lo que ocurre durante la llamada HTTP (hooks o instrumentacion de
httpx, proxies, servidor, memoria del proceso). Ver el ADR.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

import httpx

from pipeline_lab.config import Secret, Settings
from pipeline_lab.eia._sanitize import sanitize_untrusted_text
from pipeline_lab.eia.exceptions import (
    EIAAuthError,
    EIABadRequestError,
    EIAClientClosedError,
    EIAError,
    EIARateLimitError,
    EIAResponseError,
    EIAServerError,
    EIATransportError,
)
from pipeline_lab.logging_config import silence_http_loggers

logger = logging.getLogger(__name__)

BASE_URL = "https://api.eia.gov"
DATA_ROUTE = "/v2/electricity/rto/region-data/data"
MAX_LENGTH = 5000  # filas maximas por respuesta JSON de EIA
USER_AGENT = "data-pipeline-lab/0.1 (EIA ingestion client)"

_NAME = re.compile(r"[A-Za-z0-9_\-]+")  # ids de columnas y facets
_DELTA_SECONDS = re.compile(r"\d{1,9}(?:\.\d+)?")
_MAX_ERROR_CODE_LENGTH = 64
_ERROR_CODE = re.compile(rf"[A-Za-z0-9_.\-]{{1,{_MAX_ERROR_CODE_LENGTH}}}")
_RETRYABLE_KINDS = frozenset({"timeout", "connect", "network", "protocol"})
_ERROR_BODY_LIMIT = 4096  # bytes de un cuerpo de error que se llegan a examinar

_TRANSPORT_MESSAGES = {
    "timeout": "Tiempo de espera agotado al consultar EIA",
    "connect": "No se pudo conectar con EIA",
    "network": "Error de red al consultar EIA",
    "protocol": "EIA cerro la conexion de forma inesperada",
    "other": "Error inesperado al consultar EIA",
}


# ------------------------------------------------------------------------------ configuracion
@dataclass(frozen=True, slots=True)
class Timeouts:
    """Tiempos de espera independientes, en segundos."""

    connect: float = 5.0
    read: float = 30.0
    write: float = 10.0
    pool: float = 5.0

    def __post_init__(self) -> None:
        for name in ("connect", "read", "write", "pool"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
                raise ValueError(f"Timeouts.{name} debe ser un numero mayor que 0")

    def to_httpx(self) -> httpx.Timeout:
        return httpx.Timeout(connect=self.connect, read=self.read, write=self.write, pool=self.pool)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Politica de reintentos con backoff exponencial y jitter completo.

    El retardo antes del reintento n es `uniform(0, min(max_backoff, initial * multiplier**(n-1)))`.
    Si el servidor envia `Retry-After`, ese valor sustituye al backoff; si supera
    `max_retry_after` NO se reintenta (reintentar antes de lo pedido arriesga que EIA suspenda
    la clave).
    """

    max_attempts: int = 3
    initial_backoff: float = 1.0
    max_backoff: float = 8.0
    multiplier: float = 2.0
    max_retry_after: float = 30.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise ValueError("RetryPolicy.max_attempts debe ser un entero")
        if self.max_attempts < 1:
            raise ValueError("RetryPolicy.max_attempts debe ser al menos 1")
        if self.initial_backoff <= 0:
            raise ValueError("RetryPolicy.initial_backoff debe ser mayor que 0")
        if self.max_backoff < self.initial_backoff:
            raise ValueError("RetryPolicy.max_backoff no puede ser menor que initial_backoff")
        if self.multiplier < 1:
            raise ValueError("RetryPolicy.multiplier debe ser al menos 1")
        if self.max_retry_after < 0:
            raise ValueError("RetryPolicy.max_retry_after no puede ser negativo")

    def backoff_cap(self, attempt: int) -> float:
        """Techo del retardo tras el intento `attempt` (empieza en 1)."""
        return min(self.max_backoff, self.initial_backoff * self.multiplier ** (attempt - 1))


# ----------------------------------------------------------------------------------- resultado
@dataclass(frozen=True, slots=True)
class EIAPage:
    """Una pagina de datos ya validada. Inmutable.

    No conserva el objeto `request` de la respuesta de EIA (podria repetir parametros).
    `rows` son vistas de solo lectura de las filas tal como llegan (valores como texto).
    """

    total: int
    rows: tuple[Mapping[str, Any], ...]
    frequency: str | None = None
    warnings: tuple[str, ...] = ()


# -------------------------------------------------------- datos internos (sin objetos de httpx)
@dataclass(frozen=True, slots=True)
class _Reply:
    """Respuesta HTTP ya interpretada y saneada."""

    status: int
    retry_after: str | None = None
    message: str | None = None  # mensaje de error de EIA, saneado y truncado
    eia_code: str | None = None
    page: EIAPage | None = None  # solo si status == 200 y la estructura es valida
    problem: str | None = None  # solo si status == 200 y la estructura NO es valida


@dataclass(frozen=True, slots=True)
class _Failure:
    """La llamada HTTP no llego a producir respuesta."""

    kind: str


# ---------------------------------------------------------------------- funciones puras (sin I/O)
def _parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Segundos pedidos por `Retry-After` (formato en segundos o fecha HTTP), o None."""
    if value is None:
        return None
    text = value.strip()
    if _DELTA_SECONDS.fullmatch(text):
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())


def _parse_total(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and re.fullmatch(r"\d{1,15}", value.strip()):
        return int(value.strip())
    return None


def _parse_page(body: bytes, secrets: tuple[str, ...]) -> tuple[EIAPage | None, str | None]:
    """Valida la estructura basica de una respuesta 200. Devuelve (pagina, problema)."""
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError):
        return None, "La respuesta de EIA no es JSON valido"
    if not isinstance(payload, dict):
        return None, "La respuesta de EIA no es un objeto JSON"
    response = payload.get("response")
    if not isinstance(response, dict):
        return None, "Falta el objeto 'response' en la respuesta de EIA"
    data = response.get("data")
    if not isinstance(data, list):
        return None, "Falta la lista 'response.data' en la respuesta de EIA"
    if not all(isinstance(row, dict) for row in data):
        return None, "'response.data' contiene filas que no son objetos"
    total = _parse_total(response.get("total"))
    if total is None:
        return None, "'response.total' falta o no es un entero valido"

    frequency = response.get("frequency")
    warnings: list[str] = []
    raw_warnings = payload.get("warnings")
    if isinstance(raw_warnings, list):
        for item in raw_warnings:
            if not isinstance(item, dict):
                continue
            parts = [item.get("warning"), item.get("description")]
            text = " - ".join(p for p in parts if isinstance(p, str) and p.strip())
            if text:
                warnings.append(sanitize_untrusted_text(text, secrets))

    page = EIAPage(
        total=total,
        rows=tuple(MappingProxyType(row) for row in data),
        frequency=frequency if isinstance(frequency, str) else None,
        warnings=tuple(warnings),
    )
    return page, None


def _clean_error_code(raw: object, secrets: tuple[str, ...]) -> str | None:
    if isinstance(raw, bool) or not isinstance(raw, str | int):
        return None
    text = str(raw)
    if len(text) > _MAX_ERROR_CODE_LENGTH:
        return None  # un codigo no legitimo: se descarta, no se recorta
    cleaned = sanitize_untrusted_text(text, secrets, max_length=_MAX_ERROR_CODE_LENGTH)
    return cleaned if _ERROR_CODE.fullmatch(cleaned) else None


def _extract_error(body: bytes, secrets: tuple[str, ...]) -> tuple[str | None, str | None]:
    """Extrae (mensaje, codigo) de un cuerpo de error de EIA, ya saneados.

    Admite los dos formatos observados:
      403: {"error": {"code": "API_KEY_MISSING", "message": "..."}}
      400: {"error": "texto", "code": 400}
    Cualquier otra forma (HTML, texto, vacio) da (None, None).
    """
    text = body[:_ERROR_BODY_LIMIT].decode("utf-8", errors="replace")
    try:
        payload = json.loads(text)
    except (ValueError, RecursionError):
        return None, None
    if not isinstance(payload, dict):
        return None, None
    error = payload.get("error")
    raw_message: object = None
    raw_code: object = None
    if isinstance(error, dict):
        raw_code, raw_message = error.get("code"), error.get("message")
    elif isinstance(error, str):
        raw_message, raw_code = error, payload.get("code")
    message = None
    if isinstance(raw_message, str) and raw_message.strip():
        message = sanitize_untrusted_text(raw_message, secrets)
    return message, _clean_error_code(raw_code, secrets)


def _interpret(
    status: int, retry_after: str | None, body: bytes, secrets: tuple[str, ...]
) -> _Reply:
    """Convierte (estado, cabecera, cuerpo) en un `_Reply` saneado. No lanza excepciones."""
    if status == 200:
        page, problem = _parse_page(body, secrets)
        return _Reply(status=status, page=page, problem=problem)
    message, code = _extract_error(body, secrets)
    return _Reply(status=status, retry_after=retry_after, message=message, eia_code=code)


def _failure_kind(exc: BaseException) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        return "connect"
    if isinstance(exc, httpx.NetworkError):
        return "network"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol"
    return "other"


def _is_retryable(result: _Reply | _Failure) -> bool:
    if isinstance(result, _Failure):
        return result.kind in _RETRYABLE_KINDS
    return result.status == 429 or 500 <= result.status <= 599


def _reason(result: _Reply | _Failure) -> str:
    """Etiqueta corta y segura para los logs."""
    if isinstance(result, _Failure):
        return result.kind
    return "invalid_response" if result.status == 200 else f"http_{result.status}"


def _build_error(
    result: _Reply | _Failure, attempt: int, retry_after: float | None, policy: RetryPolicy
) -> EIAError:
    """Construye la excepcion propia (saneada). NO la lanza."""
    suffix = f" (tras {attempt} intentos)" if attempt > 1 else ""

    if isinstance(result, _Failure):
        text = _TRANSPORT_MESSAGES.get(result.kind, _TRANSPORT_MESSAGES["other"])
        return EIATransportError(text + suffix, kind=result.kind, attempts=attempt)

    status = result.status
    if status == 200:
        return EIAResponseError(
            result.problem or "Respuesta invalida de EIA", status_code=200, attempts=attempt
        )
    if 300 <= status < 400:
        return EIAResponseError(
            f"Redireccion inesperada (HTTP {status}); el cliente no sigue redirecciones",
            status_code=status,
            attempts=attempt,
        )
    if not 400 <= status < 600:
        return EIAResponseError(
            f"Estado HTTP inesperado ({status})", status_code=status, attempts=attempt
        )

    text = f"EIA respondio con HTTP {status}"
    if result.eia_code:
        text += f" [{result.eia_code}]"
    if result.message:
        text += f": {result.message}"
    if retry_after is not None and retry_after > policy.max_retry_after:
        text += (
            f". El servidor pidio esperar {retry_after:g} s (maximo permitido: "
            f"{policy.max_retry_after:g} s); no se reintenta"
        )
    text += suffix

    if status == 429:
        cls: type[Any] = EIARateLimitError
    elif status in (401, 403):
        cls = EIAAuthError
    elif status >= 500:
        cls = EIAServerError
    else:
        cls = EIABadRequestError
    return cls(
        text,
        status_code=status,
        eia_code=result.eia_code,
        retry_after=retry_after,
        attempts=attempt,
    )


def _non_empty_names(values: object, label: str) -> list[str]:
    if isinstance(values, str) or not isinstance(values, Sequence) or not values:
        raise ValueError(f"{label} debe ser una secuencia no vacia de nombres")
    for value in values:
        if not isinstance(value, str) or not _NAME.fullmatch(value):
            raise ValueError(f"{label} contiene un nombre invalido")
    return list(values)


def _build_query(
    *,
    frequency: str,
    data: Sequence[str],
    facets: Mapping[str, Sequence[str]] | None,
    start: str | None,
    end: str | None,
    sort: Sequence[tuple[str, str]],
    offset: int,
    length: int,
) -> list[tuple[str, str]]:
    """Valida los argumentos y arma los parametros de consulta (SIN la api_key)."""
    if not isinstance(frequency, str) or not frequency.strip():
        raise ValueError("frequency debe ser un texto no vacio")
    for label, value in (("start", start), ("end", end)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{label} debe ser un texto no vacio")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("offset debe ser un entero mayor o igual que 0")
    if isinstance(length, bool) or not isinstance(length, int) or not 1 <= length <= MAX_LENGTH:
        raise ValueError(f"length debe ser un entero entre 1 y {MAX_LENGTH}")

    query: list[tuple[str, str]] = [("frequency", frequency)]
    for i, column in enumerate(_non_empty_names(data, "data")):
        query.append((f"data[{i}]", column))
    for facet, values in (facets or {}).items():
        if not isinstance(facet, str) or not _NAME.fullmatch(facet):
            raise ValueError("facets contiene un nombre de facet invalido")
        if isinstance(values, str) or not isinstance(values, Sequence) or not values:
            raise ValueError("cada facet debe tener una secuencia no vacia de valores")
        for value in values:
            if not isinstance(value, str) or not value:
                raise ValueError("los valores de un facet deben ser textos no vacios")
            query.append((f"facets[{facet}][]", value))
    if start is not None:
        query.append(("start", start))
    if end is not None:
        query.append(("end", end))
    for i, item in enumerate(sort):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("sort debe ser una secuencia de tuplas (columna, direccion)")
        column, direction = item
        if not isinstance(column, str) or not _NAME.fullmatch(column):
            raise ValueError("sort contiene un nombre de columna invalido")
        if direction not in ("asc", "desc"):
            raise ValueError("la direccion de sort debe ser 'asc' o 'desc'")
        query.append((f"sort[{i}][column]", column))
        query.append((f"sort[{i}][direction]", direction))
    query.append(("offset", str(offset)))
    query.append(("length", str(length)))
    return query


def _utcnow() -> datetime:
    return datetime.now(UTC)


# ------------------------------------------------------------------------------------- cliente
class EIAClient:
    """Cliente sincrono de EIA. Usar como gestor de contexto para liberar la conexion.

    Args:
        api_key: la clave, envuelta en `Secret`.
        base_url: debe usar https (la clave viaja en la URL).
        timeouts: tiempos de espera.
        retry: politica de reintentos.
        sleep, rng, clock: inyectables para que las pruebas no esperen ni dependan del azar.
    """

    def __init__(
        self,
        api_key: Secret,
        *,
        base_url: str = BASE_URL,
        timeouts: Timeouts | None = None,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        if not isinstance(api_key, Secret):
            raise TypeError("api_key debe ser un Secret")
        if urlsplit(base_url).scheme != "https":
            raise ValueError("base_url debe usar https: la API key viaja en la URL")

        # No depender de que el programa principal haya llamado a configure_logging().
        silence_http_loggers()

        self._api_key = api_key
        self._base_url = base_url
        self._timeouts = timeouts or Timeouts()
        self._retry = retry or RetryPolicy()
        self._sleep = sleep
        self._rng = rng
        self._clock = clock
        self._closed = False
        # follow_redirects=False: una redireccion a otro host llevaria la clave consigo.
        self._http = httpx.Client(
            base_url=base_url,
            timeout=self._timeouts.to_httpx(),
            follow_redirects=False,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs: Any) -> EIAClient:
        return cls(settings.api_key, **kwargs)

    # -- ciclo de vida
    def close(self) -> None:
        """Cierra las conexiones. Es idempotente."""
        self._closed = True
        self._http.close()

    def __enter__(self) -> EIAClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"EIAClient(base_url={self._base_url!r}, timeouts={self._timeouts!r}, "
            f"retry={self._retry!r}, closed={self._closed})"
        )

    # -- API publica
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
        """Pide UNA pagina de datos y devuelve la respuesta validada.

        Raises:
            ValueError: argumentos invalidos (error de programacion, no de red).
            EIAClientClosedError, EIATransportError, EIAAuthError, EIABadRequestError,
            EIARateLimitError, EIAServerError, EIAResponseError: ver `exceptions.py`.
        """
        query = _build_query(
            frequency=frequency,
            data=data,
            facets=facets,
            start=start,
            end=end,
            sort=sort,
            offset=offset,
            length=length,
        )
        if self._closed:
            raise EIAClientClosedError("El cliente de EIA esta cerrado")
        return self._execute(query)

    # -- internos
    def _transport(self, query: Sequence[tuple[str, str]]) -> _Reply | _Failure:
        """Hace UNA peticion. Unico sitio con objetos de httpx. Devuelve datos, no lanza.

        La clave se revela solo en el argumento de la llamada, sin guardarla en una variable
        con nombre. Los objetos de httpx se eliminan antes de interpretar la respuesta.
        """
        try:
            response = self._http.get(
                DATA_ROUTE, params=[*query, ("api_key", self._api_key.reveal())]
            )
            status = response.status_code
            retry_after = response.headers.get("retry-after")
            body = response.content
        except Exception as exc:  # noqa: BLE001 - cualquier fallo de httpx puede contener la URL
            return _Failure(_failure_kind(exc))
        del response
        return _interpret(status, retry_after, body, (self._api_key.reveal(),))

    def _execute(self, query: Sequence[tuple[str, str]]) -> EIAPage:
        """Bucle de reintentos. SIN `except`: las excepciones se lanzan fuera de todo handler."""
        policy = self._retry
        for attempt in range(1, policy.max_attempts + 1):
            started = time.monotonic()
            result = self._transport(query)
            elapsed_ms = int((time.monotonic() - started) * 1000)

            if isinstance(result, _Reply) and result.page is not None:
                logger.debug(
                    "eia_request_ok attempt=%d/%d status=%d rows=%d elapsed_ms=%d",
                    attempt,
                    policy.max_attempts,
                    result.status,
                    len(result.page.rows),
                    elapsed_ms,
                )
                return result.page

            retry_after = None
            if isinstance(result, _Reply):
                retry_after = _parse_retry_after(result.retry_after, self._clock())
            error = _build_error(result, attempt, retry_after, policy)
            # En el ultimo intento no se calcula ningun retardo (ni se consume el azar).
            last_attempt = attempt >= policy.max_attempts
            delay = None if last_attempt else self._retry_delay(result, attempt, retry_after)

            if delay is None:
                logger.warning(
                    "eia_request_failed attempt=%d/%d reason=%s",
                    attempt,
                    policy.max_attempts,
                    _reason(result),
                )
                raise error

            logger.warning(
                "eia_request_retry attempt=%d/%d reason=%s delay_s=%.2f",
                attempt,
                policy.max_attempts,
                _reason(result),
                delay,
            )
            self._sleep(delay)
        raise AssertionError(
            "max_attempts >= 1 garantiza al menos una iteracion"
        )  # pragma: no cover

    def _retry_delay(
        self, result: _Reply | _Failure, attempt: int, retry_after: float | None
    ) -> float | None:
        """Segundos a esperar antes de reintentar, o None si no hay que reintentar."""
        if not _is_retryable(result):
            return None
        if retry_after is not None:
            # Respetar al servidor; si pide mas de lo permitido, NO reintentar.
            return retry_after if retry_after <= self._retry.max_retry_after else None
        return self._rng() * self._retry.backoff_cap(attempt)
