"""Excepciones del cliente de EIA.

Regla de diseno: TODO lo que se guarda en una excepcion (mensaje y atributos) debe estar ya
saneado por quien la crea: sin API key, sin URLs, sin parametros de consulta y sin cuerpos
de respuesta completos. Estas clases no reciben ni conservan objetos de `httpx`.

Las excepciones se lanzan siempre FUERA de un bloque `except` (ver `client.py`), de modo que
`__context__` y `__cause__` son `None` y no queda enlazada ninguna excepcion original de httpx.
"""

from __future__ import annotations

from typing import Any


def _restore(cls: type[EIAError], state: dict[str, Any]) -> EIAError:
    """Reconstruye una excepcion (pickle/copy) sin volver a llamar a `__init__`."""
    obj = cls.__new__(cls)
    Exception.__init__(obj, state["message"])
    obj.__dict__.update(state)
    return obj


class EIAError(Exception):
    """Base de los errores del cliente de EIA. El mensaje esta saneado."""

    def __init__(self, message: str, *, attempts: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.attempts = attempts

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{type(self).__name__}({fields})"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_restore, (type(self), dict(self.__dict__)))


class EIAClientClosedError(EIAError):
    """Se uso un cliente que ya estaba cerrado."""


class EIATransportError(EIAError):
    """Fallo de red o de tiempo de espera (tras agotar los reintentos si eran transitorios).

    `kind` es uno de: "timeout", "connect", "network", "protocol", "other".
    """

    def __init__(self, message: str, *, kind: str, attempts: int | None = None) -> None:
        super().__init__(message, attempts=attempts)
        self.kind = kind


class EIAHTTPError(EIAError):
    """EIA respondio con un estado HTTP de error.

    Attributes:
        status_code: codigo HTTP.
        eia_code: codigo de error de EIA (p. ej. "API_KEY_MISSING"), si se pudo extraer.
        retry_after: segundos pedidos por el servidor en `Retry-After`, si los hubo.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        eia_code: str | None = None,
        retry_after: float | None = None,
        attempts: int | None = None,
    ) -> None:
        super().__init__(message, attempts=attempts)
        self.status_code = status_code
        self.eia_code = eia_code
        self.retry_after = retry_after


class EIABadRequestError(EIAHTTPError):
    """HTTP 400 y otros 4xx permanentes (no se reintentan)."""


class EIAAuthError(EIAHTTPError):
    """HTTP 401 o 403: clave ausente, invalida o suspendida (no se reintenta)."""


class EIARateLimitError(EIAHTTPError):
    """HTTP 429: demasiadas peticiones, o el servidor pidio esperar mas de lo permitido."""


class EIAServerError(EIAHTTPError):
    """HTTP 5xx persistente tras agotar los reintentos."""


class EIAExtractionError(EIAError):
    """La extraccion paginada no pudo completarse de forma consistente.

    Casos: faltan o sobran filas respecto a `total`, el `total` cambia mientras se pagina o se
    supera el maximo de paginas esperado (proteccion contra bucles). No incluye datos de la API.
    """


class EIAResponseError(EIAError):
    """La respuesta no cumple el contrato esperado.

    Casos: HTTP 200 con JSON invalido o estructura incompleta, redireccion inesperada o un
    estado HTTP que no corresponde a ninguna otra categoria. No se reintenta.
    """

    def __init__(
        self, message: str, *, status_code: int | None = None, attempts: int | None = None
    ) -> None:
        super().__init__(message, attempts=attempts)
        self.status_code = status_code
