"""Configuracion de logging que nunca escribe secretos.

Por que un Formatter y no un Filter: el Formatter ve el texto FINAL (mensaje ya formateado
con sus argumentos y, ademas, el traceback de las excepciones). Asi enmascaramos tambien una
URL con `api_key=...` que aparezca dentro de un mensaje de error de una libreria.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterable
from typing import TextIO
from urllib.parse import quote

MASK = "***"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"

# Marca para reconocer nuestro handler y que configurar dos veces no lo duplique.
_HANDLER_ATTR = "_pipeline_lab_handler"

# Librerias que, a nivel INFO, registran la URL completa de cada peticion (con la api_key).
_NOISY_LOGGERS = ("httpx", "httpcore")


def _variants(secret: str) -> set[str]:
    """Formas en que un secreto puede aparecer en un texto: literal y codificado en URL."""
    return {secret, quote(secret, safe=""), quote(secret)}


class MaskingFormatter(logging.Formatter):
    """Formatter que sustituye cualquier secreto conocido por `***` en el texto final."""

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        super().__init__(LOG_FORMAT)
        # Hora en UTC para que los logs sean comparables entre maquinas.
        self.converter = time.gmtime
        # Un secreto vacio haria que replace("") corrompa todo el texto, por eso se descarta.
        variants: set[str] = set()
        for secret in secrets:
            if secret:
                variants |= _variants(secret)
        # Primero las variantes mas largas, por si una contiene a otra.
        self._variants = sorted(variants, key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        for variant in self._variants:
            text = text.replace(variant, MASK)
        return text


def configure_logging(
    level: str = "INFO",
    secrets: Iterable[str] = (),
    stream: TextIO | None = None,
) -> None:
    """Configura el logger raiz con enmascarado de secretos. Es seguro llamarla varias veces.

    Args:
        level: nivel minimo (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        secrets: valores reales que deben ocultarse (p. ej. `settings.api_key.reveal()`).
        stream: destino de los logs. Por defecto, stderr.
    """
    root = logging.getLogger()

    # Quita el handler anterior de esta funcion, si existe (idempotencia).
    for existing in list(root.handlers):
        if getattr(existing, _HANDLER_ATTR, False):
            root.removeHandler(existing)

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(MaskingFormatter(secrets))
    setattr(handler, _HANDLER_ATTR, True)

    root.addHandler(handler)
    root.setLevel(level.upper())

    # Defensa en profundidad: aunque el formatter enmascara, es mejor no generar el log.
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
