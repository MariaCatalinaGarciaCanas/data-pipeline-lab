"""Fixtures de las pruebas de integracion."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    """La CLI (`main`) configura el logging raiz y deja su handler instalado.

    Se restaura el estado anterior para que estas pruebas no contaminen a las demas (por ejemplo,
    a las que cuentan los handlers del logger raiz).
    """
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    saved = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    root.handlers, root.level = saved_handlers, saved_level
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)
