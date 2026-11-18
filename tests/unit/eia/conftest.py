"""Fixtures de las pruebas del cliente de EIA. Nunca se hacen llamadas reales a la red.

`respx` intercepta todas las peticiones de httpx: si una peticion no coincide con ninguna ruta
simulada, falla en lugar de salir a Internet.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator

import pytest
import respx

from pipeline_lab.config import Secret
from pipeline_lab.eia import EIAClient

# Clave FICTICIA, solo para pruebas. Incluye caracteres que se codifican en una URL.
FAKE_KEY = "fake-key/with+chars=DO-NOT-USE-0123456789"
BASE = "https://api.eia.gov"
DATA_PATH = "/v2/electricity/rto/region-data/data"


@pytest.fixture(autouse=True)
def _restore_http_loggers() -> Iterator[None]:
    """El constructor del cliente sube `httpx`/`httpcore` a WARNING (estado global): lo restaura."""
    names = ("httpx", "httpcore")
    saved = {name: logging.getLogger(name).level for name in names}
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


@pytest.fixture
def fake_key() -> str:
    return FAKE_KEY


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=BASE, assert_all_called=False) as mock_router:
        yield mock_router


@pytest.fixture
def route(router: respx.MockRouter) -> respx.Route:
    return router.get(DATA_PATH)


@pytest.fixture
def sleeps() -> list[float]:
    """Registra los retardos pedidos en lugar de esperar de verdad."""
    return []


@pytest.fixture
def make_client(sleeps: list[float]) -> Iterator[Callable[..., EIAClient]]:
    """Crea clientes sin esperas reales y con jitter fijo (0.5). Los cierra al terminar."""
    created: list[EIAClient] = []

    def factory(**kwargs: object) -> EIAClient:
        kwargs.setdefault("sleep", sleeps.append)
        kwargs.setdefault("rng", lambda: 0.5)
        client = EIAClient(Secret(FAKE_KEY), **kwargs)  # type: ignore[arg-type]
        created.append(client)
        return client

    yield factory
    for client in created:
        client.close()
