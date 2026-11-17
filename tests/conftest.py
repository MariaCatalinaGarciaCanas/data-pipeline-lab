"""Fixtures compartidas por todas las pruebas."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

# Variables que el propio pytest actualiza en cada fase del test (setup/call/teardown).
# No son un cambio de nuestro codigo, asi que se excluyen de la comparacion.
_PYTEST_MANAGED = frozenset({"PYTEST_CURRENT_TEST"})


def _environ_snapshot() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k not in _PYTEST_MANAGED}


@pytest.fixture(autouse=True)
def _environ_is_untouched() -> Iterator[None]:
    """Falla cualquier test que deje `os.environ` modificado.

    Por que existe: si un test (o el codigo bajo prueba) cambia el entorno del proceso, los
    tests siguientes ven un entorno distinto y su resultado pasa a depender del ORDEN de
    ejecucion. Al comprobarlo despues de CADA test, el culpable se detecta en el propio test,
    sin importar el orden.

    Orden de limpieza: pytest desmonta las fixtures en orden inverso al de montaje. Esta se
    monta primero (es automatica), asi que se desmonta la ultima, cuando `monkeypatch` ya ha
    restaurado sus cambios legitimos.

    Solo se muestran NOMBRES de variables, nunca valores (podrian ser secretos).
    """
    snapshot = _environ_snapshot()
    yield
    current = _environ_snapshot()
    if current == snapshot:
        return

    added = sorted(current.keys() - snapshot.keys())
    removed = sorted(snapshot.keys() - current.keys())
    changed = sorted(k for k in snapshot.keys() & current.keys() if snapshot[k] != current[k])

    # Restaura el entorno para que el fallo no se propague a los tests siguientes.
    for key in current.keys() - snapshot.keys():
        del os.environ[key]
    os.environ.update(snapshot)

    pytest.fail(
        "El test modifico os.environ "
        f"(anadidas: {added}, eliminadas: {removed}, cambiadas: {changed})",
        pytrace=False,
    )
