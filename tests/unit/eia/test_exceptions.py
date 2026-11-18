"""Pruebas de la jerarquia de excepciones del cliente de EIA."""

from __future__ import annotations

import copy
import pickle

import pytest

from pipeline_lab.eia import (
    EIAAuthError,
    EIABadRequestError,
    EIAClientClosedError,
    EIAError,
    EIAHTTPError,
    EIARateLimitError,
    EIAResponseError,
    EIAServerError,
    EIATransportError,
)

ALL_EXCEPTIONS = [
    EIAError("base", attempts=1),
    EIAClientClosedError("cerrado"),
    EIATransportError("red", kind="timeout", attempts=3),
    EIAHTTPError("http", status_code=500, eia_code="X", retry_after=1.5, attempts=2),
    EIABadRequestError("malo", status_code=400, eia_code="400"),
    EIAAuthError("auth", status_code=403, eia_code="API_KEY_MISSING"),
    EIARateLimitError("limite", status_code=429, retry_after=120.0, attempts=1),
    EIAServerError("servidor", status_code=503, attempts=3),
    EIAResponseError("respuesta", status_code=200, attempts=1),
]
IDS = [type(e).__name__ for e in ALL_EXCEPTIONS]


def test_hierarchy() -> None:
    assert issubclass(EIATransportError, EIAError)
    assert issubclass(EIAResponseError, EIAError)
    assert issubclass(EIAClientClosedError, EIAError)
    assert issubclass(EIAHTTPError, EIAError)
    for cls in (EIABadRequestError, EIAAuthError, EIARateLimitError, EIAServerError):
        assert issubclass(cls, EIAHTTPError)


def test_http_error_exposes_only_safe_fields() -> None:
    exc = EIARateLimitError("limite", status_code=429, eia_code="RL", retry_after=60.0, attempts=2)

    assert (exc.status_code, exc.eia_code, exc.retry_after, exc.attempts) == (429, "RL", 60.0, 2)
    assert exc.message == "limite"
    assert str(exc) == "limite"


def test_transport_error_exposes_kind() -> None:
    exc = EIATransportError("red", kind="connect", attempts=3)

    assert exc.kind == "connect"
    assert exc.attempts == 3


def test_repr_lists_fields_and_quotes_the_message() -> None:
    exc = EIAServerError("linea1\nlinea2", status_code=503, attempts=3)

    text = repr(exc)

    assert text.startswith("EIAServerError(")
    assert "status_code=503" in text
    assert "attempts=3" in text
    assert "\n" not in text  # el salto de linea queda escapado dentro del repr


@pytest.mark.parametrize("exc", ALL_EXCEPTIONS, ids=IDS)
def test_new_exceptions_have_no_context_or_cause(exc: EIAError) -> None:
    assert exc.__context__ is None
    assert exc.__cause__ is None


@pytest.mark.parametrize("exc", ALL_EXCEPTIONS, ids=IDS)
def test_pickle_round_trip_preserves_type_and_fields(exc: EIAError) -> None:
    restored = pickle.loads(pickle.dumps(exc))

    assert type(restored) is type(exc)
    assert str(restored) == str(exc)
    assert restored.__dict__ == exc.__dict__


@pytest.mark.parametrize("exc", ALL_EXCEPTIONS, ids=IDS)
def test_copy_and_deepcopy_preserve_type_and_fields(exc: EIAError) -> None:
    for clone in (copy.copy(exc), copy.deepcopy(exc)):
        assert type(clone) is type(exc)
        assert clone.__dict__ == exc.__dict__
