"""Pruebas de comportamiento del cliente de EIA. Sin red real: todo se simula con respx.

Las pruebas de que la API key no se filtra estan en `test_client_security.py`.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from pipeline_lab.config import Secret, Settings
from pipeline_lab.eia import (
    EIAAuthError,
    EIABadRequestError,
    EIAClient,
    EIAClientClosedError,
    EIAPage,
    EIARateLimitError,
    EIAResponseError,
    EIAServerError,
    EIATransportError,
    RetryPolicy,
    Timeouts,
)

MakeClient = Callable[..., EIAClient]


def ok_json(rows: int = 2, total: object = "2", **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "response": {
            "total": total,
            "dateFormat": 'YYYY-MM-DD"T"HH24',
            "frequency": "hourly",
            "data": [
                {
                    "period": f"2026-11-10T{i:02d}",
                    "respondent": "CISO",
                    "respondent-name": "California Independent System Operator",
                    "type": "D",
                    "type-name": "Demand",
                    "value": "27053",
                    "value-units": "megawatthours",
                }
                for i in range(rows)
            ],
        },
        "request": {"command": "/v2/electricity/rto/region-data/data/", "params": {}},
        "apiVersion": "2.1.14",
    }
    payload.update(extra)
    return payload


def ok_response(**kwargs: Any) -> httpx.Response:
    return httpx.Response(200, json=ok_json(**kwargs))


# ======================================================================== respuesta correcta
def test_returns_a_validated_page(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response(rows=2, total="2"))

    with make_client() as client:
        page = client.get_data()

    assert isinstance(page, EIAPage)
    assert page.total == 2
    assert len(page.rows) == 2
    assert page.rows[0]["respondent"] == "CISO"
    assert page.rows[1]["period"] == "2026-11-10T01"
    assert page.frequency == "hourly"
    assert page.warnings == ()
    assert route.call_count == 1


def test_page_is_immutable(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        page = client.get_data()

    assert isinstance(page.rows, tuple)
    with pytest.raises(dataclasses.FrozenInstanceError):
        page.total = 99  # type: ignore[misc]
    with pytest.raises(TypeError):
        page.rows[0]["value"] = "0"  # type: ignore[index]


def test_page_does_not_keep_the_request_echo(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        page = client.get_data()

    assert not hasattr(page, "request")
    assert {f.name for f in dataclasses.fields(page)} == {"total", "rows", "frequency", "warnings"}


@pytest.mark.parametrize("total", ["0", 0, "12", 12, " 7 "])
def test_total_accepts_integers_and_numeric_strings(
    total: object, route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=ok_response(rows=0, total=total))
    with make_client() as client:
        page = client.get_data()

    assert page.total == int(str(total).strip())


def test_warnings_are_exposed(route: respx.Route, make_client: MakeClient) -> None:
    warnings = [{"warning": "incomplete return", "description": "The API can only return 5000."}]
    route.mock(return_value=ok_response(warnings=warnings))
    with make_client() as client:
        page = client.get_data()

    assert page.warnings == ("incomplete return - The API can only return 5000.",)


# ========================================================== parametros y API key enviados
def test_sends_every_parameter_and_the_api_key(
    route: respx.Route, make_client: MakeClient, fake_key: str
) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        client.get_data(
            frequency="hourly",
            data=["value"],
            facets={"respondent": ["CISO", "SWPP"], "type": ["D"]},
            start="2026-11-10T00",
            end="2026-11-10T23",
            sort=[("period", "asc"), ("respondent", "asc"), ("type", "asc")],
            offset=5000,
            length=100,
        )

    request = route.calls.last.request
    items = request.url.params.multi_items()
    assert items[:-1] == [
        ("frequency", "hourly"),
        ("data[0]", "value"),
        ("facets[respondent][]", "CISO"),
        ("facets[respondent][]", "SWPP"),
        ("facets[type][]", "D"),
        ("start", "2026-11-10T00"),
        ("end", "2026-11-10T23"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", "respondent"),
        ("sort[1][direction]", "asc"),
        ("sort[2][column]", "type"),
        ("sort[2][direction]", "asc"),
        ("offset", "5000"),
        ("length", "100"),
    ]
    assert items[-1] == ("api_key", fake_key)  # la clave se envia, como parametro, una sola vez
    assert [k for k, _ in items].count("api_key") == 1


def test_request_shape(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        client.get_data()

    request = route.calls.last.request
    assert request.method == "GET"
    assert request.url.scheme == "https"
    assert request.url.host == "api.eia.gov"
    assert request.url.path == "/v2/electricity/rto/region-data/data"
    assert request.headers["user-agent"].startswith("data-pipeline-lab/")
    assert request.headers["accept"] == "application/json"
    assert "cookie" not in request.headers and "authorization" not in request.headers


def test_defaults(route: respx.Route, make_client: MakeClient, fake_key: str) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        client.get_data()

    items = route.calls.last.request.url.params.multi_items()
    assert items == [
        ("frequency", "hourly"),
        ("data[0]", "value"),
        ("offset", "0"),
        ("length", "5000"),
        ("api_key", fake_key),
    ]


def test_from_settings_uses_the_configured_key(route: respx.Route, fake_key: str) -> None:
    route.mock(return_value=ok_response())
    settings = Settings(api_key=Secret(fake_key))

    with EIAClient.from_settings(settings) as client:
        client.get_data(length=1)

    assert route.calls.last.request.url.params["api_key"] == fake_key


# ================================================================ validacion de argumentos
@pytest.mark.parametrize(
    "kwargs",
    [
        {"length": 0},
        {"length": 5001},
        {"length": -1},
        {"length": True},
        {"length": "5"},
        {"offset": -1},
        {"offset": True},
        {"offset": 1.5},
        {"frequency": ""},
        {"frequency": "   "},
        {"data": []},
        {"data": "value"},
        {"data": ["va lue"]},
        {"data": [""]},
        {"facets": {"bad name": ["x"]}},
        {"facets": {"respondent": []}},
        {"facets": {"respondent": "CISO"}},
        {"facets": {"respondent": [""]}},
        {"sort": [("period", "up")]},
        {"sort": [("period",)]},
        {"sort": [("bad col", "asc")]},
        {"start": ""},
        {"end": 5},
    ],
    ids=lambda kw: str(kw)[:40],
)
def test_invalid_arguments_raise_value_error_without_any_request(
    kwargs: dict[str, Any], route: respx.Route, make_client: MakeClient
) -> None:
    with make_client() as client, pytest.raises(ValueError):
        client.get_data(**kwargs)

    assert route.call_count == 0


def test_length_limits_are_accepted(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        client.get_data(length=1)
        client.get_data(length=5000)

    assert route.call_count == 2


@pytest.mark.parametrize("base_url", ["http://api.eia.gov", "ftp://api.eia.gov", "api.eia.gov", ""])
def test_base_url_must_be_https(base_url: str, fake_key: str) -> None:
    with pytest.raises(ValueError, match="https"):
        EIAClient(Secret(fake_key), base_url=base_url)


def test_api_key_must_be_a_secret(fake_key: str) -> None:
    with pytest.raises(TypeError):
        EIAClient(fake_key)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"connect": 0},
        {"read": -1},
        {"write": True},
        {"pool": "5"},
    ],
)
def test_invalid_timeouts_are_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="Timeouts"):
        Timeouts(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": True},
        {"max_attempts": 1.5},
        {"initial_backoff": 0},
        {"initial_backoff": 2.0, "max_backoff": 1.0},
        {"multiplier": 0.5},
        {"max_retry_after": -1},
    ],
)
def test_invalid_retry_policy_is_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="RetryPolicy"):
        RetryPolicy(**kwargs)


# =================================================================== timeouts y redirecciones
def test_default_timeouts_are_applied_independently(make_client: MakeClient) -> None:
    client = make_client()

    assert client._http.timeout == httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)


def test_custom_timeouts_are_applied(make_client: MakeClient) -> None:
    client = make_client(timeouts=Timeouts(connect=1, read=2, write=3, pool=4))

    assert client._http.timeout == httpx.Timeout(connect=1.0, read=2.0, write=3.0, pool=4.0)


def test_redirects_are_not_followed(make_client: MakeClient) -> None:
    assert make_client()._http.follow_redirects is False


def test_a_redirect_is_an_error_and_the_target_is_never_requested(
    route: respx.Route, router: respx.MockRouter, make_client: MakeClient
) -> None:
    target = router.get(host="evil.example").mock(return_value=ok_response())
    route.mock(return_value=httpx.Response(302, headers={"Location": "https://evil.example/x"}))

    with make_client() as client, pytest.raises(EIAResponseError) as info:
        client.get_data()

    assert info.value.status_code == 302
    assert "redirecc" in str(info.value).lower()
    assert route.call_count == 1  # una redireccion no se reintenta
    assert target.call_count == 0


# ===================================================================== reintentos (HTTP)
def test_retries_503_and_then_succeeds(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.Response(503), httpx.Response(503), ok_response()]

    with make_client() as client:
        page = client.get_data()

    assert page.total == 2
    assert route.call_count == 3
    assert sleeps == [0.5, 1.0]  # rng fijo en 0.5 x techos de 1 s y 2 s


@pytest.mark.parametrize(
    ("status", "exc_type"),
    [
        (500, EIAServerError),
        (502, EIAServerError),
        (503, EIAServerError),
        (504, EIAServerError),
        (599, EIAServerError),
        (429, EIARateLimitError),
    ],
)
def test_transient_http_errors_are_retried_until_attempts_run_out(
    status: int,
    exc_type: type[Exception],
    route: respx.Route,
    make_client: MakeClient,
    sleeps: list[float],
) -> None:
    route.side_effect = lambda request: httpx.Response(status)

    with make_client() as client, pytest.raises(exc_type) as info:
        client.get_data()

    assert route.call_count == 3  # max_attempts por defecto
    assert sleeps == [0.5, 1.0]  # solo se espera ENTRE intentos
    assert info.value.attempts == 3
    assert info.value.status_code == status
    assert "tras 3 intentos" in str(info.value)


def test_backoff_is_exponential_jittered_and_capped(
    route: respx.Route, sleeps: list[float]
) -> None:
    route.side_effect = lambda request: httpx.Response(503)
    policy = RetryPolicy(max_attempts=6)

    with (
        EIAClient(Secret("k" * 20), retry=policy, sleep=sleeps.append, rng=lambda: 1.0) as c,
        pytest.raises(EIAServerError),
    ):
        c.get_data()

    # techos: 1, 2, 4, 8 y, desde ahi, limitado por max_backoff = 8
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 8.0]


def test_jitter_can_be_zero(route: respx.Route, sleeps: list[float]) -> None:
    route.side_effect = lambda request: httpx.Response(503)

    with (
        EIAClient(Secret("k" * 20), sleep=sleeps.append, rng=lambda: 0.0) as client,
        pytest.raises(EIAServerError),
    ):
        client.get_data()

    assert sleeps == [0.0, 0.0]


def test_delay_never_exceeds_the_configured_maximum(route: respx.Route) -> None:
    route.side_effect = lambda request: httpx.Response(503)
    delays: list[float] = []
    policy = RetryPolicy(max_attempts=10, initial_backoff=1.0, max_backoff=3.0)
    values = iter([0.1, 0.9, 0.99, 0.5, 0.75, 0.3, 0.8, 0.2, 0.6])

    with (
        EIAClient(
            Secret("k" * 20), retry=policy, sleep=delays.append, rng=lambda: next(values)
        ) as client,
        pytest.raises(EIAServerError),
    ):
        client.get_data()

    assert len(delays) == 9
    assert all(0.0 <= d <= 3.0 for d in delays)


def test_single_attempt_policy_never_retries_or_sleeps(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = lambda request: httpx.Response(503)

    with make_client(retry=RetryPolicy(max_attempts=1)) as client, pytest.raises(EIAServerError):
        client.get_data()

    assert route.call_count == 1
    assert sleeps == []


# ============================================================================ Retry-After
def test_retry_after_in_seconds_replaces_the_backoff(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.Response(429, headers={"Retry-After": "2"}), ok_response()]

    with make_client() as client:
        client.get_data()

    assert sleeps == [2.0]  # no 0.5 del jitter


def test_retry_after_is_honoured_on_503_too(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.Response(503, headers={"Retry-After": "3"}), ok_response()]

    with make_client() as client:
        client.get_data()

    assert sleeps == [3.0]


def test_retry_after_as_http_date(route: respx.Route, sleeps: list[float]) -> None:
    now = datetime(2026, 10, 21, 7, 27, 50, tzinfo=UTC)
    header = "Wed, 21 Oct 2026 07:28:00 GMT"
    route.side_effect = [httpx.Response(429, headers={"Retry-After": header}), ok_response()]

    with EIAClient(Secret("k" * 20), sleep=sleeps.append, clock=lambda: now) as client:
        client.get_data()

    assert sleeps == [10.0]


def test_retry_after_date_in_the_past_means_retry_immediately(
    route: respx.Route, sleeps: list[float]
) -> None:
    now = datetime(2026, 10, 21, 9, 0, 0, tzinfo=UTC)
    header = "Wed, 21 Oct 2026 07:28:00 GMT"
    route.side_effect = [httpx.Response(429, headers={"Retry-After": header}), ok_response()]

    with EIAClient(Secret("k" * 20), sleep=sleeps.append, clock=lambda: now) as client:
        client.get_data()

    assert sleeps == [0.0]


@pytest.mark.parametrize("value", ["soon", "-5", "", "inf", "nan", "1e3", "12abc"])
def test_invalid_retry_after_falls_back_to_the_backoff(
    value: str, route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.Response(429, headers={"Retry-After": value}), ok_response()]

    with make_client() as client:
        client.get_data()

    assert sleeps == [0.5]


def test_retry_after_above_the_limit_stops_without_retrying(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = lambda request: httpx.Response(429, headers={"Retry-After": "120"})

    with make_client() as client, pytest.raises(EIARateLimitError) as info:
        client.get_data()

    assert route.call_count == 1  # no se reintenta antes de lo que pide el servidor
    assert sleeps == []
    assert info.value.retry_after == 120.0
    assert info.value.attempts == 1
    assert "120 s" in str(info.value)
    assert "30 s" in str(info.value)
    assert "no se reintenta" in str(info.value)


def test_retry_after_equal_to_the_limit_is_honoured(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.Response(429, headers={"Retry-After": "30"}), ok_response()]

    with make_client() as client:
        client.get_data()

    assert sleeps == [30.0]


def test_the_retry_after_limit_is_configurable(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = lambda request: httpx.Response(503, headers={"Retry-After": "6"})

    with (
        make_client(retry=RetryPolicy(max_retry_after=5.0)) as client,
        pytest.raises(EIAServerError) as info,
    ):
        client.get_data()

    assert route.call_count == 1
    assert sleeps == []
    assert info.value.retry_after == 6.0


def test_retry_after_on_every_attempt_waits_that_long_between_attempts(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = lambda request: httpx.Response(429, headers={"Retry-After": "1"})

    with make_client() as client, pytest.raises(EIARateLimitError) as info:
        client.get_data()

    assert route.call_count == 3
    assert sleeps == [1.0, 1.0]
    assert info.value.retry_after == 1.0


# ======================================================== errores permanentes: sin reintentos
@pytest.mark.parametrize(
    ("status", "exc_type"),
    [
        (400, EIABadRequestError),
        (401, EIAAuthError),
        (403, EIAAuthError),
        (404, EIABadRequestError),
        (422, EIABadRequestError),
    ],
)
def test_permanent_client_errors_are_never_retried(
    status: int,
    exc_type: type[Exception],
    route: respx.Route,
    make_client: MakeClient,
    sleeps: list[float],
) -> None:
    route.mock(return_value=httpx.Response(status))

    with make_client() as client, pytest.raises(exc_type) as info:
        client.get_data()

    assert route.call_count == 1
    assert sleeps == []
    assert info.value.status_code == status
    assert info.value.attempts == 1
    assert "intentos" not in str(info.value)


# ================================================================ errores de red y timeouts
TRANSPORT_CASES = [
    (lambda: httpx.ConnectTimeout("t"), "timeout"),
    (lambda: httpx.ReadTimeout("t"), "timeout"),
    (lambda: httpx.WriteTimeout("t"), "timeout"),
    (lambda: httpx.PoolTimeout("t"), "timeout"),
    (lambda: httpx.ConnectError("c"), "connect"),
    (lambda: httpx.ReadError("r"), "network"),
    (lambda: httpx.WriteError("w"), "network"),
    (lambda: httpx.RemoteProtocolError("p"), "protocol"),
]


@pytest.mark.parametrize(("factory", "kind"), TRANSPORT_CASES, ids=[k for _, k in TRANSPORT_CASES])
def test_transient_transport_errors_are_retried_then_reported(
    factory: Callable[[], Exception],
    kind: str,
    route: respx.Route,
    make_client: MakeClient,
    sleeps: list[float],
) -> None:
    route.side_effect = [factory(), factory(), factory()]

    with make_client() as client, pytest.raises(EIATransportError) as info:
        client.get_data()

    assert info.value.kind == kind
    assert info.value.attempts == 3
    assert route.call_count == 3
    assert sleeps == [0.5, 1.0]


def test_a_timeout_and_a_connection_error_can_recover(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.ReadTimeout("t"), httpx.ConnectError("c"), ok_response()]

    with make_client() as client:
        page = client.get_data()

    assert page.total == 2
    assert route.call_count == 3
    assert sleeps == [0.5, 1.0]


def test_non_transient_transport_errors_are_not_retried(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.side_effect = [httpx.LocalProtocolError("bad request")]

    with make_client() as client, pytest.raises(EIATransportError) as info:
        client.get_data()

    assert info.value.kind == "other"
    assert route.call_count == 1
    assert sleeps == []


def test_an_unmocked_request_is_reported_as_a_transport_error_not_sent_anywhere(
    router: respx.MockRouter, make_client: MakeClient
) -> None:
    # Sin ruta simulada, respx rechaza la peticion: ninguna prueba sale a Internet.
    with (
        make_client(retry=RetryPolicy(max_attempts=1)) as client,
        pytest.raises(EIATransportError) as info,
    ):
        client.get_data()

    assert info.value.kind == "other"


# ======================================================== formatos de error de EIA (403 y 400)
def test_eia_403_error_object_format(route: respx.Route, make_client: MakeClient) -> None:
    body = {
        "error": {
            "code": "API_KEY_MISSING",
            "message": "No api_key was supplied.  Please register for one at https://www.eia.gov/opendata/register.php",
        }
    }
    route.mock(return_value=httpx.Response(403, json=body))

    with make_client() as client, pytest.raises(EIAAuthError) as info:
        client.get_data()

    assert info.value.eia_code == "API_KEY_MISSING"
    assert "[API_KEY_MISSING]" in str(info.value)
    # EIA envia dos espacios tras el punto; el saneado colapsa las rachas de espacios
    assert "No api_key was supplied. Please register" in str(info.value)


def test_eia_400_error_as_plain_text(route: respx.Route, make_client: MakeClient) -> None:
    body = {
        "error": "Invalid frequency 'daily' provided. The only valid frequencies are 'hourly'.",
        "code": 400,
    }
    route.mock(return_value=httpx.Response(400, json=body))

    with make_client() as client, pytest.raises(EIABadRequestError) as info:
        client.get_data()

    assert info.value.eia_code == "400"
    assert "Invalid frequency 'daily' provided." in str(info.value)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(400, text="<html><body>Bad Request</body></html>"),
        httpx.Response(400, content=b""),
        httpx.Response(400, json=["lista"]),
        httpx.Response(400, json={"error": 5}),
        httpx.Response(400, json={"error": None}),
        httpx.Response(400, json={"otra": "cosa"}),
        httpx.Response(400, content=b'{"error": "texto cortado'),
        httpx.Response(400, content=b"\xff\xfe\x00 no es utf-8"),
    ],
    ids=[
        "html",
        "vacio",
        "lista",
        "error-numero",
        "error-null",
        "sin-error",
        "json-roto",
        "binario",
    ],
)
def test_unknown_error_shapes_give_a_generic_message(
    response: httpx.Response, route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=response)

    with make_client() as client, pytest.raises(EIABadRequestError) as info:
        client.get_data()

    assert str(info.value) == "EIA respondio con HTTP 400"
    assert info.value.eia_code is None


def test_eia_message_is_truncated_to_200_characters(
    route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=httpx.Response(400, json={"error": "x" * 500, "code": 400}))

    with make_client() as client, pytest.raises(EIABadRequestError) as info:
        client.get_data()

    assert "x" * 197 + "..." in str(info.value)
    assert "x" * 198 not in str(info.value)


def test_control_characters_in_eia_message_are_removed(
    route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=httpx.Response(400, json={"error": "a\r\nb\x00c\td", "code": 400}))

    with make_client() as client, pytest.raises(EIABadRequestError) as info:
        client.get_data()

    assert "a b c d" in str(info.value)
    assert "\n" not in str(info.value) and "\x00" not in str(info.value)


@pytest.mark.parametrize("code", ["bad code!", "x" * 80, "", None, True, ["a"], {"a": 1}])
def test_suspicious_eia_codes_are_discarded(
    code: object, route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=httpx.Response(400, json={"error": {"code": code, "message": "m"}}))

    with make_client() as client, pytest.raises(EIABadRequestError) as info:
        client.get_data()

    assert info.value.eia_code is None
    assert str(info.value) == "EIA respondio con HTTP 400: m"


# ======================================== HTTP 200 que no cumple el contrato (no se reintenta)
def test_http_200_with_invalid_json_is_a_response_error_and_not_retried(
    route: respx.Route, make_client: MakeClient, sleeps: list[float]
) -> None:
    route.mock(return_value=httpx.Response(200, text="<html>Gateway page</html>"))

    with make_client() as client, pytest.raises(EIAResponseError) as info:
        client.get_data()

    assert "JSON" in str(info.value)
    assert info.value.status_code == 200
    assert info.value.attempts == 1
    assert route.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("payload", "fragment"),
    [
        ([], "no es un objeto"),
        ("texto", "no es un objeto"),
        (None, "no es un objeto"),
        ({}, "'response'"),
        ({"response": []}, "'response'"),
        ({"response": {"total": "1"}}, "response.data"),
        ({"response": {"total": "1", "data": "x"}}, "response.data"),
        ({"response": {"total": "1", "data": {"a": 1}}}, "response.data"),
        ({"response": {"total": "1", "data": [1, 2]}}, "no son objetos"),
        ({"response": {"data": []}}, "response.total"),
        ({"response": {"total": "abc", "data": []}}, "response.total"),
        ({"response": {"total": "1e3", "data": []}}, "response.total"),
        ({"response": {"total": -1, "data": []}}, "response.total"),
        ({"response": {"total": 1.5, "data": []}}, "response.total"),
        ({"response": {"total": True, "data": []}}, "response.total"),
        ({"response": {"total": None, "data": []}}, "response.total"),
    ],
)
def test_incomplete_or_malformed_structure_is_a_response_error(
    payload: object,
    fragment: str,
    route: respx.Route,
    make_client: MakeClient,
    sleeps: list[float],
) -> None:
    # content explicito: con json=None httpx enviaria un cuerpo vacio, no el literal `null`.
    body = json.dumps(payload).encode()
    route.mock(
        return_value=httpx.Response(200, content=body, headers={"Content-Type": "application/json"})
    )

    with make_client() as client, pytest.raises(EIAResponseError) as info:
        client.get_data()

    assert fragment in str(info.value)
    assert route.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize("status", [204, 205, 100])
def test_unexpected_success_class_statuses_are_response_errors(
    status: int, route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=httpx.Response(status))

    with make_client() as client, pytest.raises(EIAResponseError) as info:
        client.get_data()

    assert info.value.status_code == status
    assert route.call_count == 1


def test_non_object_warnings_are_ignored(route: respx.Route, make_client: MakeClient) -> None:
    route.mock(return_value=ok_response(warnings=["texto", 5, {"nada": 1}, None]))
    with make_client() as client:
        page = client.get_data()

    assert page.warnings == ()


# ====================================================================== ciclo de vida
def test_context_manager_closes_the_http_client(make_client: MakeClient) -> None:
    client = make_client()
    with client:
        assert client._http.is_closed is False

    assert client._http.is_closed is True


def test_the_client_is_closed_even_if_the_block_raises(make_client: MakeClient) -> None:
    client = make_client()

    with pytest.raises(RuntimeError, match="boom"), client:
        raise RuntimeError("boom")

    assert client._http.is_closed is True


def test_the_client_is_closed_after_an_eia_error(
    route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=httpx.Response(403))
    client = make_client()

    with pytest.raises(EIAAuthError), client:
        client.get_data()

    assert client._http.is_closed is True


def test_close_is_idempotent(make_client: MakeClient) -> None:
    client = make_client()

    client.close()
    client.close()

    assert client._http.is_closed is True


def test_using_a_closed_client_raises_and_sends_nothing(
    route: respx.Route, make_client: MakeClient
) -> None:
    client = make_client()
    client.close()

    with pytest.raises(EIAClientClosedError):
        client.get_data()

    assert route.call_count == 0


def test_a_failed_validation_does_not_leave_the_client_unusable(
    route: respx.Route, make_client: MakeClient
) -> None:
    route.mock(return_value=ok_response())
    with make_client() as client:
        with pytest.raises(ValueError):
            client.get_data(length=0)
        assert client.get_data().total == 2


def test_repr_describes_the_configuration_without_the_key(
    make_client: MakeClient, fake_key: str
) -> None:
    text = repr(make_client())

    assert text.startswith("EIAClient(base_url='https://api.eia.gov'")
    assert "RetryPolicy(max_attempts=3" in text
    assert fake_key not in text


def test_constructing_a_client_silences_the_http_loggers(fake_key: str) -> None:
    import logging

    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.DEBUG)

    EIAClient(Secret(fake_key)).close()

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
