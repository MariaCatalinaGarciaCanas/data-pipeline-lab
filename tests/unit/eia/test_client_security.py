"""Pruebas de seguridad del cliente: la API key (FICTICIA) no debe aparecer en ningun sitio.

Que se comprueba, para cada escenario hostil (el servidor o httpx repiten la clave):
  str, repr, args y vars() de la excepcion; traceback completo; traceback con variables locales
  (modulo `traceback` y `pytest --showlocals`); `__context__` y `__cause__`; el grafo de
  objetos alcanzable desde la excepcion; objetos de httpx en las variables locales de los
  frames del traceback; logs (incluso con los loggers de httpx forzados a DEBUG).

LIMITES (ver docs/adr/0001-eia-api-key-handling.md): estas pruebas NO demuestran proteccion
frente a hooks o instrumentacion de httpx, proxies, el servidor, ni acceso a la memoria del
proceso. `test_documented_limit_*` deja constancia de ello.
"""

from __future__ import annotations

import copy
import io
import logging
import pickle
import re
import traceback
import types
from collections.abc import Callable, Iterator
from urllib.parse import quote

import httpx
import pytest
import respx

from pipeline_lab.config import Secret
from pipeline_lab.eia import (
    EIAAuthError,
    EIABadRequestError,
    EIAClient,
    EIAError,
    EIARateLimitError,
    EIAResponseError,
    EIAServerError,
    EIATransportError,
)
from pipeline_lab.logging_config import configure_logging

# Clave FICTICIA (la misma forma que usa el resto de pruebas: incluye caracteres que se codifican)
FAKE_KEY = "fake-key/with+chars=DO-NOT-USE-0123456789"
ENCODED = quote(FAKE_KEY, safe="")
VARIANTS = (FAKE_KEY, ENCODED, quote(FAKE_KEY))
DATA_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data"

Handler = Callable[[httpx.Request], httpx.Response]


def _json(status: int, body: object, **kwargs: object) -> httpx.Response:
    return httpx.Response(status, json=body, **kwargs)  # type: ignore[arg-type]


def _raising(factory: Callable[[httpx.Request], Exception]) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        raise factory(request)

    return handler


# nombre -> (manejador del servidor simulado, tipo de excepcion esperado)
SCENARIOS: dict[str, tuple[Handler, type[EIAError]]] = {
    "400-eco-literal": (
        lambda r: _json(400, {"error": f"bad request api_key={FAKE_KEY}", "code": 400}),
        EIABadRequestError,
    ),
    "400-eco-codificado": (
        lambda r: _json(400, {"error": f"bad request api_key={ENCODED}", "code": 400}),
        EIABadRequestError,
    ),
    "400-eco-parcial": (
        lambda r: _json(400, {"error": f"clave {FAKE_KEY[:12]} invalida", "code": 400}),
        EIABadRequestError,
    ),
    "400-eco-en-el-codigo": (
        lambda r: _json(400, {"error": {"code": FAKE_KEY, "message": "x"}}),
        EIABadRequestError,
    ),
    "403-objeto-eco": (
        lambda r: _json(
            403, {"error": {"code": "API_KEY_INVALID", "message": f"key {FAKE_KEY} rejected"}}
        ),
        EIAAuthError,
    ),
    "403-eco-de-la-url-real": (
        lambda r: _json(403, {"error": {"code": "E", "message": f"GET {r.url}"}}),
        EIAAuthError,
    ),
    "429-agotado": (
        lambda r: _json(429, {"error": f"slow down {ENCODED}", "code": 429}),
        EIARateLimitError,
    ),
    "429-retry-after-largo": (
        lambda r: _json(
            429, {"error": f"wait {FAKE_KEY}", "code": 429}, headers={"Retry-After": "999"}
        ),
        EIARateLimitError,
    ),
    "503-agotado": (
        lambda r: _json(503, {"error": f"down {FAKE_KEY}", "code": 503}),
        EIAServerError,
    ),
    "timeout-url-en-el-mensaje": (
        _raising(lambda r: httpx.ReadTimeout(f"timed out: {r.url}", request=r)),
        EIATransportError,
    ),
    "conexion-url-en-el-mensaje": (
        _raising(lambda r: httpx.ConnectError(f"cannot connect to {r.url}", request=r)),
        EIATransportError,
    ),
    "protocolo-url-en-el-mensaje": (
        _raising(lambda r: httpx.LocalProtocolError(f"bad request {r.url}", request=r)),
        EIATransportError,
    ),
    "200-json-invalido-eco": (
        lambda r: httpx.Response(200, text=f"<html>{FAKE_KEY}</html>"),
        EIAResponseError,
    ),
    "200-estructura-incompleta-eco": (
        lambda r: _json(200, {"response": {"data": "x"}, "eco": FAKE_KEY}),
        EIAResponseError,
    ),
    "200-total-invalido-eco": (
        lambda r: _json(200, {"response": {"data": [], "total": FAKE_KEY}}),
        EIAResponseError,
    ),
    "302-location-con-la-clave": (
        lambda r: httpx.Response(302, headers={"Location": f"https://evil.example/?k={FAKE_KEY}"}),
        EIAResponseError,
    ),
}


# ------------------------------------------------------------------------------------ ayudas
_FRAGMENT = 8  # un trozo literal de la clave de esta longitud ya cuenta como fuga
_WINDOWS = {
    variant[i : i + _FRAGMENT] for variant in VARIANTS for i in range(len(variant) - _FRAGMENT + 1)
}


def _has_key(text: str) -> bool:
    """True si `text` contiene la clave (literal o codificada) o un trozo de 8+ caracteres."""
    return any(window in text for window in _WINDOWS)


def _capture(*, force_http_debug: bool = False) -> EIAError:
    """Ejecuta una llamada que debe fallar y devuelve la excepcion.

    A proposito NO guarda la clave en ninguna variable local ni usa fixtures: asi el frame de
    esta funcion (que aparece en el traceback) no puede ser la causa de una falsa fuga.
    """
    client = EIAClient(Secret(FAKE_KEY), sleep=lambda seconds: None, rng=lambda: 0.5)
    if force_http_debug:
        # El constructor silencia httpx/httpcore; se fuerzan a DEBUG para probar el enmascarado.
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).setLevel(logging.DEBUG)
    try:
        with client:
            client.get_data(length=5)
    except EIAError as exc:
        return exc
    raise AssertionError("se esperaba una excepcion del cliente")


_SKIP_TYPES = (
    type,
    types.ModuleType,
    types.FunctionType,
    types.BuiltinFunctionType,
    types.CodeType,
    types.MethodType,
    int,
    float,
    bool,
    type(None),
)
_HTTPX_TYPES = (httpx.Request, httpx.Response, httpx.URL, httpx.Client, httpx.Headers)


def _graph_leaks(root: BaseException, limit: int = 400_000) -> list[str]:
    """Recorre los objetos alcanzables desde la excepcion (incluidas las variables locales de los
    frames de su traceback) y devuelve el tipo contenedor de cada cadena que contiene la clave.

    `Secret` se trata como opaco: el cliente lo guarda A PROPOSITO y es alcanzable desde `self`
    (limite documentado en el ADR). Todo lo demas debe estar limpio.
    """
    seen: set[int] = set()
    stack: list[tuple[object, str]] = [(root, "exc")]
    leaks: list[str] = []
    while stack and len(seen) < limit:
        obj, path = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        if isinstance(obj, str | bytes | bytearray):
            text = obj if isinstance(obj, str) else bytes(obj).decode("latin-1")
            if _has_key(text):
                leaks.append(path.rsplit(">", 1)[-1])
            continue
        if isinstance(obj, (*_SKIP_TYPES, Secret)):
            continue
        if isinstance(obj, types.TracebackType):
            stack.append((obj.tb_frame, path + ">tb_frame"))
            if obj.tb_next is not None:
                stack.append((obj.tb_next, path + ">tb_next"))
            continue
        if isinstance(obj, types.FrameType):  # solo sus variables locales, no los frames padre
            stack.append((dict(obj.f_locals), path + ">locals"))
            continue
        for referent in __import__("gc").get_referents(obj):
            stack.append((referent, f"{path}>{type(obj).__name__}"))
    return sorted(set(leaks))


def _httpx_objects_in_frames(exc: BaseException) -> list[str]:
    """Objetos de httpx guardados en variables locales de los frames del traceback."""
    found: list[str] = []
    tb = exc.__traceback__
    while tb is not None:
        for name, value in tb.tb_frame.f_locals.items():
            if isinstance(value, _HTTPX_TYPES):
                found.append(f"{tb.tb_frame.f_code.co_name}.{name}:{type(value).__name__}")
        tb = tb.tb_next
    return found


def _leaking_surfaces(exc: BaseException) -> list[str]:
    """Nombres (nunca contenidos) de las superficies donde aparece la clave."""
    leaks: list[str] = []

    def check(name: str, text: str) -> None:
        if _has_key(text):
            leaks.append(name)

    check("str", str(exc))
    check("repr", repr(exc))
    check("args", repr(exc.args))
    check("vars", repr(vars(exc)))
    check("format_exception", "".join(traceback.format_exception(exc)))
    check(
        "traceback+locals",
        "".join(traceback.TracebackException.from_exception(exc, capture_locals=True).format()),
    )
    info = pytest.ExceptionInfo.from_exception(exc)  # type: ignore[arg-type]
    check("pytest-showlocals", str(info.getrepr(showlocals=True, style="long", chain=True)))
    check("pytest-short", str(info.getrepr(style="short", chain=True)))
    if exc.__context__ is not None:
        leaks.append("__context__")
    if exc.__cause__ is not None:
        leaks.append("__cause__")
    leaks += [f"grafo:{owner}" for owner in _graph_leaks(exc)]
    leaks += [f"httpx-en-locals:{where}" for where in _httpx_objects_in_frames(exc)]
    return leaks


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Restaura handlers y niveles que estas pruebas cambian."""
    root = logging.getLogger()
    client_logger = logging.getLogger("pipeline_lab.eia.client")
    saved = (list(root.handlers), root.level, list(client_logger.handlers), client_logger.level)
    levels = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    root.handlers, root.level = saved[0], saved[1]
    client_logger.handlers, client_logger.level = saved[2], saved[3]
    for name, level in levels.items():
        logging.getLogger(name).setLevel(level)


# =============================================== los detectores detectan (prueba de los oraculos)
def _raised(exc: BaseException) -> BaseException:
    """Lanza y captura `exc` para que tenga traceback, como una excepcion real."""
    try:
        raise exc
    except BaseException as caught:  # noqa: BLE001
        return caught


def _leak_in_message() -> BaseException:
    try:
        raise RuntimeError(f"fallo en {DATA_URL}?api_key={FAKE_KEY}")
    except RuntimeError as exc:
        return exc


def _leak_via_context_only() -> BaseException:
    try:
        try:
            raise RuntimeError(f"original {FAKE_KEY}")
        except RuntimeError:
            raise EIAError("mensaje limpio") from None  # `from None` NO elimina __context__
    except EIAError as exc:
        return exc


def _leak_via_attribute() -> BaseException:
    exc = EIAError("mensaje limpio")
    exc.request = httpx.Request("GET", DATA_URL, params={"api_key": FAKE_KEY})  # type: ignore[attr-defined]
    return _raised(exc)


def _leak_via_frame_local() -> BaseException:
    def inner() -> None:
        secret_in_a_local = f"{FAKE_KEY}"  # noqa: F841 - es justo lo que se quiere detectar
        raise EIAError("mensaje limpio")

    try:
        inner()
    except EIAError as exc:
        return exc
    raise AssertionError("unreachable")


def _leak_via_httpx_object_in_frame() -> BaseException:
    def inner() -> None:
        response = httpx.Response(  # noqa: F841
            503, request=httpx.Request("GET", DATA_URL, params={"api_key": FAKE_KEY})
        )
        raise EIAError("mensaje limpio")

    try:
        inner()
    except EIAError as exc:
        return exc
    raise AssertionError("unreachable")


def _leak_partial_fragment() -> BaseException:
    return _raised(EIAError(f"eco parcial: {FAKE_KEY[5:20]}"))


KNOWN_LEAKS: dict[str, tuple[Callable[[], BaseException], str]] = {
    "mensaje": (_leak_in_message, "str"),
    "solo-en-__context__": (_leak_via_context_only, "__context__"),
    "atributo-con-request": (_leak_via_attribute, "grafo:"),
    "variable-local": (_leak_via_frame_local, "traceback+locals"),
    "objeto-httpx-en-local": (_leak_via_httpx_object_in_frame, "httpx-en-locals:"),
    "fragmento-parcial": (_leak_partial_fragment, "str"),
}


@pytest.mark.parametrize("name", KNOWN_LEAKS)
def test_the_oracles_detect_known_leaks(name: str) -> None:
    """Si un detector no ve una fuga fabricada a proposito, los tests verdes no valen nada."""
    build, expected_surface = KNOWN_LEAKS[name]

    leaks = _leaking_surfaces(build())

    assert any(leak.startswith(expected_surface) for leak in leaks), leaks


def test_the_oracles_report_nothing_for_a_clean_exception() -> None:
    assert _leaking_surfaces(_raised(EIAError("mensaje limpio", attempts=1))) == []


# ====================================================== que los escenarios son hostiles de verdad
@pytest.mark.parametrize("name", SCENARIOS)
def test_every_scenario_really_delivers_the_key_to_the_client(name: str) -> None:
    """Evita que las pruebas de abajo pasen en vacio: el escenario SI contiene la clave."""
    handler, _ = SCENARIOS[name]
    request = httpx.Request("GET", DATA_URL, params={"api_key": FAKE_KEY})

    try:
        response = handler(request)
        delivered = response.text + repr(dict(response.headers))
    except Exception as exc:  # noqa: BLE001
        delivered = str(exc) + str(exc.request.url)  # type: ignore[attr-defined]

    assert _has_key(delivered), f"el escenario {name} no entrega la clave al cliente"


# ================================================================ ninguna superficie la expone
@pytest.mark.parametrize("name", SCENARIOS)
def test_no_observable_surface_exposes_the_key(name: str, route: respx.Route) -> None:
    handler, expected = SCENARIOS[name]
    route.side_effect = handler

    exc = _capture()

    assert type(exc) is expected
    leaks = _leaking_surfaces(exc)
    assert not leaks, f"la clave aparece en: {leaks}"


@pytest.mark.parametrize("name", SCENARIOS)
def test_exceptions_survive_pickle_and_copy_without_the_key(name: str, route: respx.Route) -> None:
    route.side_effect = SCENARIOS[name][0]
    exc = _capture()

    blob = pickle.dumps(exc)

    assert not any(variant.encode() in blob for variant in VARIANTS)
    for clone in (pickle.loads(blob), copy.copy(exc), copy.deepcopy(exc)):
        assert not _has_key(repr(clone) + str(clone))


def test_partial_echo_is_replaced_by_a_marker(route: respx.Route) -> None:
    route.side_effect = SCENARIOS["400-eco-parcial"][0]

    exc = _capture()

    assert "[mensaje omitido por seguridad]" in str(exc)


def test_key_echoed_in_the_error_code_is_discarded(route: respx.Route) -> None:
    route.side_effect = SCENARIOS["400-eco-en-el-codigo"][0]

    exc = _capture()

    assert isinstance(exc, EIABadRequestError)
    assert exc.eia_code is None


# Clave FICTICIA solo con letras y digitos: tiene la forma de un codigo de error valido.
ALPHANUMERIC_KEY = "Ab12Cd34Ef56Gh78Ij90Kl12Mn34Op56Qr78St90"


def test_alphanumeric_key_echoed_as_error_code_is_discarded(route: respx.Route) -> None:
    """Regresion (mutacion N5): el saneado del codigo de error debe actuar por si solo.

    `FAKE_KEY` contiene '/', '+' y '=', asi que el patron de forma de un codigo ya la descarta y
    la llamada a `sanitize_untrusted_text` quedaria sin probar. Una clave alfanumerica SI cumple
    ese patron: solo el saneado impide que llegue a la excepcion.
    """
    assert re.fullmatch(r"[A-Za-z0-9_.\-]{1,64}", ALPHANUMERIC_KEY)  # la premisa de la prueba
    route.mock(return_value=_json(400, {"error": {"code": ALPHANUMERIC_KEY, "message": "mensaje"}}))

    with (
        EIAClient(Secret(ALPHANUMERIC_KEY), sleep=lambda seconds: None) as client,
        pytest.raises(EIABadRequestError) as info,
    ):
        client.get_data(length=1)

    assert info.value.eia_code is None
    assert str(info.value) == "EIA respondio con HTTP 400: mensaje"
    assert ALPHANUMERIC_KEY not in repr(info.value)


def test_url_echoed_by_the_server_is_omitted(route: respx.Route) -> None:
    route.side_effect = SCENARIOS["403-eco-de-la-url-real"][0]

    exc = _capture()

    assert "[mensaje omitido por seguridad]" in str(exc)


def test_warnings_echoing_the_key_are_sanitised_on_success(route: respx.Route) -> None:
    body = {
        "response": {"total": "0", "data": []},
        "warnings": [{"warning": "w", "description": f"clave {FAKE_KEY}"}],
    }
    route.mock(return_value=_json(200, body))

    with EIAClient(Secret(FAKE_KEY)) as client:
        page = client.get_data()

    assert page.warnings == ("[mensaje omitido por seguridad]",)
    assert not _has_key(repr(page))


# ================================================================ estructura de las excepciones
@pytest.mark.parametrize("name", SCENARIOS)
def test_no_httpx_object_is_reachable_from_the_traceback_frames(
    name: str, route: respx.Route
) -> None:
    route.side_effect = SCENARIOS[name][0]

    exc = _capture()

    assert _httpx_objects_in_frames(exc) == []
    assert exc.__context__ is None
    assert exc.__cause__ is None
    assert exc.__suppress_context__ is False  # no se oculta nada con `from None`: no hay nada


# ======================================================================================= logs
class _Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@pytest.mark.parametrize("name", SCENARIOS)
def test_logs_never_contain_the_key_even_with_http_loggers_at_debug(
    name: str, route: respx.Route
) -> None:
    route.side_effect = SCENARIOS[name][0]
    masked = io.StringIO()
    configure_logging("DEBUG", secrets=[FAKE_KEY], stream=masked)
    ours = _Collector()
    logging.getLogger("pipeline_lab.eia.client").addHandler(ours)

    _capture(force_http_debug=True)

    assert not _has_key(masked.getvalue())
    assert not any(_has_key(message) for message in ours.messages)


@pytest.mark.parametrize("name", SCENARIOS)
def test_our_own_log_events_carry_no_url_or_query(name: str, route: respx.Route) -> None:
    route.side_effect = SCENARIOS[name][0]
    ours = _Collector()
    logger = logging.getLogger("pipeline_lab.eia.client")
    logger.addHandler(ours)
    logger.setLevel(logging.DEBUG)

    _capture()

    assert ours.messages, "se esperaban eventos de log"
    forbidden = re.compile(r"https?://|api\.eia\.gov|api_key|facets|offset=|length=|\?")
    assert not [m for m in ours.messages if forbidden.search(m)]
    assert all(m.startswith("eia_request_") for m in ours.messages)


def test_unmasked_http_logging_would_expose_the_key(route: respx.Route) -> None:
    """Demuestra que el enmascarado es lo que protege: sin el, httpx escribe la clave."""
    route.side_effect = SCENARIOS["503-agotado"][0]
    raw = io.StringIO()
    handler = logging.StreamHandler(raw)  # SIN MaskingFormatter
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

    _capture(force_http_debug=True)

    assert _has_key(raw.getvalue()), "httpx deberia haber registrado la URL con la clave"


def test_the_client_silences_http_loggers_by_default(route: respx.Route) -> None:
    route.side_effect = SCENARIOS["503-agotado"][0]
    raw = io.StringIO()
    root = logging.getLogger()
    root.addHandler(logging.StreamHandler(raw))  # SIN MaskingFormatter
    root.setLevel(logging.DEBUG)
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.DEBUG)

    _capture()  # el constructor del cliente sube httpx/httpcore a WARNING

    assert not _has_key(raw.getvalue())


# ========================================================================= el propio cliente
def test_client_representations_do_not_expose_the_key() -> None:
    with EIAClient(Secret(FAKE_KEY)) as client:
        texts = [repr(client), str(client), repr(vars(client)), f"{client!r:>200}"]

    assert not any(_has_key(text) for text in texts)


# ================================================================== limites documentados (ADR)
def test_documented_limit_httpx_hooks_see_the_full_url_including_the_key(
    route: respx.Route,
) -> None:
    """LIMITE: EIA obliga a poner la clave en la URL; cualquier hook o instrumentacion de httpx
    la ve durante la llamada. El cliente no puede evitarlo. Ver el ADR."""
    route.mock(return_value=_json(200, {"response": {"total": "0", "data": []}}))
    seen: list[str] = []

    with EIAClient(Secret(FAKE_KEY)) as client:
        client._http.event_hooks["request"].append(lambda request: seen.append(str(request.url)))
        client.get_data()

    assert any(_has_key(url) for url in seen)


def test_documented_limit_secret_is_reachable_from_the_client_object() -> None:
    """LIMITE: `Secret` previene accidentes, no acceso deliberado. Quien tenga el cliente puede
    leer la clave (por eso el grafo de arriba trata a `Secret` como opaco)."""
    with EIAClient(Secret(FAKE_KEY)) as client:
        assert client._api_key.reveal() == FAKE_KEY
        assert client._api_key._value == FAKE_KEY
