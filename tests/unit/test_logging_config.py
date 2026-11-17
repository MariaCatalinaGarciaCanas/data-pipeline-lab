"""Pruebas de que el logging enmascara secretos, incluso en excepciones y URLs."""

from __future__ import annotations

import io
import logging
from collections.abc import Iterator
from urllib.parse import quote

import pytest

from pipeline_lab.logging_config import MASK, MaskingFormatter, configure_logging

FAKE_KEY = "fake-key/with+chars=DO-NOT-USE-0123456789"


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """Deja el logging como estaba al terminar cada prueba."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved_levels = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    root.handlers = saved_handlers
    root.setLevel(saved_level)
    for name, level in saved_levels.items():
        logging.getLogger(name).setLevel(level)


def _logger_with_output(
    secrets: list[str], level: str = "DEBUG"
) -> tuple[logging.Logger, io.StringIO]:
    stream = io.StringIO()
    configure_logging(level, secrets=secrets, stream=stream)
    return logging.getLogger("pipeline_lab.test"), stream


def test_masks_secret_in_message() -> None:
    logger, stream = _logger_with_output([FAKE_KEY])

    logger.info("clave usada: %s", FAKE_KEY)

    output = stream.getvalue()
    assert FAKE_KEY not in output
    assert MASK in output


def test_masks_secret_inside_a_url() -> None:
    logger, stream = _logger_with_output([FAKE_KEY])

    logger.error("Fallo GET https://api.eia.gov/v2/x?api_key=%s&length=5", FAKE_KEY)

    assert FAKE_KEY not in stream.getvalue()
    assert "length=5" in stream.getvalue()  # el resto del mensaje se conserva


def test_masks_url_encoded_secret() -> None:
    logger, stream = _logger_with_output([FAKE_KEY])
    encoded = quote(FAKE_KEY, safe="")
    assert encoded != FAKE_KEY  # la clave de prueba tiene caracteres que se codifican

    logger.error("GET https://api.eia.gov/v2/x?api_key=%s", encoded)

    assert encoded not in stream.getvalue()
    assert FAKE_KEY not in stream.getvalue()


def test_masks_secret_in_exception_traceback() -> None:
    logger, stream = _logger_with_output([FAKE_KEY])

    try:
        raise RuntimeError(f"error llamando a https://api.eia.gov/v2/x?api_key={FAKE_KEY}")
    except RuntimeError:
        logger.exception("la peticion fallo")

    output = stream.getvalue()
    assert "RuntimeError" in output  # el traceback sigue siendo util
    assert FAKE_KEY not in output


def test_empty_secret_does_not_corrupt_messages() -> None:
    logger, stream = _logger_with_output(["", FAKE_KEY])

    logger.info("mensaje normal")

    assert "mensaje normal" in stream.getvalue()


def test_formatter_without_secrets_leaves_text_untouched() -> None:
    record = logging.LogRecord("n", logging.INFO, __file__, 1, "hola %s", ("mundo",), None)

    assert "hola mundo" in MaskingFormatter().format(record)


def test_httpx_request_logs_are_not_emitted_at_info() -> None:
    _, stream = _logger_with_output([FAKE_KEY], level="DEBUG")

    logging.getLogger("httpx").info(
        "HTTP Request: GET https://api.eia.gov/v2/x?api_key=%s", FAKE_KEY
    )

    assert stream.getvalue() == ""  # ni siquiera se genera, ademas de no filtrar la clave


def test_configure_logging_is_idempotent() -> None:
    root = logging.getLogger()
    before = len(root.handlers)

    configure_logging("INFO", secrets=[FAKE_KEY], stream=io.StringIO())
    configure_logging("INFO", secrets=[FAKE_KEY], stream=io.StringIO())

    assert len(root.handlers) == before + 1


def test_respects_log_level() -> None:
    logger, stream = _logger_with_output([FAKE_KEY], level="WARNING")

    logger.info("no deberia verse")
    logger.warning("si deberia verse")

    assert "no deberia verse" not in stream.getvalue()
    assert "si deberia verse" in stream.getvalue()
