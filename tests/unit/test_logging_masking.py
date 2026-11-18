"""Pruebas de `mask_secrets` y `silence_http_loggers` (anadidos al modulo de logging)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from urllib.parse import quote

import pytest

from pipeline_lab.logging_config import (
    MASK,
    MaskingFormatter,
    mask_secrets,
    silence_http_loggers,
)

KEY = "fake-key/with+chars=DO-NOT-USE-0123456789"


@pytest.fixture(autouse=True)
def _restore_http_loggers() -> Iterator[None]:
    saved = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


@pytest.mark.parametrize(
    ("text", "secrets", "expected"),
    [
        (f"a {KEY} b", [KEY], f"a {MASK} b"),
        (f"a {quote(KEY, safe='')} b", [KEY], f"a {MASK} b"),
        (f"a {quote(KEY)} b", [KEY], f"a {MASK} b"),
        (f"{KEY}{KEY}", [KEY], f"{MASK}{MASK}"),
        ("sin secretos", [KEY], "sin secretos"),
        ("sin secretos", [], "sin secretos"),
        ("texto intacto", ["", KEY], "texto intacto"),  # un secreto vacio no corrompe el texto
        ("", [KEY], ""),
        ("uno dos", ["uno", "dos"], f"{MASK} {MASK}"),
        ("abcdef", ["abc", "abcdef"], MASK),  # gana la variante mas larga
        ("xx abc xx", ["abc", "abcdef"], f"xx {MASK} xx"),
    ],
)
def test_mask_secrets(text: str, secrets: list[str], expected: str) -> None:
    assert mask_secrets(text, secrets) == expected


def test_mask_secrets_accepts_any_iterable() -> None:
    assert mask_secrets("a b", (s for s in ["a"])) == f"{MASK} b"


def test_mask_secrets_matches_the_formatter_behaviour() -> None:
    secrets = ["", KEY, "abc", "abcdef"]
    message = f"x {KEY} y {quote(KEY, safe='')} z abcdef w abc"
    record = logging.LogRecord("n", logging.INFO, __file__, 1, "%s", (message,), None)

    formatted = MaskingFormatter(secrets).format(record)

    assert formatted.endswith(mask_secrets(message, secrets))


def test_silence_http_loggers_raises_levels_to_warning() -> None:
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.DEBUG)

    silence_http_loggers()

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_silence_http_loggers_is_idempotent() -> None:
    silence_http_loggers()
    silence_http_loggers()

    assert logging.getLogger("httpx").level == logging.WARNING
