"""Pruebas del saneado de texto no confiable (mensajes devueltos por el servidor)."""

from __future__ import annotations

from urllib.parse import quote

import pytest

from pipeline_lab.eia._sanitize import (
    MAX_MESSAGE_LENGTH,
    MIN_FRAGMENT_LENGTH,
    OMITTED,
    sanitize_untrusted_text,
)

KEY = "fake-key/with+chars=DO-NOT-USE-0123456789"


def test_plain_text_is_kept() -> None:
    assert (
        sanitize_untrusted_text("Invalid frequency 'daily'", [KEY]) == "Invalid frequency 'daily'"
    )


def test_control_characters_become_spaces_and_edges_are_trimmed() -> None:
    assert sanitize_untrusted_text("\n a\r\nb\x00c\x7f d \t", [KEY]) == "a b c d"


def test_long_text_is_truncated_with_ellipsis() -> None:
    result = sanitize_untrusted_text("x" * 1000, [KEY])

    assert len(result) == MAX_MESSAGE_LENGTH
    assert result.endswith("...")
    assert result == "x" * (MAX_MESSAGE_LENGTH - 3) + "..."


def test_text_at_the_limit_is_not_truncated() -> None:
    text = "y" * MAX_MESSAGE_LENGTH

    assert sanitize_untrusted_text(text, [KEY]) == text


def test_custom_max_length() -> None:
    assert sanitize_untrusted_text("abcdefghij", [KEY], max_length=6) == "abc..."


@pytest.mark.parametrize(
    "variant",
    [KEY, quote(KEY, safe=""), quote(KEY)],
    ids=["literal", "url-encoded-strict", "url-encoded"],
)
def test_text_containing_the_key_is_omitted_entirely(variant: str) -> None:
    result = sanitize_untrusted_text(f"rechazada: {variant} por favor", [KEY])

    assert result == OMITTED
    assert KEY not in result


def test_partial_echo_of_the_key_is_omitted() -> None:
    fragment = KEY[5 : 5 + MIN_FRAGMENT_LENGTH]

    assert sanitize_untrusted_text(f"algo {fragment} algo", [KEY]) == OMITTED


def test_fragment_shorter_than_the_threshold_is_not_treated_as_an_echo() -> None:
    fragment = KEY[5 : 5 + MIN_FRAGMENT_LENGTH - 1]

    assert sanitize_untrusted_text(f"algo {fragment} algo", [KEY]) == f"algo {fragment} algo"


def test_short_secret_is_searched_whole() -> None:
    assert sanitize_untrusted_text("clave abc123 aqui", ["abc123"]) == OMITTED
    assert sanitize_untrusted_text("clave abc12 aqui", ["abc123"]) == "clave abc12 aqui"


def test_empty_secrets_are_ignored() -> None:
    assert sanitize_untrusted_text("mensaje normal", ["", KEY]) == "mensaje normal"
    assert sanitize_untrusted_text("mensaje normal", []) == "mensaje normal"


def test_key_split_by_control_characters_is_still_detected_after_cleaning() -> None:
    # Los caracteres de control se reemplazan ANTES de buscar la clave.
    text = f"{KEY[:10]}\x00{KEY[10:]}"

    assert sanitize_untrusted_text(text, [KEY]) == OMITTED  # los trozos de >= 8 caracteres delatan


def test_documented_limit_a_transformed_key_is_not_detected() -> None:
    # LIMITE CONOCIDO (ver el ADR): solo se detectan coincidencias exactas y fragmentos literales.
    reversed_key = KEY[::-1]
    # El texto invertido no contiene ningun fragmento literal de la clave...
    assert not any(KEY[i : i + 8] in reversed_key for i in range(len(KEY) - 7))

    assert sanitize_untrusted_text(f"eco: {reversed_key}", [KEY]) == f"eco: {reversed_key}"
