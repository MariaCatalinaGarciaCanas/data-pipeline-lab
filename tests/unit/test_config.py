"""Pruebas de configuracion y de que la API key no se filtre."""

from __future__ import annotations

import copy
import os
import pickle
from pathlib import Path

import pytest

from pipeline_lab.config import (
    API_KEY_VAR,
    ConfigError,
    Secret,
    Settings,
    load_settings,
)

FAKE_KEY = "fake-key-DO-NOT-USE-0123456789abcdef"
REPO_ROOT = Path(__file__).resolve().parents[2]


# --- Lectura de configuracion -------------------------------------------------------------


def test_loads_key_from_env_mapping() -> None:
    settings = load_settings(env={API_KEY_VAR: FAKE_KEY}, dotenv_path=None)

    assert settings.api_key.reveal() == FAKE_KEY


def test_defaults_when_only_key_is_given() -> None:
    settings = load_settings(env={API_KEY_VAR: FAKE_KEY}, dotenv_path=None)

    assert settings.data_dir == Path("data")
    assert settings.log_level == "INFO"


def test_loads_values_from_dotenv_file(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"EIA_API_KEY={FAKE_KEY}\nDATA_DIR=mis_datos\nLOG_LEVEL=debug\n")

    settings = load_settings(env={}, dotenv_path=dotenv)

    assert settings.api_key.reveal() == FAKE_KEY
    assert settings.data_dir == Path("mis_datos")
    assert settings.log_level == "DEBUG"


def test_dotenv_supports_quoted_values(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(f'EIA_API_KEY="{FAKE_KEY}"\n')

    settings = load_settings(env={}, dotenv_path=dotenv)

    assert settings.api_key.reveal() == FAKE_KEY


def test_real_environment_overrides_dotenv(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("EIA_API_KEY=from-file\nLOG_LEVEL=DEBUG\n")

    settings = load_settings(env={API_KEY_VAR: "from-env"}, dotenv_path=dotenv)

    assert settings.api_key.reveal() == "from-env"
    assert settings.log_level == "DEBUG"  # lo que no esta en el entorno viene del archivo


def test_missing_dotenv_file_is_not_an_error(tmp_path: Path) -> None:
    settings = load_settings(env={API_KEY_VAR: FAKE_KEY}, dotenv_path=tmp_path / "no_existe.env")

    assert settings.api_key.reveal() == FAKE_KEY


def test_loading_does_not_modify_os_environ(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Partimos de un entorno SIN la variable, pase lo que pase en otros tests o en tu equipo.
    # Asi, si load_settings cargara el .env en os.environ, la variable apareceria y lo veriamos.
    monkeypatch.delenv(API_KEY_VAR, raising=False)
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"EIA_API_KEY={FAKE_KEY}\n")
    before = dict(os.environ)

    load_settings(env={}, dotenv_path=dotenv)

    assert API_KEY_VAR not in os.environ
    assert dict(os.environ) == before


def test_uses_os_environ_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(API_KEY_VAR, FAKE_KEY)

    settings = load_settings(dotenv_path=None)  # sin env explicito: usa os.environ

    assert settings.api_key.reveal() == FAKE_KEY


def test_dotenv_values_are_not_interpolated(tmp_path: Path) -> None:
    # Con interpolacion, "${OTRA}" se sustituiria por "zzz" y la clave quedaria alterada.
    dotenv = tmp_path / ".env"
    dotenv.write_text("OTRA=zzz\nEIA_API_KEY=a${OTRA}b${NO_DEFINIDA}c\n")

    settings = load_settings(env={}, dotenv_path=dotenv)

    assert settings.api_key.reveal() == "a${OTRA}b${NO_DEFINIDA}c"


# --- Errores de configuracion -------------------------------------------------------------


def test_missing_key_raises_clear_error() -> None:
    with pytest.raises(ConfigError, match=API_KEY_VAR):
        load_settings(env={}, dotenv_path=None)


@pytest.mark.parametrize("empty", ["", "   ", "\t"])
def test_empty_key_is_rejected(empty: str) -> None:
    with pytest.raises(ConfigError, match=API_KEY_VAR):
        load_settings(env={API_KEY_VAR: empty}, dotenv_path=None)


def test_empty_key_in_dotenv_is_rejected(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("EIA_API_KEY=\n")  # como en .env.example

    with pytest.raises(ConfigError, match=API_KEY_VAR):
        load_settings(env={}, dotenv_path=dotenv)


def test_invalid_log_level_is_rejected_without_leaking_key() -> None:
    with pytest.raises(ConfigError) as excinfo:
        load_settings(env={API_KEY_VAR: FAKE_KEY, "LOG_LEVEL": "VERBOSE"}, dotenv_path=None)

    assert "LOG_LEVEL" in str(excinfo.value)
    assert FAKE_KEY not in str(excinfo.value)


# --- La API key no debe filtrarse ---------------------------------------------------------


def test_secret_is_masked_in_every_text_representation() -> None:
    secret = Secret(FAKE_KEY)

    assert FAKE_KEY not in repr(secret)
    assert FAKE_KEY not in str(secret)
    assert FAKE_KEY not in f"{secret}"
    assert FAKE_KEY not in f"{secret!r}"
    assert FAKE_KEY not in "%s %r" % (secret, secret)  # noqa: UP031 (probamos el estilo antiguo)
    assert FAKE_KEY not in "{:>40}".format(secret)  # noqa: UP032 (probamos str.format)


def test_settings_repr_and_str_do_not_contain_key() -> None:
    settings = load_settings(env={API_KEY_VAR: FAKE_KEY}, dotenv_path=None)

    assert FAKE_KEY not in repr(settings)
    assert FAKE_KEY not in str(settings)
    assert "***" in repr(settings)


def test_secret_cannot_be_pickled_or_copied_into_text() -> None:
    secret = Secret(FAKE_KEY)

    with pytest.raises(TypeError):
        pickle.dumps(secret)
    with pytest.raises(TypeError):
        copy.deepcopy(secret)


def test_settings_is_immutable() -> None:
    settings = load_settings(env={API_KEY_VAR: FAKE_KEY}, dotenv_path=None)

    with pytest.raises(AttributeError):
        settings.log_level = "DEBUG"  # type: ignore[misc]
    assert isinstance(settings, Settings)


# --- Archivos del repositorio -------------------------------------------------------------


def test_env_example_contains_no_real_value() -> None:
    lines = (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    assignments = {
        line.split("=", 1)[0].strip(): line.split("=", 1)[1].strip()
        for line in lines
        if "=" in line and not line.lstrip().startswith("#")
    }

    assert assignments.get(API_KEY_VAR) == ""  # la plantilla no trae ninguna clave


def test_dotenv_is_listed_in_gitignore() -> None:
    patterns = {
        line.strip() for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    }

    assert ".env" in patterns
