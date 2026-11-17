"""Configuracion del proyecto, leida desde variables de entorno y/o un archivo .env.

Principios de seguridad:
- La API key se guarda en un objeto `Secret` cuya representacion (`repr`, `str`, f-strings)
  es siempre "***". Para usar el valor real hay que llamar a `reveal()` de forma explicita.
- Los mensajes de error nunca incluyen el valor de la key.
- Cargar la configuracion NO modifica `os.environ`.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

API_KEY_VAR = "EIA_API_KEY"
DATA_DIR_VAR = "DATA_DIR"
LOG_LEVEL_VAR = "LOG_LEVEL"

DEFAULT_DOTENV_PATH = Path(".env")
DEFAULT_DATA_DIR = Path("data")
DEFAULT_LOG_LEVEL = "INFO"

VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class ConfigError(Exception):
    """La configuracion es invalida o esta incompleta. El mensaje nunca contiene secretos."""


class Secret:
    """Envuelve un valor sensible para que no se imprima por accidente.

    Alcance de la proteccion: evita EXPOSICIONES ACCIDENTALES (un `print(settings)`, un
    `logger.info(settings)`, un f-string, una serializacion a JSON o pickle). NO es una
    barrera frente a acceso deliberado: cualquier codigo del mismo proceso puede leer el
    valor con `reveal()` o accediendo directamente al atributo `_value`. Python no tiene
    atributos realmente privados.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        """Devuelve el valor real. Usar solo en el punto exacto donde se necesita."""
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    def __str__(self) -> str:
        return "***"

    def __format__(self, format_spec: str) -> str:
        return "***"

    def __reduce__(self) -> tuple[type, tuple[str]]:
        # Evita que el secreto acabe serializado (pickle) sin querer.
        raise TypeError("Secret no se puede serializar")


@dataclass(frozen=True, slots=True)
class Settings:
    """Configuracion validada del pipeline."""

    api_key: Secret
    data_dir: Path = DEFAULT_DATA_DIR
    log_level: str = DEFAULT_LOG_LEVEL


def load_settings(
    *,
    env: Mapping[str, str] | None = None,
    dotenv_path: Path | None = DEFAULT_DOTENV_PATH,
) -> Settings:
    """Construye `Settings` desde el entorno y, opcionalmente, un archivo .env.

    Precedencia: variables de entorno reales > archivo .env > valores por defecto.

    Args:
        env: mapa de variables de entorno. Si es None se usa `os.environ`.
        dotenv_path: ruta del .env. Si es None no se lee ningun archivo.

    Raises:
        ConfigError: si falta la API key o algun valor es invalido.
    """
    environment = os.environ if env is None else env

    # dotenv_values devuelve un diccionario SIN tocar os.environ.
    # interpolate=False: los valores se leen tal cual. Si no, un valor como "a${OTRA}b" se
    # expandiria y alteraria en silencio una clave que contuviera esos caracteres.
    file_values: dict[str, str | None] = {}
    if dotenv_path is not None and Path(dotenv_path).is_file():
        file_values = dict(dotenv_values(dotenv_path, interpolate=False))

    def lookup(name: str) -> str | None:
        value = environment.get(name)
        if value is None or value.strip() == "":
            value = file_values.get(name)
        if value is None:
            return None
        value = value.strip()
        return value or None

    api_key = lookup(API_KEY_VAR)
    if api_key is None:
        raise ConfigError(
            f"Falta la variable {API_KEY_VAR}. Definela en el entorno o en tu archivo .env "
            "(usa .env.example como plantilla)."
        )

    log_level = (lookup(LOG_LEVEL_VAR) or DEFAULT_LOG_LEVEL).upper()
    if log_level not in VALID_LOG_LEVELS:
        raise ConfigError(
            f"{LOG_LEVEL_VAR} invalido. Valores permitidos: {', '.join(sorted(VALID_LOG_LEVELS))}."
        )

    data_dir = Path(lookup(DATA_DIR_VAR) or DEFAULT_DATA_DIR)

    logging.getLogger(__name__).debug("Configuracion cargada (log_level=%s)", log_level)
    return Settings(api_key=Secret(api_key), data_dir=data_dir, log_level=log_level)
