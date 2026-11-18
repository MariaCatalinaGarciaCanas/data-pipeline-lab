"""Saneado de texto NO confiable (mensajes que devuelve el servidor de EIA).

Un servidor, o un intermediario, puede devolver en un mensaje de error la URL de la peticion,
y con ella la API key. Este modulo reduce ese riesgo antes de que el texto llegue a una
excepcion. No es una garantia absoluta: ver docs/adr/0001-eia-api-key-handling.md.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pipeline_lab.logging_config import _variants, mask_secrets

MAX_MESSAGE_LENGTH = 200
# Longitud minima del fragmento de la clave que se considera un "eco parcial".
MIN_FRAGMENT_LENGTH = 8
OMITTED = "[mensaje omitido por seguridad]"
_ELLIPSIS = "..."

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f]+")


def _contains_fragment(text: str, secret: str) -> bool:
    """True si `text` contiene un trozo (de al menos 8 caracteres) del secreto.

    Se comprueban todas las ventanas de esa longitud de cada variante (literal y codificada
    en URL). Si el secreto es mas corto que 8 caracteres, se busca completo.
    """
    for variant in _variants(secret):
        size = min(len(variant), MIN_FRAGMENT_LENGTH)
        if size == 0:
            continue
        if any(variant[i : i + size] in text for i in range(len(variant) - size + 1)):
            return True
    return False


def sanitize_untrusted_text(
    text: str, secrets: Iterable[str], *, max_length: int = MAX_MESSAGE_LENGTH
) -> str:
    """Devuelve `text` listo para incluir en una excepcion.

    1. Sustituye los caracteres de control por espacios, colapsa las rachas de espacios en uno
       solo y recorta los bordes.
    2. Si aparece la clave completa o un fragmento de 8 o mas caracteres, descarta TODO el
       texto y devuelve un marcador.
    3. Enmascara cualquier coincidencia exacta restante (defensa en profundidad).
    4. Trunca a `max_length` caracteres.

    Limite conocido: no detecta la clave transformada (base64, invertida, partida...).
    """
    secrets = [s for s in secrets if s]
    cleaned = " ".join(_CONTROL_CHARS.sub(" ", text).split())
    if any(_contains_fragment(cleaned, secret) for secret in secrets):
        return OMITTED
    cleaned = mask_secrets(cleaned, secrets)
    if len(cleaned) > max_length:
        return cleaned[: max_length - len(_ELLIPSIS)] + _ELLIPSIS
    return cleaned
