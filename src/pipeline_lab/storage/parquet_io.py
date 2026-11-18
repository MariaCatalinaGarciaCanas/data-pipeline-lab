"""Escritura de Parquet que nunca deja un archivo a medias."""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path

import polars as pl


def atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Escribe `df` en `path` de forma atomica.

    Se escribe primero a un archivo temporal EN LA MISMA CARPETA (con extension distinta de
    `.parquet`, para que ningun lector lo confunda con un dato) y despues se renombra con
    `os.replace`, que es atomico: quien lea `path` ve el archivo anterior completo o el nuevo
    completo, nunca uno parcial. Si algo falla, el temporal se borra y `path` queda intacto.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        df.write_parquet(tmp)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
        raise
