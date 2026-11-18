"""Linea de comandos: `uv run python -m pipeline_lab <comando>`.

    ingest     descarga (o genera) datos y los guarda como Parquet Raw
    transform  limpia con Polars: Raw -> Clean
    load       carga Clean en DuckDB
    run        ingest + transform + load

Nada descarga datos reales de EIA por accidente: `ingest` y `run` exigen elegir la fuente con
`--synthetic` (datos inventados, sin red ni API key) o `--live` (EIA real, necesita EIA_API_KEY).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from pipeline_lab import pipeline
from pipeline_lab.config import ConfigError, Settings, load_settings
from pipeline_lab.eia import EIAClient, EIAError
from pipeline_lab.eia.extractor import DEFAULT_TYPES, DataSource, ExtractionRequest
from pipeline_lab.layout import raw_dir
from pipeline_lab.logging_config import configure_logging
from pipeline_lab.storage.raw import RawStorageError
from pipeline_lab.synthetic import SyntheticSource
from pipeline_lab.transform.clean import SchemaError

# Ventana por defecto: la que se definio para el primer desarrollo (4 al 10 de noviembre de 2026).
DEFAULT_START = date(2026, 11, 4)
DEFAULT_END = date(2026, 11, 10)

logger = logging.getLogger(__name__)


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"fecha invalida {text!r}; usa AAAA-MM-DD") from exc


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="carpeta de datos (por defecto DATA_DIR o ./data)",
    )


def _ingest_options(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--synthetic", action="store_true", help="datos inventados (sin red ni API key)"
    )
    source.add_argument(
        "--live", action="store_true", help="datos reales de EIA (necesita EIA_API_KEY)"
    )
    parser.add_argument(
        "--start", type=_iso_date, default=DEFAULT_START, help="primer dia UTC, AAAA-MM-DD"
    )
    parser.add_argument(
        "--end", type=_iso_date, default=DEFAULT_END, help="ultimo dia UTC (incluido)"
    )
    parser.add_argument(
        "--types",
        nargs="+",
        default=list(DEFAULT_TYPES),
        metavar="TIPO",
        help="tipos de observacion: D DF NG TI",
    )
    parser.add_argument(
        "--respondents",
        nargs="*",
        default=[],
        metavar="REGION",
        help="limitar a estas regiones (por defecto, todas)",
    )
    parser.add_argument(
        "--page-size", type=int, default=5000, help="filas por pagina (maximo 5000)"
    )
    parser.add_argument(
        "--anomalies",
        action="store_true",
        help="con --synthetic: incluir filas problematicas para ejercitar la limpieza",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline_lab", description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("ingest", "descargar o generar datos -> Parquet Raw"),
        ("transform", "Raw -> Clean (Polars)"),
        ("load", "Clean -> DuckDB"),
        ("run", "ingest + transform + load"),
    ):
        sub = commands.add_parser(name, help=help_text)
        _common(sub)
        if name in ("ingest", "run"):
            _ingest_options(sub)
    return parser


def _data_dir(args: argparse.Namespace, settings: Settings) -> Path:
    return args.output_dir if args.output_dir is not None else settings.data_dir


def _build_source(
    args: argparse.Namespace, settings: Settings
) -> tuple[DataSource, EIAClient | None]:
    if args.live:
        client = EIAClient.from_settings(settings)
        return client, client
    return SyntheticSource(anomalies=args.anomalies), None


SYNTHETIC_MARKER = ".synthetic"


def _guard_data_dir(data_dir: Path, *, synthetic: bool) -> None:
    """Impide mezclar datos sinteticos con datos reales en la misma carpeta de datos.

    * `--synthetic` marca la carpeta con `.synthetic`. Se niega a escribir donde ya hay datos Raw
      SIN esa marca (podria reemplazar particiones reales de las mismas fechas).
    * `--live` se niega a escribir en una carpeta marcada como sintetica.
    """
    marker = data_dir / SYNTHETIC_MARKER
    has_raw = (
        any(raw_dir(data_dir).glob("date=*/*.parquet")) if raw_dir(data_dir).is_dir() else False
    )
    if synthetic and has_raw and not marker.exists():
        raise RawStorageError(
            f"{raw_dir(data_dir)} ya contiene datos que no son sinteticos. Usa otra carpeta, por "
            "ejemplo --output-dir data/synthetic, para no sobrescribirlos."
        )
    if not synthetic and marker.exists():
        raise RawStorageError(
            f"{data_dir} contiene datos SINTETICOS (marca {SYNTHETIC_MARKER}). Usa otra carpeta "
            "para los datos reales o borra esta."
        )
    if synthetic:
        data_dir.mkdir(parents=True, exist_ok=True)
        marker.write_text(
            "Datos inventados: no mezclar con datos reales de EIA.\n", encoding="utf-8"
        )


def _ingest(args: argparse.Namespace, settings: Settings, data_dir: Path) -> None:
    if not (args.synthetic or args.live):
        raise SystemExit(
            "Elige la fuente: --synthetic (datos inventados) o --live (EIA real, usa tu API key)."
        )
    _guard_data_dir(data_dir, synthetic=bool(args.synthetic))
    request = ExtractionRequest(
        start=args.start,
        end=args.end,
        types=tuple(args.types),
        respondents=tuple(args.respondents),
        page_size=args.page_size,
    )
    source, client = _build_source(args, settings)
    try:
        summary = pipeline.ingest(source, request, data_dir)
    finally:
        if client is not None:
            client.close()
    print(
        f"[ingest] {summary.rows} filas en {summary.pages} paginas "
        f"(total anunciado: {summary.total_announced}, "
        f"duplicados descartados: {summary.duplicates_dropped})"
    )
    print(
        f"[ingest] particiones escritas: {len(summary.write.partitions)} "
        f"({len(summary.write.replaced)} reemplazadas) en {data_dir / 'raw'}"
    )


def _transform(data_dir: Path) -> None:
    summary = pipeline.transform(data_dir)
    stats = summary.stats
    print(
        f"[transform] raw={stats.raw_rows} limpias={stats.clean_rows} "
        f"rechazadas={stats.rejected_rows}"
    )
    for reason, count in stats.rejected_by_reason.items():
        print(f"[transform]   rechazadas por {reason}: {count}")
    print(f"[transform] escrito: {summary.observations_path}")


def _load(data_dir: Path) -> None:
    summary = pipeline.load(data_dir)
    print(
        f"[load] {summary.rows_in_file} filas leidas: {summary.inserted} nuevas, "
        f"{summary.replaced} reemplazadas; hechos totales: {summary.fact_rows_after} "
        f"({summary.respondents} regiones, {summary.types} tipos)"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    needs_key = bool(getattr(args, "live", False))

    try:
        settings = load_settings(require_api_key=needs_key)
    except ConfigError as exc:
        print(f"Error de configuracion: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level, secrets=[settings.api_key.reveal()])
    data_dir = _data_dir(args, settings)

    try:
        if args.command in ("ingest", "run"):
            _ingest(args, settings, data_dir)
        if args.command in ("transform", "run"):
            _transform(data_dir)
        if args.command in ("load", "run"):
            _load(data_dir)
    except (EIAError, SchemaError, RawStorageError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:  # argumentos invalidos (p. ej. fechas al reves)
        print(f"Argumentos invalidos: {exc}", file=sys.stderr)
        return 2
    return 0
