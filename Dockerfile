# syntax=docker/dockerfile:1
#
# Imagen de data-pipeline-lab: ejecuta el pipeline (`python -m pipeline_lab`) y dbt.
#
#   docker build -t data-pipeline-lab .
#   docker run --rm -v "$PWD/data:/data" data-pipeline-lab run --synthetic
#
# La API key NO esta en la imagen: se pasa al ejecutar (`-e EIA_API_KEY` o `--env-file .env`).

FROM python:3.12-slim-bookworm

# uv, copiado desde su imagen oficial con la version fijada.
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DATA_DIR=/data \
    DBT_DUCKDB_PATH=/data/warehouse/analytics.duckdb

# Usuario sin privilegios: el pipeline no necesita ser root.
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app /data \
    && chown app:app /app /data

WORKDIR /app

# 1) Dependencias (capa cacheable: solo se reconstruye si cambian pyproject.toml o uv.lock).
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

# 2) Codigo del proyecto, dbt y consultas de ejemplo.
# --chown: dbt (que corre como `app`) necesita poder crear dbt/target y dbt/logs.
COPY --chown=app:app src/ ./src/
COPY --chown=app:app dbt/ ./dbt/
COPY --chown=app:app sql/ ./sql/
RUN uv sync --locked --no-dev

USER app

# Los datos generados viven en un volumen: se conservan al borrar el contenedor.
VOLUME ["/data"]

ENTRYPOINT ["python", "-m", "pipeline_lab"]
CMD ["--help"]
