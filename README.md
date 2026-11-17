# data-pipeline-lab

Plataforma de ingeniería de datos *local-first*, inspirada en entornos de producción,
que ingesta datos energéticos de la API de EIA (U.S. Energy Information Administration),
los transforma y los modela para análisis.

> **Estado:** en desarrollo (Fase 0: configuración del proyecto).

## Objetivos

- Construir pipelines de datos fiables e idempotentes.
- Aplicar calidad de datos y pruebas automatizadas.
- Escribir código Python mantenible, fuera de notebooks.
- Documentar las decisiones de arquitectura.

## Stack tecnológico

| Herramienta | Uso |
|---|---|
| Python 3.12 | Lenguaje principal |
| uv | Gestión de dependencias y entornos |
| Polars | Transformación de datos |
| DuckDB | Almacén analítico local |
| dbt Core | Modelado con SQL y tests de calidad |
| pytest | Pruebas automatizadas |
| Docker | Contenedorización |
| GitHub Actions | Integración continua |

## Arquitectura

Flujo previsto:

    EIA API → Ingesta (Python) → Raw (Parquet) → Transformación (Polars) → DuckDB → dbt

Se documentará en detalle en `docs/` conforme avancen las fases.

## Puesta en marcha

Requisitos: [uv](https://docs.astral.sh/uv/) y Git.

    git clone https://github.com/MariaCatalinaGarciaCanas/data-pipeline-lab.git
    cd data-pipeline-lab
    uv sync

## Hoja de ruta

- [x] Fase 0: configuración del proyecto
- [ ] Fase 1: ingesta desde la API de EIA
- [ ] Fase 2: transformación y carga en DuckDB
- [ ] Fase 3: pruebas con pytest
- [ ] Fase 4: modelos y tests con dbt
- [ ] Fase 5: CI con GitHub Actions
- [ ] Fase 6: Docker
- [ ] Fase 7: documentación y ADRs
