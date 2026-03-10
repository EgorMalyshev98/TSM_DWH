FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app
COPY pyproject.toml uv.lock ./
COPY dv_gen ./dv_gen
RUN uv sync --frozen --no-dev

FROM python:3.11-slim

WORKDIR /usr/app
COPY --from=builder /usr/app/.venv ./.venv

COPY dbt_project.yml ./
COPY tsm_dwh ./tsm_dwh
COPY profiles.yml ./

ENV PATH="/usr/app/.venv/bin:$PATH"
