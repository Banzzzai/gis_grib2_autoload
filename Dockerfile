FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml ./

RUN uv sync --no-dev --no-install-project

COPY app ./app

RUN uv sync --no-dev


FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /tmp/ecmwf-downloads \
    && chown -R appuser:appuser /app /tmp/ecmwf-downloads

COPY --from=builder --chown=appuser:appuser /app /app

USER appuser

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
