FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY . .

# ВАЖНО: убрали --frozen, чтобы uv создал свежий lock-файл без common-stribog3
RUN uv sync --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "main.py"]