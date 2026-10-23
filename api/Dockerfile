FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
COPY api/pyproject.toml api/
COPY data/pyproject.toml data/
RUN uv sync --frozen --no-dev --no-install-workspace --package api

COPY data/src data/src
COPY api/src api/src
RUN uv sync --frozen --no-dev --package api

RUN useradd --system --no-create-home app
USER app

WORKDIR /app/api

CMD ["uvicorn", "api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
