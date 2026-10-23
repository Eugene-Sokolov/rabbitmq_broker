FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
COPY relay/pyproject.toml relay/
COPY data/pyproject.toml data/
RUN uv sync --frozen --no-dev --no-install-workspace --package relay

COPY data/src data/src
COPY relay/src relay/src
RUN uv sync --frozen --no-dev --package relay

RUN useradd --system --no-create-home app
USER app

WORKDIR /app/relay

CMD ["relay"]
