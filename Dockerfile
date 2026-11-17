
# Stage 1: Build dependencies
FROM ghcr.io/astral-sh/uv:0.13.8 AS uv

FROM python:3.14-slim AS builder

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY --from=uv /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN uv sync --frozen --no-dev


# Stage 2: Runtime
FROM python:3.14-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ENV=production

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini pyproject.toml ./

RUN groupadd --system --gid 10001 butler \
    && useradd --system --uid 10001 --gid butler \
       --no-create-home butler

USER 10001:10001

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
