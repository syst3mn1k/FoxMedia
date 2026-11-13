from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.config import settings
from app.db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )

    app.state.redis = client

    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()


app = FastAPI(
    title="FoxMedia Butler",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    dependencies: dict[str, str] = {}

    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

        dependencies["postgresql"] = "ok"
    except Exception:
        dependencies["postgresql"] = "unavailable"

    try:
        await request.app.state.redis.ping()
        dependencies["redis"] = "ok"
    except Exception:
        dependencies["redis"] = "unavailable"

    ready = all(status == "ok" for status in dependencies.values())

    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "not_ready",
            "dependencies": dependencies,
        },
    )
