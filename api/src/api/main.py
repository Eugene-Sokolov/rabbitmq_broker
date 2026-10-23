from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.errors import register_exception_handlers
from api.routers import health, payments
from api.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = create_async_engine(str(settings.database_url))

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        yield
        await engine.dispose()

    app = FastAPI(
        title="Payments API",
        version="0.1.0",
        description="Accepts payments for asynchronous processing and reports their status.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
    app.include_router(payments.router)
    app.include_router(health.router)
    register_exception_handlers(app)
    return app
