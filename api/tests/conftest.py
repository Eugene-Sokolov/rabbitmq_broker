from collections.abc import AsyncIterator, Iterator

import pytest
from api.main import create_app
from api.settings import Settings
from data.models import Base, Outbox, Payment
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.postgres import PostgresContainer


API_KEY = "test-api-key"


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:18-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture(scope="session")
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def sessions(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    yield sessions
    async with sessions.begin() as session:
        await session.execute(delete(Outbox))
        await session.execute(delete(Payment))


@pytest.fixture
async def app(
    postgres_url: str,
    sessions: async_sessionmaker[AsyncSession],  # noqa: ARG001
) -> AsyncIterator[FastAPI]:
    app = create_app(Settings(database_url=postgres_url, api_key=API_KEY))
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client
