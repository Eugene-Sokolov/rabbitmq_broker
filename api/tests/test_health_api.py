from collections.abc import AsyncIterator

import pytest
from api.main import create_app
from api.settings import Settings
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def anonymous_client(client: AsyncClient) -> AsyncClient:
    client.headers.pop("X-API-Key")
    return client


@pytest.fixture
async def client_without_database() -> AsyncIterator[AsyncClient]:
    app = create_app(
        Settings(
            database_url="postgresql+asyncpg://user:password@127.0.0.1:1/missing",
            api_key="unused",
        )
    )
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


async def test_health_reports_ok_without_api_key(anonymous_client: AsyncClient) -> None:
    response = await anonymous_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


async def test_health_reports_unavailable_when_database_is_down(
    client_without_database: AsyncClient,
) -> None:
    response = await client_without_database.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "unavailable"}
