from datetime import datetime
from typing import Any
from uuid import UUID, uuid7

import pytest
from data.models import WEBHOOK_URL_MAX_LENGTH, Outbox
from data.repositories.payments import PAYMENT_CREATED_EVENT
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PAYMENTS_URL = "/api/v1/payments"
WEBHOOK_URL_PREFIX = "https://merchant.example.com/"


def _url_of_length(length: int) -> str:
    return WEBHOOK_URL_PREFIX + "a" * (length - len(WEBHOOK_URL_PREFIX))


def _body(**overrides: object) -> dict[str, object]:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042},
        "webhook_url": "https://merchant.example.com/webhooks/payments",
        **overrides,
    }


async def _create(client: AsyncClient, key: str = "key-1", **overrides: object) -> Response:
    return await client.post(
        PAYMENTS_URL, json=_body(**overrides), headers={"Idempotency-Key": key}
    )


async def _outbox(sessions: async_sessionmaker[AsyncSession]) -> list[Outbox]:
    async with sessions() as session:
        return list(await session.scalars(select(Outbox)))


async def test_create_payment_returns_accepted_pending_payment(client: AsyncClient) -> None:
    response = await _create(client)

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert UUID(body["payment_id"])
    assert body["status"] == "pending"
    assert datetime.fromisoformat(body["created_at"])


async def test_create_payment_writes_outbox_event(
    client: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = (await _create(client)).json()["payment_id"]

    [event] = await _outbox(sessions)
    assert event.event_type == PAYMENT_CREATED_EVENT
    assert event.payload == {"payment_id": payment_id}


async def test_create_payment_with_same_key_and_body_returns_same_payment(
    client: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    first = await _create(client)
    second = await _create(client)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert len(await _outbox(sessions)) == 1


async def test_create_payment_with_same_key_and_different_body_conflicts(
    client: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _create(client)

    response = await _create(client, amount="99.00")

    assert response.status_code == 409
    assert "key-1" in response.json()["detail"]
    assert len(await _outbox(sessions)) == 1


async def test_create_payment_with_different_keys_creates_separate_payments(
    client: AsyncClient,
) -> None:
    first = await _create(client, key="key-1")
    second = await _create(client, key="key-2")

    assert first.json()["payment_id"] != second.json()["payment_id"]


async def test_create_payment_requires_idempotency_key(client: AsyncClient) -> None:
    response = await client.post(PAYMENTS_URL, json=_body())

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-10.00"},
        {"amount": "10.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"webhook_url": _url_of_length(WEBHOOK_URL_MAX_LENGTH + 1)},
        {"description": None},
    ],
)
async def test_create_payment_rejects_invalid_body(
    client: AsyncClient,
    overrides: dict[str, Any],
) -> None:
    response = await _create(client, **overrides)

    assert response.status_code == 422


async def test_create_payment_accepts_webhook_url_of_max_length(client: AsyncClient) -> None:
    url = _url_of_length(WEBHOOK_URL_MAX_LENGTH)

    created = (await _create(client, webhook_url=url)).json()
    response = await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

    assert response.json()["webhook_url"] == url


async def test_get_payment_returns_details(client: AsyncClient) -> None:
    created = (await _create(client)).json()

    response = await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "https://merchant.example.com/webhooks/payments",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_unknown_payment_returns_not_found(client: AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid7()}")

    assert response.status_code == 404


async def test_get_payment_with_malformed_id_is_rejected(client: AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/not-a-uuid")

    assert response.status_code == 422


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", PAYMENTS_URL), ("GET", f"{PAYMENTS_URL}/{uuid7()}")],
)
async def test_endpoints_require_valid_api_key(
    client: AsyncClient,
    headers: dict[str, str],
    method: str,
    path: str,
) -> None:
    client.headers.pop("X-API-Key")

    response = await client.request(
        method,
        path,
        json=_body(),
        headers={"Idempotency-Key": "key-1", **headers},
    )

    assert response.status_code == 401


async def test_openapi_documents_endpoints_and_api_key(client: AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()

    create = schema["paths"][PAYMENTS_URL]["post"]
    assert {"202", "401", "409", "422"} <= set(create["responses"])
    assert any(parameter["name"] == "Idempotency-Key" for parameter in create["parameters"])
    assert set(schema["paths"][f"{PAYMENTS_URL}/{{payment_id}}"]["get"]["responses"]) >= {
        "200",
        "401",
        "404",
    }
    assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
