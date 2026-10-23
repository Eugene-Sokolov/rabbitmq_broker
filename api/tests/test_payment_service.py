from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from api.services.payments import (
    IdempotencyKeyConflictError,
    PaymentNotFoundError,
    PaymentService,
)
from data.enums import Currency, PaymentStatus
from data.repositories.payments import PaymentCreate, PaymentRead, PaymentRepository
from sqlalchemy.ext.asyncio import AsyncSession


def _request(**overrides: object) -> PaymentCreate:
    fields = {
        "amount": Decimal("10.50"),
        "currency": Currency.USD,
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "https://example.com/webhook",
        "idempotency_key": "key-1",
    }
    return PaymentCreate(**{**fields, **overrides})


def _stored(request: PaymentCreate) -> PaymentRead:
    return PaymentRead(
        id=uuid7(),
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        meta=request.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=request.idempotency_key,
        webhook_url=str(request.webhook_url),
        created_at=datetime.now(UTC),
        processed_at=None,
    )


@pytest.fixture
def session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def repository() -> AsyncMock:
    return AsyncMock(spec=PaymentRepository)


@pytest.fixture
def service(session: AsyncMock, repository: AsyncMock) -> PaymentService:
    return PaymentService(session, repository)


async def test_create_payment_commits_and_returns_payment(
    service: PaymentService,
    session: AsyncMock,
    repository: AsyncMock,
) -> None:
    request = _request()
    stored = _stored(request)
    repository.create.return_value = stored

    payment = await service.create_payment(request)

    assert payment == stored
    repository.create.assert_awaited_once_with(request)
    session.commit.assert_awaited_once()


async def test_create_payment_treats_equal_amounts_with_different_scale_as_same_request(
    service: PaymentService,
    repository: AsyncMock,
) -> None:
    repository.create.return_value = _stored(_request(amount=Decimal("10.50")))

    payment = await service.create_payment(_request(amount=Decimal("10.5")))

    assert payment.amount == Decimal("10.50")


@pytest.mark.parametrize(
    "changed",
    [
        {"amount": Decimal("11.00")},
        {"currency": Currency.EUR},
        {"description": "Order #2"},
        {"metadata": {"order_id": 2}},
        {"webhook_url": "https://example.com/other"},
    ],
)
async def test_create_payment_rejects_reused_key_with_different_request(
    service: PaymentService,
    session: AsyncMock,
    repository: AsyncMock,
    changed: dict[str, Any],
) -> None:
    repository.create.return_value = _stored(_request())

    with pytest.raises(IdempotencyKeyConflictError):
        await service.create_payment(_request(**changed))

    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


async def test_get_payment_returns_payment(
    service: PaymentService,
    repository: AsyncMock,
) -> None:
    stored = _stored(_request())
    repository.get.return_value = stored

    assert await service.get_payment(stored.id) == stored


async def test_get_payment_raises_when_missing(
    service: PaymentService,
    repository: AsyncMock,
) -> None:
    repository.get.return_value = None

    with pytest.raises(PaymentNotFoundError):
        await service.get_payment(uuid7())
