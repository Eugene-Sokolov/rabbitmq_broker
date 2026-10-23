from decimal import Decimal
from uuid import uuid7

import pytest
from data.enums import Currency, PaymentStatus
from data.models import Outbox
from data.repositories.payments import (
    PAYMENT_CREATED_EVENT,
    PaymentCreate,
    PaymentRepository,
)
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


def _payment(idempotency_key: str = "key-1") -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal("10.50"),
        currency=Currency.USD,
        description="Order #1",
        metadata={"order_id": 1},
        webhook_url="https://example.com/webhook",
        idempotency_key=idempotency_key,
    )


async def _outbox_rows(session: AsyncSession) -> list[Outbox]:
    return list(await session.scalars(select(Outbox)))


async def test_create_inserts_pending_payment(session: AsyncSession) -> None:
    payment = await PaymentRepository(session).create(_payment())

    assert payment.amount == Decimal("10.50")
    assert payment.currency is Currency.USD
    assert payment.description == "Order #1"
    assert payment.metadata == {"order_id": 1}
    assert payment.webhook_url == "https://example.com/webhook"
    assert payment.idempotency_key == "key-1"
    assert payment.status is PaymentStatus.PENDING
    assert payment.created_at is not None
    assert payment.processed_at is None


async def test_create_writes_outbox_event(session: AsyncSession) -> None:
    payment = await PaymentRepository(session).create(_payment())

    [event] = await _outbox_rows(session)
    assert event.event_type == PAYMENT_CREATED_EVENT
    assert event.payload == {"payment_id": str(payment.id)}
    assert event.published_at is None


async def test_create_with_same_idempotency_key_returns_existing_payment(
    session: AsyncSession,
) -> None:
    repository = PaymentRepository(session)

    first = await repository.create(_payment())
    second = await repository.create(_payment())

    assert second == first
    assert len(await _outbox_rows(session)) == 1


async def test_create_with_different_idempotency_keys_creates_separate_payments(
    session: AsyncSession,
) -> None:
    repository = PaymentRepository(session)

    first = await repository.create(_payment("key-1"))
    second = await repository.create(_payment("key-2"))

    assert first.id != second.id
    assert {event.payload["payment_id"] for event in await _outbox_rows(session)} == {
        str(first.id),
        str(second.id),
    }


async def test_get_returns_created_payment(session: AsyncSession) -> None:
    repository = PaymentRepository(session)
    created = await repository.create(_payment())
    session.expunge_all()

    assert await repository.get(created.id) == created


async def test_get_unknown_payment_returns_none(session: AsyncSession) -> None:
    assert await PaymentRepository(session).get(uuid7()) is None


@pytest.mark.parametrize("amount", ["0", "-1", "10.001"])
def test_payment_create_rejects_invalid_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**_payment().model_dump(), "amount": Decimal(amount)})


def test_payment_create_rejects_invalid_webhook_url() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**_payment().model_dump(), "webhook_url": "not-a-url"})


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_complete_sets_status_and_processed_at(
    session: AsyncSession,
    status: PaymentStatus,
) -> None:
    repository = PaymentRepository(session)
    created = await repository.create(_payment())

    completed = await repository.complete(created.id, status)

    assert completed is not None
    assert completed.status is status
    assert completed.processed_at is not None
    session.expunge_all()
    assert await repository.get(created.id) == completed


async def test_complete_does_not_change_already_processed_payment(session: AsyncSession) -> None:
    repository = PaymentRepository(session)
    created = await repository.create(_payment())
    first = await repository.complete(created.id, PaymentStatus.SUCCEEDED)

    second = await repository.complete(created.id, PaymentStatus.FAILED)

    assert second is None
    session.expunge_all()
    assert await repository.get(created.id) == first


async def test_complete_unknown_payment_returns_none(session: AsyncSession) -> None:
    assert await PaymentRepository(session).complete(uuid7(), PaymentStatus.SUCCEEDED) is None
