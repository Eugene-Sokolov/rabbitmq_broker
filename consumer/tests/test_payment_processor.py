from decimal import Decimal
from uuid import UUID, uuid7

import pytest
from consumer.services.payments import PaymentNotFoundError, PaymentProcessor
from data.enums import Currency, PaymentStatus
from data.repositories.payments import PaymentCreate, PaymentRead, PaymentRepository
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class _FakeGateway:
    def __init__(self, result: PaymentStatus) -> None:
        self.result = result
        self.charged: list[UUID] = []

    async def charge(self, payment: PaymentRead) -> PaymentStatus:
        self.charged.append(payment.id)
        return self.result


class _FakeNotifier:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.notified: list[PaymentRead] = []

    async def notify(self, payment: PaymentRead) -> None:
        self.notified.append(payment)
        if self.error:
            raise self.error


async def _create_payment(sessions: async_sessionmaker[AsyncSession]) -> PaymentRead:
    async with sessions.begin() as session:
        return await PaymentRepository(session).create(
            PaymentCreate(
                amount=Decimal("10.50"),
                currency=Currency.USD,
                description="Order #1",
                webhook_url="https://example.com/webhook",
                idempotency_key="key-1",
            )
        )


async def _stored(sessions: async_sessionmaker[AsyncSession], payment_id: UUID) -> PaymentRead:
    async with sessions() as session:
        payment = await PaymentRepository(session).get(payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_process_charges_pending_payment_and_notifies_result(
    sessions: async_sessionmaker[AsyncSession],
    result: PaymentStatus,
) -> None:
    payment = await _create_payment(sessions)
    gateway, notifier = _FakeGateway(result), _FakeNotifier()

    processed = await PaymentProcessor(sessions, gateway, notifier).process(payment.id)

    assert gateway.charged == [payment.id]
    assert processed.status is result
    assert processed.processed_at is not None
    assert await _stored(sessions, payment.id) == processed
    assert notifier.notified == [processed]


async def test_process_does_not_charge_already_processed_payment(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    payment = await _create_payment(sessions)
    await PaymentProcessor(
        sessions, _FakeGateway(PaymentStatus.SUCCEEDED), _FakeNotifier()
    ).process(payment.id)
    gateway, notifier = _FakeGateway(PaymentStatus.FAILED), _FakeNotifier()

    processed = await PaymentProcessor(sessions, gateway, notifier).process(payment.id)

    assert gateway.charged == []
    assert processed.status is PaymentStatus.SUCCEEDED
    assert notifier.notified == [processed]


async def test_process_keeps_result_when_webhook_fails(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    payment = await _create_payment(sessions)
    processor = PaymentProcessor(
        sessions,
        _FakeGateway(PaymentStatus.SUCCEEDED),
        _FakeNotifier(error=ConnectionError("receiver down")),
    )

    with pytest.raises(ConnectionError):
        await processor.process(payment.id)

    assert (await _stored(sessions, payment.id)).status is PaymentStatus.SUCCEEDED


async def test_process_uses_stored_result_when_another_delivery_completed_first(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    payment = await _create_payment(sessions)

    class _RacingGateway(_FakeGateway):
        async def charge(self, payment: PaymentRead) -> PaymentStatus:
            async with sessions.begin() as session:
                await PaymentRepository(session).complete(payment.id, PaymentStatus.FAILED)
            return await super().charge(payment)

    notifier = _FakeNotifier()

    processed = await PaymentProcessor(
        sessions, _RacingGateway(PaymentStatus.SUCCEEDED), notifier
    ).process(payment.id)

    assert processed.status is PaymentStatus.FAILED
    assert notifier.notified == [processed]


async def test_process_unknown_payment_raises(sessions: async_sessionmaker[AsyncSession]) -> None:
    processor = PaymentProcessor(sessions, _FakeGateway(PaymentStatus.SUCCEEDED), _FakeNotifier())

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid7())
