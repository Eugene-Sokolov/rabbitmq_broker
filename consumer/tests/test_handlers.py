from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid7

import pytest
from consumer.handlers import register_handlers
from consumer.services.payments import PaymentNotFoundError
from consumer.topology import ATTEMPT_HEADER, PAYMENTS_QUEUE, RetryPolicy
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage


RETRY = RetryPolicy(max_attempts=3)


class _FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[UUID] = []

    async def process(self, payment_id: UUID) -> None:
        self.processed.append(payment_id)
        if self.error:
            raise self.error


class _Retries:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any], RabbitMessage]] = []


def _capture_into(retries: _Retries, queue_name: str) -> Callable[..., Awaitable[None]]:
    async def capture(body: dict[str, Any], message: RabbitMessage) -> None:
        retries.messages.append((queue_name, body, message))

    return capture


def _broker(processor: _FakeProcessor, retries: _Retries) -> RabbitBroker:
    broker = RabbitBroker()
    register_handlers(broker, processor, RETRY, prefetch_count=1)
    for queue in RETRY.queues():
        broker.subscriber(queue.name)(_capture_into(retries, queue.name))
    return broker


async def _publish(broker: RabbitBroker, payment_id: UUID, attempt: int | None = None) -> None:
    headers = {ATTEMPT_HEADER: attempt} if attempt else None
    await broker.publish(
        {"payment_id": str(payment_id)},
        queue=PAYMENTS_QUEUE,
        headers=headers,
        message_id="event-1",
    )


async def test_successful_processing_is_not_retried() -> None:
    processor, retries = _FakeProcessor(), _Retries()
    payment_id = uuid7()

    async with TestRabbitBroker(_broker(processor, retries)) as broker:
        await _publish(broker, payment_id)

    assert processor.processed == [payment_id]
    assert retries.messages == []


@pytest.mark.parametrize(
    ("attempt", "queue_name"),
    [(None, "payments.new.retry.1"), (2, "payments.new.retry.2")],
)
async def test_failed_attempt_is_republished_to_retry_queue_with_next_attempt(
    attempt: int | None,
    queue_name: str,
) -> None:
    processor, retries = _FakeProcessor(error=ConnectionError("webhook down")), _Retries()
    payment_id = uuid7()

    async with TestRabbitBroker(_broker(processor, retries)) as broker:
        await _publish(broker, payment_id, attempt)

    [(retried_to, body, message)] = retries.messages
    assert retried_to == queue_name
    assert body == {"payment_id": str(payment_id)}
    assert message.headers[ATTEMPT_HEADER] == (attempt or 1) + 1
    assert message.message_id == "event-1"


async def test_last_failed_attempt_is_rejected_for_dead_lettering() -> None:
    processor, retries = _FakeProcessor(error=ConnectionError("webhook down")), _Retries()

    async with TestRabbitBroker(_broker(processor, retries)) as broker:
        with pytest.raises(ConnectionError):
            await _publish(broker, uuid7(), attempt=RETRY.max_attempts)

    assert retries.messages == []


async def test_unknown_payment_is_rejected_without_retry() -> None:
    payment_id = uuid7()
    processor, retries = _FakeProcessor(error=PaymentNotFoundError(payment_id)), _Retries()

    async with TestRabbitBroker(_broker(processor, retries)) as broker:
        with pytest.raises(PaymentNotFoundError):
            await _publish(broker, payment_id)

    assert retries.messages == []
