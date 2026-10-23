import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import AMQPConnectionError
from data.models import Outbox
from data.repositories.payments import PAYMENT_CREATED_EVENT
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from relay.relay import BatchResult, OutboxRelay
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from relay import relay as relay_module


def _event(
    minutes_ago: int,
    event_type: str = PAYMENT_CREATED_EVENT,
    *,
    published_days_ago: int | None = None,
) -> Outbox:
    now = datetime.now(UTC)
    return Outbox(
        event_type=event_type,
        payload={"payment_id": f"payment-{minutes_ago}"},
        created_at=now - timedelta(minutes=minutes_ago),
        published_at=None
        if published_days_ago is None
        else now - timedelta(days=published_days_ago),
    )


def _relay(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    batch_size: int = 10,
) -> OutboxRelay:
    return OutboxRelay(
        sessions,
        broker,
        batch_size=batch_size,
        poll_interval=0.01,
        max_backoff=0.05,
        cleanup_interval=3600,
        retention=timedelta(days=7),
    )


async def _add(sessions: async_sessionmaker[AsyncSession], *events: Outbox) -> None:
    async with sessions.begin() as session:
        session.add_all(events)


async def _published(sessions: async_sessionmaker[AsyncSession]) -> dict[str, bool]:
    async with sessions() as session:
        rows = await session.execute(select(Outbox.payload, Outbox.published_at))
        return {payload["payment_id"]: published_at is not None for payload, published_at in rows}


@pytest.fixture
def received() -> list[tuple[dict[str, Any], RabbitMessage]]:
    return []


@pytest.fixture
def delivered() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture
def broker(
    received: list[tuple[dict[str, Any], RabbitMessage]],
    delivered: asyncio.Event,
) -> RabbitBroker:
    broker = RabbitBroker()

    @broker.subscriber("payments.new")
    async def _handle(body: dict[str, Any], message: RabbitMessage) -> None:
        received.append((body, message))
        delivered.set()

    return broker


async def test_publish_batch_sends_events_oldest_first_and_marks_them_published(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    received: list[tuple[dict[str, Any], RabbitMessage]],
) -> None:
    await _add(sessions, _event(1), _event(2))

    async with TestRabbitBroker(broker) as test_broker:
        result = await _relay(sessions, test_broker).publish_batch()

    assert result == BatchResult(fetched=2, published=2, failed=False)
    assert [body for body, _ in received] == [
        {"payment_id": "payment-2"},
        {"payment_id": "payment-1"},
    ]
    assert await _published(sessions) == {"payment-1": True, "payment-2": True}


async def test_publish_batch_sets_message_id_and_type(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    received: list[tuple[dict[str, Any], RabbitMessage]],
) -> None:
    await _add(sessions, _event(1))
    async with sessions() as session:
        event_id = await session.scalar(select(Outbox.id))

    async with TestRabbitBroker(broker) as test_broker:
        await _relay(sessions, test_broker).publish_batch()

    [(_, message)] = received
    assert message.message_id == str(event_id)
    assert message.raw_message.type == PAYMENT_CREATED_EVENT


async def test_publish_batch_does_not_republish_published_events(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    received: list[tuple[dict[str, Any], RabbitMessage]],
) -> None:
    await _add(sessions, _event(1))

    async with TestRabbitBroker(broker) as test_broker:
        relay = _relay(sessions, test_broker)
        await relay.publish_batch()
        second = await relay.publish_batch()

    assert second == BatchResult(fetched=0, published=0, failed=False)
    assert len(received) == 1


async def test_publish_batch_respects_batch_size(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
) -> None:
    await _add(sessions, _event(3), _event(2), _event(1))

    async with TestRabbitBroker(broker) as test_broker:
        result = await _relay(sessions, test_broker, batch_size=2).publish_batch()

    assert result == BatchResult(fetched=2, published=2, failed=False)
    assert await _published(sessions) == {"payment-3": True, "payment-2": True, "payment-1": False}


async def test_publish_batch_marks_only_confirmed_events_when_some_fail(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _add(sessions, _event(3), _event(2), _event(1))
    broker = AsyncMock(spec=RabbitBroker)
    broker.publish.side_effect = [None, AMQPConnectionError("connection lost"), None]

    result = await _relay(sessions, broker).publish_batch()

    assert result == BatchResult(fetched=3, published=2, failed=True)
    assert await _published(sessions) == {"payment-3": True, "payment-2": False, "payment-1": True}


async def test_publish_batch_leaves_unknown_event_type_unpublished(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _add(sessions, _event(2, event_type="unknown.event"), _event(1))
    broker = AsyncMock(spec=RabbitBroker)

    result = await _relay(sessions, broker).publish_batch()

    assert result == BatchResult(fetched=2, published=1, failed=True)
    broker.publish.assert_awaited_once()
    assert await _published(sessions) == {"payment-2": False, "payment-1": True}


async def test_run_publishes_events_until_stopped(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    delivered: asyncio.Event,
) -> None:
    stop = asyncio.Event()

    async with TestRabbitBroker(broker) as test_broker:
        task = asyncio.create_task(_relay(sessions, test_broker).run(stop))
        await _add(sessions, _event(1))
        await asyncio.wait_for(delivered.wait(), timeout=5)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    assert await _published(sessions) == {"payment-1": True}


async def test_run_retries_after_broker_failure(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _add(sessions, _event(1))
    published = asyncio.Event()
    failures = [AMQPConnectionError("down"), AMQPConnectionError("down")]

    async def publish(*_: object, **__: object) -> None:
        if failures:
            raise failures.pop()
        published.set()

    broker = AsyncMock(spec=RabbitBroker)
    broker.publish.side_effect = publish
    stop = asyncio.Event()

    task = asyncio.create_task(_relay(sessions, broker).run(stop))
    await asyncio.wait_for(published.wait(), timeout=5)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert broker.publish.await_count == 3
    assert await _published(sessions) == {"payment-1": True}


async def _remaining(sessions: async_sessionmaker[AsyncSession]) -> set[str]:
    return set(await _published(sessions))


async def test_delete_expired_removes_only_events_published_before_retention(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _add(
        sessions,
        _event(1, published_days_ago=8),
        _event(2, published_days_ago=1),
        _event(3),
    )

    deleted = await _relay(sessions, AsyncMock(spec=RabbitBroker)).delete_expired()

    assert deleted == 1
    assert await _remaining(sessions) == {"payment-2", "payment-3"}


async def test_delete_expired_deletes_in_batches_until_done(
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(relay_module, "CLEANUP_BATCH_SIZE", 2)
    await _add(sessions, *(_event(minutes, published_days_ago=10) for minutes in range(5)))

    deleted = await _relay(sessions, AsyncMock(spec=RabbitBroker)).delete_expired()

    assert deleted == 5
    assert await _remaining(sessions) == set()


async def test_run_cleans_up_expired_events_on_start(
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _add(sessions, _event(1, published_days_ago=30))
    relay = _relay(sessions, AsyncMock(spec=RabbitBroker))
    delete_expired = relay.delete_expired
    cleaned = asyncio.Event()

    async def track_cleanup() -> int:
        deleted = await delete_expired()
        cleaned.set()
        return deleted

    monkeypatch.setattr(relay, "delete_expired", track_cleanup)
    stop = asyncio.Event()

    task = asyncio.create_task(relay.run(stop))
    await asyncio.wait_for(cleaned.wait(), timeout=5)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert await _remaining(sessions) == set()


async def test_run_keeps_publishing_when_cleanup_fails(
    sessions: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    delivered: asyncio.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()

    async with TestRabbitBroker(broker) as test_broker:
        relay = _relay(sessions, test_broker)
        monkeypatch.setattr(relay, "delete_expired", AsyncMock(side_effect=OSError("db down")))
        task = asyncio.create_task(relay.run(stop))
        await _add(sessions, _event(1))
        await asyncio.wait_for(delivered.wait(), timeout=5)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    assert await _published(sessions) == {"payment-1": True}
