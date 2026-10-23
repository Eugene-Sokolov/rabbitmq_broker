import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta

from data.queues import PAYMENTS_NEW_QUEUE
from data.repositories.outbox import OutboxEvent, OutboxRepository
from data.repositories.payments import PAYMENT_CREATED_EVENT
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


logger = logging.getLogger(__name__)

EVENT_QUEUES = {PAYMENT_CREATED_EVENT: PAYMENTS_NEW_QUEUE}
CLEANUP_BATCH_SIZE = 1000


class UnknownEventTypeError(Exception):
    def __init__(self, event: OutboxEvent) -> None:
        super().__init__(f"No queue mapped for event type {event.event_type!r} (event {event.id})")


class BatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    fetched: int
    published: int
    failed: int


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval: float,
        max_backoff: float,
        cleanup_interval: float,
        retention: timedelta,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_backoff = max_backoff
        self._cleanup_interval = cleanup_interval
        self._retention = retention

    async def run(self, stop: asyncio.Event) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._publish_loop(stop))
            tasks.create_task(self._cleanup_loop(stop))

    async def _publish_loop(self, stop: asyncio.Event) -> None:
        backoff = self._poll_interval
        while not stop.is_set():
            try:
                result = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                result = None

            if result is None or (result.failed and not result.published):
                await _wait(stop, backoff)
                backoff = min(backoff * 2, self._max_backoff)
                continue

            backoff = self._poll_interval
            if result.fetched < self._batch_size:
                await _wait(stop, self._poll_interval)

    async def publish_batch(self) -> BatchResult:
        async with self._session_factory.begin() as session:
            repository = OutboxRepository(session)
            events = await repository.fetch_unpublished(self._batch_size)
            results = await asyncio.gather(
                *(self._publish(event) for event in events),
                return_exceptions=True,
            )
            published = []
            for event, result in zip(events, results, strict=True):
                if isinstance(result, BaseException):
                    logger.error(f"Failed to publish outbox event {event.id}", exc_info=result)
                else:
                    published.append(event.id)
            await repository.mark_published(published)

        if published:
            logger.info(f"Published {len(published)} outbox events")
        return BatchResult(
            fetched=len(events),
            published=len(published),
            failed=len(events) - len(published),
        )

    async def delete_expired(self) -> int:
        cutoff = datetime.now(UTC) - self._retention
        total = 0
        while True:
            async with self._session_factory.begin() as session:
                deleted = await OutboxRepository(session).delete_published_before(
                    cutoff, CLEANUP_BATCH_SIZE
                )
            total += deleted
            if deleted < CLEANUP_BATCH_SIZE:
                break
        if total:
            logger.info(f"Deleted {total} outbox events published before {cutoff.isoformat()}")
        return total

    async def _cleanup_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.delete_expired()
            except Exception:
                logger.exception("Outbox cleanup failed")
            await _wait(stop, self._cleanup_interval)

    async def _publish(self, event: OutboxEvent) -> None:
        queue = EVENT_QUEUES.get(event.event_type)
        if queue is None:
            raise UnknownEventTypeError(event)
        await self._broker.publish(
            event.payload,
            queue=queue,
            message_id=str(event.id),
            message_type=event.event_type,
            timestamp=event.created_at,
            persist=True,
        )


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(seconds):
            await stop.wait()
