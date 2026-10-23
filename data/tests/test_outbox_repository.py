from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from data.models import Outbox
from data.repositories.outbox import OutboxRepository
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def _event(minutes_ago: int, *, published: bool = False) -> Outbox:
    created_at = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    return Outbox(
        event_type="test.event",
        payload={"minutes_ago": minutes_ago},
        created_at=created_at,
        published_at=created_at if published else None,
    )


@pytest.fixture
async def committed_sessions(
    engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    yield sessions
    async with sessions.begin() as session:
        await session.execute(delete(Outbox))


async def test_fetch_unpublished_returns_oldest_first(session: AsyncSession) -> None:
    session.add_all([_event(1), _event(3), _event(2)])
    await session.flush()

    events = await OutboxRepository(session).fetch_unpublished(limit=10)

    assert [event.payload["minutes_ago"] for event in events] == [3, 2, 1]


async def test_fetch_unpublished_skips_published_events(session: AsyncSession) -> None:
    session.add_all([_event(2, published=True), _event(1)])
    await session.flush()

    events = await OutboxRepository(session).fetch_unpublished(limit=10)

    assert [event.payload["minutes_ago"] for event in events] == [1]


async def test_fetch_unpublished_respects_limit(session: AsyncSession) -> None:
    session.add_all([_event(3), _event(2), _event(1)])
    await session.flush()

    events = await OutboxRepository(session).fetch_unpublished(limit=2)

    assert [event.payload["minutes_ago"] for event in events] == [3, 2]


async def test_mark_published_sets_published_at(session: AsyncSession) -> None:
    first, second = _event(2), _event(1)
    session.add_all([first, second])
    await session.flush()
    repository = OutboxRepository(session)

    await repository.mark_published([first.id])

    published_at = dict((await session.execute(select(Outbox.id, Outbox.published_at))).all())
    assert published_at[first.id] is not None
    assert published_at[second.id] is None
    assert [event.id for event in await repository.fetch_unpublished(limit=10)] == [second.id]


async def test_mark_published_with_no_ids_does_nothing(session: AsyncSession) -> None:
    session.add(_event(1))
    await session.flush()
    repository = OutboxRepository(session)

    await repository.mark_published([])

    assert len(await repository.fetch_unpublished(limit=10)) == 1


async def test_fetch_unpublished_skips_rows_locked_by_another_transaction(
    committed_sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with committed_sessions.begin() as session:
        session.add_all([_event(3), _event(2), _event(1)])

    async with committed_sessions.begin() as first, committed_sessions.begin() as second:
        locked = await OutboxRepository(first).fetch_unpublished(limit=2)
        remaining = await OutboxRepository(second).fetch_unpublished(limit=10)

    assert [event.payload["minutes_ago"] for event in locked] == [3, 2]
    assert [event.payload["minutes_ago"] for event in remaining] == [1]


async def test_delete_published_before_removes_only_old_published_events(
    session: AsyncSession,
) -> None:
    old, recent, pending = _event(60, published=True), _event(5, published=True), _event(90)
    session.add_all([old, recent, pending])
    await session.flush()
    cutoff = datetime.now(UTC) - timedelta(minutes=30)

    deleted = await OutboxRepository(session).delete_published_before(cutoff, limit=10)

    assert deleted == 1
    remaining = set(await session.scalars(select(Outbox.id)))
    assert remaining == {recent.id, pending.id}


async def test_delete_published_before_respects_limit_oldest_first(
    session: AsyncSession,
) -> None:
    events = [_event(minutes, published=True) for minutes in (30, 20, 10)]
    session.add_all(events)
    await session.flush()
    repository = OutboxRepository(session)

    deleted = await repository.delete_published_before(datetime.now(UTC), limit=2)

    assert deleted == 2
    assert set(await session.scalars(select(Outbox.id))) == {events[2].id}


async def test_delete_published_before_returns_zero_when_nothing_expired(
    session: AsyncSession,
) -> None:
    session.add(_event(5, published=True))
    await session.flush()
    cutoff = datetime.now(UTC) - timedelta(hours=1)

    assert await OutboxRepository(session).delete_published_before(cutoff, limit=10) == 0
