from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from data.models import Outbox


class OutboxEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    event_type: str
    payload: dict[str, Any]
    created_at: datetime


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def fetch_unpublished(self, limit: int) -> list[OutboxEvent]:
        events = await self._session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [OutboxEvent.model_validate(event) for event in events]

    async def mark_published(self, event_ids: Sequence[UUID]) -> None:
        if not event_ids:
            return
        await self._session.execute(
            update(Outbox).where(Outbox.id.in_(event_ids)).values(published_at=func.now())
        )

    async def delete_published_before(self, cutoff: datetime, limit: int) -> int:
        expired = (
            select(Outbox.id)
            .where(Outbox.published_at < cutoff)
            .order_by(Outbox.published_at)
            .limit(limit)
        )
        result = await self._session.execute(
            delete(Outbox)
            .where(Outbox.id.in_(expired))
            .execution_options(synchronize_session=False)
        )
        return result.rowcount
