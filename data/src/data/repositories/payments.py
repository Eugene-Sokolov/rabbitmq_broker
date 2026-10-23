from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from data.enums import Currency, PaymentStatus
from data.models import Outbox, Payment


PAYMENT_CREATED_EVENT = "payment.created"


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl
    idempotency_key: str = Field(min_length=1, max_length=255)


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, payment: PaymentCreate) -> PaymentRead:
        statement = (
            insert(Payment)
            .values(
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                meta=payment.metadata,
                webhook_url=str(payment.webhook_url),
                idempotency_key=payment.idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        created = (await self._session.execute(statement)).scalar_one_or_none()
        if created is None:
            existing = await self._session.scalar(
                select(Payment).where(Payment.idempotency_key == payment.idempotency_key)
            )
            return PaymentRead.model_validate(existing)

        self._session.add(
            Outbox(
                event_type=PAYMENT_CREATED_EVENT,
                payload={"payment_id": str(created.id)},
            )
        )
        await self._session.flush()
        return PaymentRead.model_validate(created)

    async def get(self, payment_id: UUID) -> PaymentRead | None:
        payment = await self._session.get(Payment, payment_id)
        return PaymentRead.model_validate(payment) if payment else None

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> PaymentRead | None:
        payment = await self._session.scalar(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
        )
        return PaymentRead.model_validate(payment) if payment else None
