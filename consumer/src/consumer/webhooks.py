from datetime import datetime
from decimal import Decimal
from typing import Self, override
from uuid import UUID

import httpx
from data.enums import Currency, PaymentStatus
from data.repositories.payments import PaymentRead
from pydantic import BaseModel

from consumer.services.payments import PaymentNotifier


class WebhookPayload(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: PaymentRead) -> Self:
        return cls(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )


class WebhookNotifier(PaymentNotifier):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @override
    async def notify(self, payment: PaymentRead) -> None:
        response = await self._client.post(
            payment.webhook_url,
            json=WebhookPayload.from_payment(payment).model_dump(mode="json"),
        )
        response.raise_for_status()
