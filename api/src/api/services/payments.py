from uuid import UUID

from data.repositories.payments import PaymentCreate, PaymentRead, PaymentRepository
from sqlalchemy.ext.asyncio import AsyncSession


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")


class IdempotencyKeyConflictError(Exception):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency key {idempotency_key!r} was already used with different parameters"
        )


class PaymentService:
    def __init__(self, session: AsyncSession, payments: PaymentRepository) -> None:
        self._session = session
        self._payments = payments

    async def create_payment(self, request: PaymentCreate) -> PaymentRead:
        payment = await self._payments.create(request)
        if not _matches_request(payment, request):
            await self._session.rollback()
            raise IdempotencyKeyConflictError(request.idempotency_key)
        await self._session.commit()
        return payment

    async def get_payment(self, payment_id: UUID) -> PaymentRead:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment


def _matches_request(payment: PaymentRead, request: PaymentCreate) -> bool:
    return (
        payment.amount == request.amount
        and payment.currency == request.currency
        and payment.description == request.description
        and payment.metadata == request.metadata
        and payment.webhook_url == str(request.webhook_url)
    )
