from typing import Protocol
from uuid import UUID

from data.enums import PaymentStatus
from data.repositories.payments import PaymentRead, PaymentRepository
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class PaymentGateway(Protocol):
    async def charge(self, payment: PaymentRead) -> PaymentStatus: ...


class PaymentNotifier(Protocol):
    async def notify(self, payment: PaymentRead) -> None: ...


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")


class PaymentProcessor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        notifier: PaymentNotifier,
    ) -> None:
        self._sessions = sessions
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: UUID) -> PaymentRead:
        payment = await self._get(payment_id)
        if payment.status is PaymentStatus.PENDING:
            status = await self._gateway.charge(payment)
            payment = await self._complete(payment_id, status)
        await self._notifier.notify(payment)
        return payment

    async def _get(self, payment_id: UUID) -> PaymentRead:
        async with self._sessions() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _complete(self, payment_id: UUID, status: PaymentStatus) -> PaymentRead:
        async with self._sessions.begin() as session:
            repository = PaymentRepository(session)
            payment = await repository.complete(payment_id, status) or await repository.get(
                payment_id
            )
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
