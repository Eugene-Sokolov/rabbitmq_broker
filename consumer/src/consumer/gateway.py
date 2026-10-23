import asyncio
import random
from typing import override

from data.enums import PaymentStatus
from data.repositories.payments import PaymentRead

from consumer.services.payments import PaymentGateway


class EmulatedPaymentGateway(PaymentGateway):
    def __init__(self, *, success_rate: float, min_seconds: float, max_seconds: float) -> None:
        self._success_rate = success_rate
        self._min_seconds = min_seconds
        self._max_seconds = max_seconds

    @override
    async def charge(self, payment: PaymentRead) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self._min_seconds, self._max_seconds))  # noqa: S311
        if random.random() < self._success_rate:  # noqa: S311
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
