from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from consumer.gateway import EmulatedPaymentGateway
from data.enums import Currency, PaymentStatus
from data.repositories.payments import PaymentRead

from consumer import gateway as gateway_module


PAYMENT = PaymentRead(
    id=uuid7(),
    amount=Decimal("10.00"),
    currency=Currency.USD,
    description="Order #1",
    meta={},
    status=PaymentStatus.PENDING,
    idempotency_key="key-1",
    webhook_url="https://example.com/webhook",
    created_at=datetime.now(UTC),
    processed_at=None,
)
GATEWAY = EmulatedPaymentGateway(success_rate=0.9, min_seconds=2, max_seconds=5)


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    sleep = AsyncMock()
    monkeypatch.setattr(gateway_module.asyncio, "sleep", sleep)
    return sleep


def _roll(monkeypatch: pytest.MonkeyPatch, chance: float) -> None:
    monkeypatch.setattr(gateway_module.random, "random", lambda: chance)


async def test_charge_waits_for_random_duration_between_min_and_max(
    monkeypatch: pytest.MonkeyPatch,
    sleep: AsyncMock,
) -> None:
    bounds: list[tuple[float, float]] = []

    def uniform(low: float, high: float) -> float:
        bounds.append((low, high))
        return 3.7

    monkeypatch.setattr(gateway_module.random, "uniform", uniform)

    await GATEWAY.charge(PAYMENT)

    assert bounds == [(2, 5)]
    sleep.assert_awaited_once_with(3.7)


@pytest.mark.parametrize(
    ("chance", "expected"),
    [
        (0.0, PaymentStatus.SUCCEEDED),
        (0.899, PaymentStatus.SUCCEEDED),
        (0.9, PaymentStatus.FAILED),
        (0.999, PaymentStatus.FAILED),
    ],
)
async def test_charge_succeeds_when_roll_is_below_success_rate(
    monkeypatch: pytest.MonkeyPatch,
    sleep: AsyncMock,  # noqa: ARG001
    chance: float,
    expected: PaymentStatus,
) -> None:
    _roll(monkeypatch, chance)

    assert await GATEWAY.charge(PAYMENT) is expected
