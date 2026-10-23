import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid7

import httpx
import pytest
from consumer.webhooks import WebhookNotifier
from data.enums import Currency, PaymentStatus
from data.repositories.payments import PaymentRead


PROCESSED_AT = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
PAYMENT = PaymentRead(
    id=uuid7(),
    amount=Decimal("1500.00"),
    currency=Currency.RUB,
    description="Order #1042",
    meta={"order_id": 1042},
    status=PaymentStatus.SUCCEEDED,
    idempotency_key="key-1",
    webhook_url="https://merchant.example.com/webhooks/payments",
    created_at=PROCESSED_AT,
    processed_at=PROCESSED_AT,
)


def _notifier(handler: httpx.MockTransport) -> WebhookNotifier:
    return WebhookNotifier(httpx.AsyncClient(transport=handler))


async def test_notify_posts_payment_result_to_webhook_url() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    await _notifier(httpx.MockTransport(handle)).notify(PAYMENT)

    [request] = requests
    assert request.method == "POST"
    assert str(request.url) == PAYMENT.webhook_url
    assert json.loads(request.content) == {
        "payment_id": str(PAYMENT.id),
        "status": "succeeded",
        "amount": "1500.00",
        "currency": "RUB",
        "processed_at": "2026-10-22T12:00:00Z",
    }


@pytest.mark.parametrize("status_code", [400, 404, 500, 503])
async def test_notify_raises_on_error_response(status_code: int) -> None:
    notifier = _notifier(httpx.MockTransport(lambda _: httpx.Response(status_code)))

    with pytest.raises(httpx.HTTPStatusError):
        await notifier.notify(PAYMENT)


async def test_notify_raises_when_receiver_is_unreachable() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        message = "connection refused"
        raise httpx.ConnectError(message, request=request)

    with pytest.raises(httpx.ConnectError):
        await _notifier(httpx.MockTransport(handle)).notify(PAYMENT)
