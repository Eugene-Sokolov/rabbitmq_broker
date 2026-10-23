import pytest
from api.schemas.payments import PaymentCreateRequest
from data.models import WEBHOOK_URL_MAX_LENGTH
from pydantic import ValidationError


WEBHOOK_URL_PREFIX = "https://example.com/"


def _request(webhook_url: str) -> PaymentCreateRequest:
    return PaymentCreateRequest(
        amount="10.00",
        currency="RUB",
        description="Order #1",
        webhook_url=webhook_url,
    )


def test_webhook_url_of_max_length_is_accepted() -> None:
    url = WEBHOOK_URL_PREFIX + "a" * (WEBHOOK_URL_MAX_LENGTH - len(WEBHOOK_URL_PREFIX))

    assert str(_request(url).webhook_url) == url


def test_webhook_url_longer_than_max_length_is_rejected() -> None:
    url = WEBHOOK_URL_PREFIX + "a" * (WEBHOOK_URL_MAX_LENGTH - len(WEBHOOK_URL_PREFIX) + 1)

    with pytest.raises(ValidationError, match=f"at most {WEBHOOK_URL_MAX_LENGTH} characters"):
        _request(url)


def test_webhook_url_exceeding_max_length_after_percent_encoding_is_rejected() -> None:
    encoded_chars = 5
    url = (
        WEBHOOK_URL_PREFIX
        + "a" * (WEBHOOK_URL_MAX_LENGTH - len(WEBHOOK_URL_PREFIX) - encoded_chars - 3)
        + "ä" * encoded_chars
    )
    assert len(url) <= WEBHOOK_URL_MAX_LENGTH

    with pytest.raises(ValidationError, match=f"at most {WEBHOOK_URL_MAX_LENGTH} characters"):
        _request(url)
