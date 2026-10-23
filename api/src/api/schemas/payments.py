from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Self
from uuid import UUID

from data.enums import Currency, PaymentStatus
from data.models import WEBHOOK_URL_MAX_LENGTH
from data.repositories.payments import PaymentRead
from pydantic import AfterValidator, BaseModel, Field, HttpUrl


def _check_webhook_url_length(url: HttpUrl) -> HttpUrl:
    if len(str(url)) > WEBHOOK_URL_MAX_LENGTH:
        raise ValueError(f"URL should have at most {WEBHOOK_URL_MAX_LENGTH} characters")
    return url


class PaymentCreateRequest(BaseModel):
    amount: Decimal = Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description="Amount to charge. Must be positive, with at most 2 decimal places.",
        examples=["1500.00"],
    )
    currency: Currency = Field(description="ISO 4217 currency code.", examples=[Currency.RUB])
    description: str = Field(
        description="Human-readable purpose of the payment.",
        examples=["Order #1042"],
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON object stored with the payment and returned as is.",
        examples=[{"order_id": 1042, "customer_id": "c-77"}],
    )
    webhook_url: Annotated[HttpUrl, AfterValidator(_check_webhook_url_length)] = Field(
        description=(
            "URL that receives a POST notification once the payment is processed. "
            f"At most {WEBHOOK_URL_MAX_LENGTH} characters after normalization."
        ),
        examples=["https://merchant.example.com/webhooks/payments"],
    )


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: PaymentRead) -> Self:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: UUID
    amount: Decimal = Field(description="Amount to charge.")
    currency: Currency = Field(description="ISO 4217 currency code.")
    description: str = Field(description="Human-readable purpose of the payment.")
    metadata: dict[str, Any] = Field(description="Client-provided JSON object.")
    status: PaymentStatus
    idempotency_key: str = Field(description="Idempotency key the payment was created with.")
    webhook_url: str = Field(description="URL notified once the payment is processed.")
    created_at: datetime
    processed_at: datetime | None = Field(
        description="When processing finished. `null` while the payment is pending.",
    )

    @classmethod
    def from_payment(cls, payment: PaymentRead) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    detail: str
