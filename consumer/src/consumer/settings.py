from typing import Annotated, Self

from pydantic import (
    AmqpDsn,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    PostgresDsn,
    UrlConstraints,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    database_url: Annotated[PostgresDsn, UrlConstraints(allowed_schemes=["postgresql+asyncpg"])]
    rabbitmq_url: AmqpDsn
    prefetch_count: PositiveInt = Field(default=10, validation_alias="CONSUMER_PREFETCH_COUNT")
    max_attempts: PositiveInt = Field(default=3, validation_alias="CONSUMER_MAX_ATTEMPTS")
    webhook_timeout: PositiveFloat = Field(
        default=10.0, validation_alias="CONSUMER_WEBHOOK_TIMEOUT"
    )
    gateway_success_rate: float = Field(
        default=0.9, ge=0, le=1, validation_alias="GATEWAY_SUCCESS_RATE"
    )
    gateway_min_seconds: NonNegativeFloat = Field(
        default=2.0, validation_alias="GATEWAY_MIN_SECONDS"
    )
    gateway_max_seconds: NonNegativeFloat = Field(
        default=5.0, validation_alias="GATEWAY_MAX_SECONDS"
    )

    @model_validator(mode="after")
    def _check_gateway_duration(self) -> Self:
        if self.gateway_max_seconds < self.gateway_min_seconds:
            message = "GATEWAY_MAX_SECONDS must not be smaller than GATEWAY_MIN_SECONDS"
            raise ValueError(message)
        return self
