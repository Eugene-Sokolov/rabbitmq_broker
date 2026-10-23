from datetime import timedelta
from typing import Self

from pydantic import AmqpDsn, Field, PositiveFloat, PositiveInt, PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    database_url: PostgresDsn
    rabbitmq_url: AmqpDsn
    batch_size: PositiveInt = Field(default=100, validation_alias="RELAY_BATCH_SIZE")
    poll_interval: PositiveFloat = Field(default=1.0, validation_alias="RELAY_POLL_INTERVAL")
    max_backoff: PositiveFloat = Field(default=30.0, validation_alias="RELAY_MAX_BACKOFF")
    cleanup_interval: PositiveFloat = Field(
        default=3600.0, validation_alias="RELAY_CLEANUP_INTERVAL"
    )
    outbox_retention: timedelta = Field(
        default=timedelta(days=7), gt=timedelta(0), validation_alias="RELAY_OUTBOX_RETENTION"
    )

    @model_validator(mode="after")
    def _check_backoff(self) -> Self:
        if self.max_backoff < self.poll_interval:
            message = "RELAY_MAX_BACKOFF must not be smaller than RELAY_POLL_INTERVAL"
            raise ValueError(message)
        return self
