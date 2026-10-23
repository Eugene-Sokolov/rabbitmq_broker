from typing import Annotated

from pydantic import Field, PostgresDsn, SecretStr, UrlConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    database_url: Annotated[PostgresDsn, UrlConstraints(allowed_schemes=["postgresql+asyncpg"])]
    api_key: SecretStr = Field(min_length=1)
