import asyncio
from logging.config import fileConfig
from typing import Annotated

from alembic import context
from data.models import Base
from pydantic import PostgresDsn, UrlConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine


LOCK_TIMEOUT = "5s"


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    database_url: Annotated[PostgresDsn, UrlConstraints(allowed_schemes=["postgresql+asyncpg"])]


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
database_url = str(_Settings().database_url)


def _run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_migrations_online() -> None:
    engine = create_async_engine(
        database_url,
        poolclass=pool.NullPool,
        connect_args={"server_settings": {"lock_timeout": LOCK_TIMEOUT}},
    )
    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    _run_migrations_offline()
else:
    asyncio.run(_run_migrations_online())
