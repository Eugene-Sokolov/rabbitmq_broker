import asyncio
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from data.models import Base
from pydantic import ValidationError
from sqlalchemy import Enum
from sqlalchemy.exc import DBAPIError
from testcontainers.community.postgres import PostgresContainer


MIGRATIONS_DIR = Path(__file__).parents[1]
LOCK_HOLD_SECONDS = 30


def _config() -> Config:
    return Config(
        MIGRATIONS_DIR / "alembic.ini",
        toml_file=MIGRATIONS_DIR / "pyproject.toml",
    )


@pytest.fixture(scope="module")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:18-alpine", driver="asyncpg") as postgres:
        yield postgres


@pytest.fixture
def alembic_config(
    postgres: PostgresContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Config]:
    monkeypatch.setenv("DATABASE_URL", postgres.get_connection_url())
    config = _config()
    yield config
    command.downgrade(config, "base")


def _fetch(postgres: PostgresContainer, query: str) -> list[asyncpg.Record]:
    async def fetch() -> list[asyncpg.Record]:
        connection = await asyncpg.connect(postgres.get_connection_url(driver=None))
        try:
            return await connection.fetch(query)
        finally:
            await connection.close()

    return asyncio.run(fetch())


def _schema(postgres: PostgresContainer) -> tuple[set[str], set[str]]:
    tables = _fetch(postgres, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    types = _fetch(postgres, "SELECT typname FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid")
    return {row["tablename"] for row in tables}, {row["typname"] for row in types}


def _database_enums(postgres: PostgresContainer) -> dict[str, set[str]]:
    rows = _fetch(
        postgres,
        "SELECT t.typname, e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid",
    )
    enums: dict[str, set[str]] = {}
    for row in rows:
        enums.setdefault(row["typname"], set()).add(row["enumlabel"])
    return enums


def _model_enums() -> dict[str, set[str]]:
    return {
        column.type.name: set(column.type.enums)
        for table in Base.metadata.tables.values()
        for column in table.columns
        if isinstance(column.type, Enum)
    }


@contextmanager
def _locked(postgres: PostgresContainer, table: str) -> Iterator[None]:
    locked, release = threading.Event(), threading.Event()

    async def hold() -> None:
        connection = await asyncpg.connect(postgres.get_connection_url(driver=None))
        try:
            async with connection.transaction():
                await connection.execute(f"LOCK TABLE {table} IN ACCESS SHARE MODE")
                locked.set()
                await asyncio.to_thread(release.wait, LOCK_HOLD_SECONDS)
        finally:
            await connection.close()

    holder = threading.Thread(target=asyncio.run, args=(hold(),))
    holder.start()
    try:
        assert locked.wait(LOCK_HOLD_SECONDS)
        yield
    finally:
        release.set()
        holder.join()


def test_upgrade_creates_schema_matching_models(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    command.check(alembic_config)
    tables, types = _schema(postgres)
    assert {"payments", "outbox", "alembic_version"} <= tables
    assert {"currency", "payment_status"} <= types


def test_upgrade_creates_enums_matching_models(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    assert _database_enums(postgres) == _model_enums()


def test_downgrade_removes_schema_and_allows_upgrading_again(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    command.downgrade(alembic_config, "base")

    assert _schema(postgres) == ({"alembic_version"}, set())
    command.upgrade(alembic_config, "head")


def test_migration_fails_instead_of_waiting_for_locked_table(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    with _locked(postgres, "outbox"), pytest.raises(DBAPIError, match="lock timeout"):
        command.downgrade(alembic_config, "-1")


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:pass@db/payments",
        "mysql://user:pass@db/payments",
        "not-a-url",
    ],
)
def test_migrations_reject_non_asyncpg_database_url(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", url)

    with pytest.raises(ValidationError, match="database_url"):
        command.upgrade(_config(), "head")


def test_migrations_require_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValidationError, match="database_url"):
        command.upgrade(_config(), "head")
