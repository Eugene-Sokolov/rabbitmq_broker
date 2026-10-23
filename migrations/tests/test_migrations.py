import asyncio
from collections.abc import Iterator
from pathlib import Path

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from testcontainers.community.postgres import PostgresContainer


MIGRATIONS_DIR = Path(__file__).parents[1]


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
    config = Config(
        MIGRATIONS_DIR / "alembic.ini",
        toml_file=MIGRATIONS_DIR / "pyproject.toml",
    )
    yield config
    command.downgrade(config, "base")


def _schema(postgres: PostgresContainer) -> tuple[set[str], set[str]]:
    async def fetch() -> tuple[set[str], set[str]]:
        connection = await asyncpg.connect(postgres.get_connection_url(driver=None))
        try:
            tables = await connection.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            )
            types = await connection.fetch(
                "SELECT typname FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid"
            )
        finally:
            await connection.close()
        return {row["tablename"] for row in tables}, {row["typname"] for row in types}

    return asyncio.run(fetch())


def test_upgrade_creates_schema_matching_models(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    command.check(alembic_config)
    tables, types = _schema(postgres)
    assert {"payments", "outbox", "alembic_version"} <= tables
    assert {"currency", "payment_status"} <= types


def test_downgrade_removes_schema_and_allows_upgrading_again(
    alembic_config: Config,
    postgres: PostgresContainer,
) -> None:
    command.upgrade(alembic_config, "head")

    command.downgrade(alembic_config, "base")

    assert _schema(postgres) == ({"alembic_version"}, set())
    command.upgrade(alembic_config, "head")
