from datetime import timedelta

import pytest
from pydantic import ValidationError
from relay.settings import Settings


DATABASE_URL = "postgresql+asyncpg://payments:payments@postgres:5432/payments"
RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("RABBITMQ_URL", RABBITMQ_URL)
    for variable in (
        "RELAY_BATCH_SIZE",
        "RELAY_POLL_INTERVAL",
        "RELAY_MAX_BACKOFF",
        "RELAY_CLEANUP_INTERVAL",
        "RELAY_OUTBOX_RETENTION",
    ):
        monkeypatch.delenv(variable, raising=False)


def test_settings_load_urls_and_defaults() -> None:
    settings = Settings()

    assert str(settings.database_url) == DATABASE_URL
    assert str(settings.rabbitmq_url) == RABBITMQ_URL
    assert settings.batch_size == 100
    assert settings.poll_interval == 1.0
    assert settings.max_backoff == 30.0
    assert settings.cleanup_interval == 3600.0
    assert settings.outbox_retention == timedelta(days=7)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("P3D", timedelta(days=3)), ("PT12H", timedelta(hours=12))],
)
def test_settings_parse_outbox_retention(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: timedelta,
) -> None:
    monkeypatch.setenv("RELAY_OUTBOX_RETENTION", value)

    assert Settings().outbox_retention == expected


def test_settings_read_relay_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELAY_BATCH_SIZE", "50")
    monkeypatch.setenv("RELAY_POLL_INTERVAL", "0.5")
    monkeypatch.setenv("RELAY_MAX_BACKOFF", "10")

    settings = Settings()

    assert settings.batch_size == 50
    assert settings.poll_interval == 0.5
    assert settings.max_backoff == 10.0


@pytest.mark.parametrize("variable", ["DATABASE_URL", "RABBITMQ_URL"])
def test_settings_require_urls(monkeypatch: pytest.MonkeyPatch, variable: str) -> None:
    monkeypatch.delenv(variable)

    with pytest.raises(ValidationError, match=variable.lower()):
        Settings()


def test_settings_reject_non_amqp_rabbitmq_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_URL", "http://rabbitmq:15672/")

    with pytest.raises(ValidationError, match="rabbitmq_url"):
        Settings()


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("RELAY_BATCH_SIZE", "0"),
        ("RELAY_BATCH_SIZE", "ten"),
        ("RELAY_POLL_INTERVAL", "-1"),
        ("RELAY_MAX_BACKOFF", "0"),
        ("RELAY_CLEANUP_INTERVAL", "0"),
        ("RELAY_OUTBOX_RETENTION", "PT0S"),
        ("RELAY_OUTBOX_RETENTION", "-P1D"),
    ],
)
def test_settings_reject_invalid_relay_variables(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ValidationError, match=variable):
        Settings()


def test_settings_reject_backoff_smaller_than_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RELAY_POLL_INTERVAL", "5")
    monkeypatch.setenv("RELAY_MAX_BACKOFF", "2")

    with pytest.raises(ValidationError, match="RELAY_MAX_BACKOFF"):
        Settings()
