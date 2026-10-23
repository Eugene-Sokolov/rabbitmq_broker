import pytest
from consumer.settings import Settings
from pydantic import ValidationError


DATABASE_URL = "postgresql+asyncpg://payments:payments@postgres:5432/payments"
RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
OPTIONAL_VARIABLES = (
    "CONSUMER_PREFETCH_COUNT",
    "CONSUMER_MAX_ATTEMPTS",
    "CONSUMER_WEBHOOK_TIMEOUT",
    "GATEWAY_SUCCESS_RATE",
    "GATEWAY_MIN_SECONDS",
    "GATEWAY_MAX_SECONDS",
)


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("RABBITMQ_URL", RABBITMQ_URL)
    for variable in OPTIONAL_VARIABLES:
        monkeypatch.delenv(variable, raising=False)


def test_settings_defaults_follow_specification() -> None:
    settings = Settings()

    assert str(settings.database_url) == DATABASE_URL
    assert str(settings.rabbitmq_url) == RABBITMQ_URL
    assert settings.max_attempts == 3
    assert settings.gateway_success_rate == 0.9
    assert (settings.gateway_min_seconds, settings.gateway_max_seconds) == (2.0, 5.0)


@pytest.mark.parametrize("variable", ["DATABASE_URL", "RABBITMQ_URL"])
def test_settings_require_urls(monkeypatch: pytest.MonkeyPatch, variable: str) -> None:
    monkeypatch.delenv(variable)

    with pytest.raises(ValidationError, match=variable.lower()):
        Settings()


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("CONSUMER_PREFETCH_COUNT", "0"),
        ("CONSUMER_MAX_ATTEMPTS", "0"),
        ("CONSUMER_WEBHOOK_TIMEOUT", "0"),
        ("GATEWAY_SUCCESS_RATE", "1.5"),
        ("GATEWAY_SUCCESS_RATE", "-0.1"),
        ("GATEWAY_MIN_SECONDS", "-1"),
    ],
)
def test_settings_reject_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ValidationError, match=variable):
        Settings()


def test_settings_reject_max_duration_below_min(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GATEWAY_MIN_SECONDS", "5")
    monkeypatch.setenv("GATEWAY_MAX_SECONDS", "2")

    with pytest.raises(ValidationError, match="GATEWAY_MAX_SECONDS"):
        Settings()
