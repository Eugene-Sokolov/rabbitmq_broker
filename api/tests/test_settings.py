import pytest
from api.settings import Settings
from pydantic import ValidationError


DATABASE_URL = "postgresql+asyncpg://payments:payments@postgres:5432/payments"


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("API_KEY", "secret-key")


def test_settings_load_from_environment() -> None:
    settings = Settings()

    assert str(settings.database_url) == DATABASE_URL
    assert settings.api_key.get_secret_value() == "secret-key"


def test_settings_hide_api_key() -> None:
    assert "secret-key" not in repr(Settings())


@pytest.mark.parametrize("variable", ["DATABASE_URL", "API_KEY"])
def test_settings_require_variable(monkeypatch: pytest.MonkeyPatch, variable: str) -> None:
    monkeypatch.delenv(variable)

    with pytest.raises(ValidationError, match=variable.lower()):
        Settings()


def test_settings_reject_empty_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY", "")

    with pytest.raises(ValidationError, match="api_key"):
        Settings()


@pytest.mark.parametrize("url", ["mysql://user:pass@db/payments", "not-a-url"])
def test_settings_reject_non_postgres_database_url(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", url)

    with pytest.raises(ValidationError, match="database_url"):
        Settings()
