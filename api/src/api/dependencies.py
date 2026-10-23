import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from data.repositories.payments import PaymentRepository
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from api.services.health import HealthService
from api.services.payments import PaymentService
from api.settings import Settings


_api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="Static API key issued to the client.",
)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as session:
        yield session


def verify_api_key(
    api_key: Annotated[str | None, Security(_api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    if api_key is None or not secrets.compare_digest(
        api_key.encode(), settings.api_key.get_secret_value().encode()
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing or invalid API key")


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(session, PaymentRepository(session))


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]


def get_health_service(session: Annotated[AsyncSession, Depends(get_session)]) -> HealthService:
    return HealthService(session)


HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
