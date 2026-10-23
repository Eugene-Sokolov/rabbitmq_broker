import asyncio

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession


DATABASE_TIMEOUT_SECONDS = 2


class HealthService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def database_is_available(self) -> bool:
        try:
            async with asyncio.timeout(DATABASE_TIMEOUT_SECONDS):
                await self._session.execute(text("SELECT 1"))
        except SQLAlchemyError, OSError, TimeoutError:
            return False
        return True
