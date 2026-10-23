from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from api.services.payments import IdempotencyKeyConflictError, PaymentNotFoundError


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(PaymentNotFoundError, _respond_with(status.HTTP_404_NOT_FOUND))
    app.add_exception_handler(IdempotencyKeyConflictError, _respond_with(status.HTTP_409_CONFLICT))


def _respond_with(status_code: int) -> Callable[[Request, Exception], Awaitable[JSONResponse]]:
    async def respond(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse(status_code=status_code, content={"detail": str(error)})

    return respond
