from fastapi import APIRouter, Response, status

from api.dependencies import HealthServiceDep
from api.schemas.health import HealthResponse


router = APIRouter(tags=["health"])


@router.get(
    "/health",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthResponse},
    },
)
async def health(service: HealthServiceDep, response: Response) -> HealthResponse:
    if await service.database_is_available():
        return HealthResponse(status="ok", database="ok")
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthResponse(status="unavailable", database="unavailable")
