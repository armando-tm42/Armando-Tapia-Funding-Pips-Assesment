"""
Health check endpoints
"""
from fastapi import APIRouter, Depends
from app.services.health_service import HealthService
from app.models.health import HealthResponse

router = APIRouter()

@router.get("/", response_model=HealthResponse)
async def health_check(
    health_service: HealthService = Depends()
):
    """Health check endpoint"""
    return health_service.get_health_status()

@router.get("/detailed", response_model=HealthResponse)
async def detailed_health_check(
    health_service: HealthService = Depends()
):
    """Detailed health check endpoint"""
    return health_service.get_detailed_health_status() 