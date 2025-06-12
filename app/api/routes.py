"""
Main API router that includes all sub-routers
"""
from fastapi import APIRouter
from app.api.endpoints import health, users

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(users.router, prefix="/users", tags=["users"])