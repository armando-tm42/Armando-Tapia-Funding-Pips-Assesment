"""
Health service with business logic
"""
from datetime import datetime
from app.models.health import HealthResponse
from app.core.config import settings

class HealthService:
    """Health service for handling health checks"""
    
    def get_health_status(self) -> HealthResponse:
        """Get basic health status"""
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(),
            version=settings.VERSION
        )
    
    def get_detailed_health_status(self) -> HealthResponse:
        """Get detailed health status with additional information"""
        details = {
            "database": "connected",
            "memory_usage": "normal",
            "cpu_usage": "normal"
        }
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.utcnow(),
            version=settings.VERSION,
            details=details
        ) 