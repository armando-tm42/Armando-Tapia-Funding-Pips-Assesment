"""
Health check models
"""
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: datetime
    version: str
    details: Dict[str, Any] = {} 