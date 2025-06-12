"""
Application configuration settings
"""
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """Application settings"""
    
    PROJECT_NAME: str = "Trade Analysis API"
    PROJECT_DESCRIPTION: str = "Microservice for trades analysis"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # Database settings
    APP_DATABASE_URL: Optional[str] = None
    
    # Security settings
    # SECRET_KEY: str = "your-secret-key-here"
    # ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra environment variables

settings = Settings() 