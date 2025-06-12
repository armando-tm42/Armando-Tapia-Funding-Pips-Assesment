"""
Main entry point for the FastAPI application
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.routes import api_router
from app.core.config import settings
from app.db.database import create_app_tables

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await create_app_tables()
    yield
    # Shutdown (if needed)

def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    app = FastAPI(
        title=settings.PROJECT_NAME,
        description=settings.PROJECT_DESCRIPTION,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        lifespan=lifespan
    )
    
    # Include API router
    app.include_router(api_router, prefix=settings.API_V1_STR)
    
    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 