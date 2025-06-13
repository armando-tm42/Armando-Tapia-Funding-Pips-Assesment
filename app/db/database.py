"""
Database connection and session management with SQLAlchemy (Async)
App database for microservice operations
"""
from typing import Optional, AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from app.core.config import settings
from app.models import Base
from os import (
    makedirs,
    path,
    open,
)



def setup_app_database_url(env_var_value: Optional[str], default_filename: str) -> str:
    """Setup app database URL with SQLite fallback if env var is None"""
    if env_var_value is None:
        # Create data directory if it doesn't exist
        makedirs("data", exist_ok=True)
        
        # Create async SQLite database path
        db_path = f"sqlite+aiosqlite:///./data/{default_filename}"
        
        # Create empty file if it doesn't exist
        file_path = f"data/{default_filename}"
        if not path.exists(file_path):
            open(file_path, 'a').close()
        
        print(f"Created {default_filename}: {db_path}")
        return db_path
    
    return env_var_value

def create_async_database_engine(database_url: str) -> AsyncEngine:
    """Create async SQLAlchemy engine"""
    return create_async_engine(database_url, echo=False)

def create_async_session_factory(engine: AsyncEngine):
    """Create async sessionmaker for given engine"""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False
    )

def create_sync_database_engine(database_url: str) -> Engine:
    """Create sync SQLAlchemy engine"""
    # Convert async URL to sync URL for SQLite
    if database_url.startswith("sqlite+aiosqlite:///"):
        sync_url = database_url.replace("sqlite+aiosqlite:///", "sqlite:///")
    else:
        sync_url = database_url
    return create_engine(sync_url, echo=False)

def create_sync_session_factory(engine: Engine):
    """Create sync sessionmaker for given engine"""
    return sessionmaker(
        bind=engine,
        class_=Session,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False
    )

async def create_database_dependency(session_factory):
    """Create async database dependency function"""
    async def get_database_session() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session
    return get_database_session

# Setup app database URL
APP_DATABASE_URL = setup_app_database_url(settings.APP_DATABASE_URL, "db.sqlite3")

# Update settings with resolved URL
settings.APP_DATABASE_URL = APP_DATABASE_URL

# Create app engine and session (both async and sync)
app_engine = create_async_database_engine(APP_DATABASE_URL)
AppSessionLocal = create_async_session_factory(app_engine)

# Create sync versions for thread-safe operations
app_sync_engine = create_sync_database_engine(APP_DATABASE_URL)
AppSyncSessionLocal = create_sync_session_factory(app_sync_engine)

from app.models import metrics, trades_summary

async def create_app_tables():
    """Create app database tables for microservice operations"""
    # Import models to register them with Base.metadata
    
    # App database: Microservice-specific tables (metrics, config, etc.)
    app_tables = ['metrics', 'trades_summary']  # Add microservice tables here
    if app_tables:
        print("Creating app database tables...")
        async with app_engine.begin() as conn:
            # Create specific tables for the app database
            await conn.run_sync(Base.metadata.create_all)
        print(f"App database tables created successfully: {', '.join(app_tables)}")
    else:
        print("No app-specific tables to create")

# Default database dependency (app database)
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Async dependency to get app database session"""
    async with AppSessionLocal() as session:
        yield session 

def get_sync_db() -> Session:
    """Sync dependency to get app database session"""
    return AppSyncSessionLocal()