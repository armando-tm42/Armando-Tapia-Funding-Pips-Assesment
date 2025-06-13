"""
Data models and schemas
"""
from typing import Type
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

# Initialize Base with proper type annotation
Base: Type[DeclarativeMeta] = declarative_base()