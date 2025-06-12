"""
User models
"""
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserBase(BaseModel):
    """Base user model"""
    email: EmailStr
    name: str
    is_active: bool = True

class UserCreate(UserBase):
    """User creation model"""
    password: str

class UserUpdate(BaseModel):
    """User update model"""
    email: Optional[EmailStr] = None
    name: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None

class User(UserBase):
    """User response model"""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True 