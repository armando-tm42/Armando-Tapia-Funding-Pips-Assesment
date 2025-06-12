"""
Account Pydantic models for API responses and validation
"""
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

class AccountBase(BaseModel):
    """Base account model with shared attributes"""
    login: int = Field(..., description="Account login ID (unique identifier)")
    account_size: float = Field(..., ge=0, description="Account size/balance")
    platform: int = Field(..., description="Trading platform ID")
    phase: int = Field(..., ge=0, description="Account phase/level")
    user_id: int = Field(..., description="User ID associated with account")
    challenge_id: int = Field(..., description="Challenge ID")

class AccountResponse(AccountBase):
    """Account response model for API endpoints"""
    
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "login": 123456,
                "account_size": 10000.0,
                "platform": 1,
                "phase": 2,
                "user_id": 789,
                "challenge_id": 101
            }
        }
    )

class AccountCreate(AccountBase):
    """Account creation model (if needed for future endpoints)"""
    pass

class AccountUpdate(BaseModel):
    """Account update model with optional fields"""
    login: Optional[int] = Field(None, description="Account login ID")
    account_size: Optional[float] = Field(None, ge=0, description="Account size/balance")
    platform: Optional[int] = Field(None, description="Trading platform ID")
    phase: Optional[int] = Field(None, ge=0, description="Account phase/level")
    user_id: Optional[int] = Field(None, description="User ID")
    challenge_id: Optional[int] = Field(None, description="Challenge ID")
    
    model_config = ConfigDict(from_attributes=True) 