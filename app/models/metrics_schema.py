"""
Pydantic schemas for metrics validation
"""
from datetime import date as Date
from typing import Optional
from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    field_validator,
)

class MetricsBase(BaseModel):
    """Base metrics schema with shared attributes"""
    login: int = Field(..., description="Trading account login")
    date: Date = Field(..., description="Date of the metrics")
    win_ratio: float = Field(
        ..., 
        ge=0, 
        le=100, 
        description="Win ratio as percentage (0-100)"
    )
    profit_factor: float = Field(
        ..., 
        gt=0, 
        description="Profit factor (must be positive)"
    )
    hft: int = Field(
        ..., 
        ge=0,
        description="High frequency trading count (non-negative)"
    )
    layering: int = Field(
        ..., 
        ge=0, 
        description="Layering count (non-negative)"
    )
    max_relative_drawdown: float = Field(
        ..., 
        ge=0, 
        le=100, 
        description="Maximum relative drawdown as percentage (0-100)"
    )

    @field_validator('win_ratio', 'max_relative_drawdown')
    @classmethod
    def validate_percentage(cls, v):
        """Validate that percentage values are between 0 and 100"""
        if not (0 <= v <= 100):
            raise ValueError('Percentage values must be between 0 and 100')
        return v

    @field_validator('profit_factor')
    @classmethod
    def validate_profit_factor(cls, v):
        """Validate that profit factor is positive"""
        if v <= 0:
            raise ValueError('Profit factor must be positive')
        return v

    @field_validator('layering')
    @classmethod
    def validate_layering(cls, v):
        """Validate that layering is non-negative"""
        if v < 0:
            raise ValueError('Layering must be non-negative')
        return v

    @field_validator('hft')
    @classmethod
    def validate_hft(cls, v):
        """Validate that hft is non-negative"""
        if v < 0:
            raise ValueError('HFT count must be non-negative')
        return v

class MetricsCreate(MetricsBase):
    """Schema for creating metrics"""
    pass

class MetricsUpdate(BaseModel):
    """Schema for updating metrics (all fields optional)"""
    login: Optional[int] = Field(None, description="Trading account login")
    date: Optional[Date] = Field(None, description="Date of the metrics")
    win_ratio: Optional[float] = Field(
        None, 
        ge=0, 
        le=100, 
        description="Win ratio as percentage (0-100)"
    )
    profit_factor: Optional[float] = Field(
        None, 
        gt=0, 
        description="Profit factor (must be positive)"
    )
    hft: Optional[int] = Field(
        None, 
        ge=0,
        description="High frequency trading count (non-negative)"
    )
    layering: Optional[int] = Field(
        None, 
        ge=0, 
        description="Layering count (non-negative)"
    )
    max_relative_drawdown: Optional[float] = Field(
        None, 
        ge=0, 
        le=100, 
        description="Maximum relative drawdown as percentage (0-100)"
    )

    @field_validator('win_ratio', 'max_relative_drawdown')
    @classmethod
    def validate_percentage(cls, v):
        """Validate that percentage values are between 0 and 100"""
        if v is not None and not (0 <= v <= 100):
            raise ValueError('Percentage values must be between 0 and 100')
        return v

    @field_validator('profit_factor')
    @classmethod
    def validate_profit_factor(cls, v):
        """Validate that profit factor is positive"""
        if v is not None and v <= 0:
            raise ValueError('Profit factor must be positive')
        return v

    @field_validator('layering')
    @classmethod
    def validate_layering(cls, v):
        """Validate that layering is non-negative"""
        if v is not None and v < 0:
            raise ValueError('Layering must be non-negative')
        return v

    @field_validator('hft')
    @classmethod
    def validate_hft(cls, v):
        """Validate that hft is non-negative"""
        if v is not None and v < 0:
            raise ValueError('HFT count must be non-negative')
        return v

class MetricsResponse(MetricsBase):
    """Schema for metrics response"""
    id: int
    
    model_config = ConfigDict(from_attributes=True) 