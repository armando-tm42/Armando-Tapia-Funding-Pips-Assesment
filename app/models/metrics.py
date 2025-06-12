"""
Metrics SQLAlchemy model
"""
from datetime import date
from sqlalchemy import Column, Integer, Float, Boolean, Date, UniqueConstraint, CheckConstraint
from app.db.database import Base

class Metrics(Base):
    """SQLAlchemy model for metrics data"""
    __tablename__ = "metrics"
    
    id = Column(Integer, primary_key=True)
    login = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    win_ratio = Column(Float, nullable=False)
    profit_factor = Column(Float, nullable=False)
    hft = Column(Integer, nullable=False)
    layering = Column(Integer, nullable=False)
    
    max_relative_drawdown = Column(Float, nullable=False)
    
    # Compound unique constraint to prevent duplicate metrics for same login on same date
    __table_args__ = (
        UniqueConstraint('login', 'date', name='uq_metrics_login_date'),
        # Database-level constraints
        CheckConstraint('win_ratio >= 0 AND win_ratio <= 100', name='check_win_ratio_percentage'),
        CheckConstraint('max_relative_drawdown >= 0 AND max_relative_drawdown <= 100', name='check_max_relative_drawdown_percentage'),
        CheckConstraint('profit_factor > 0', name='check_profit_factor_positive'),
        CheckConstraint('layering >= 0', name='check_layering_non_negative'),
        CheckConstraint('hft >= 0', name='check_hft_non_negative'),
    ) 