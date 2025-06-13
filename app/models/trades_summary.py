"""
Indicator SQLAlchemy model for storing trading indicators and metrics
"""

from sqlalchemy import Column, Integer, Float, DateTime, Index, PrimaryKeyConstraint
from app.models import Base


class TradesSummaryRecord(Base):
    """
    SQLAlchemy model for trading indicator data
    
    Stores aggregated trading metrics calculated from rolling windows.
    Uses compound primary key (trading_account_login, closed_at) to ensure
    unique indicators per account per time window.
    """
    __tablename__ = "trades_summary"
    
    # Windowing parameters
    _lower_boundary = Column(DateTime, nullable=False, doc="Window lower boundary")
    _upper_boundary = Column(DateTime, nullable=False, doc="Window upper boundary")

    # Core identification columns
    trading_account_login = Column(Integer, nullable=False, doc="Trading account login ID")
    closed_at = Column(DateTime, nullable=False, doc="Window closing timestamp")
    
    # Trading performance metrics
    hft = Column(Integer, nullable=False, default=0, doc="High frequency trading count")
    win_ratio = Column(Float, nullable=False, default=0.0, doc="Win ratio (0-1)")
    profit_factor = Column(Float, nullable=False, default=0.0, doc="Profit factor")
    sl_percent = Column(Float, nullable=False, default=0.0, doc="Stop loss percentage")
    
    # Layered trading analysis
    layered_trade_count = Column(Integer, nullable=False, default=0, doc="Layered trade count")
    
    # Time and equity tracking
    last_trade_at = Column(DateTime, nullable=False, doc="Last trade timestamp in window")
    last_equity = Column(Float, nullable=False, default=0.0, doc="Last equity value")
    last_peak = Column(Float, nullable=False, default=0.0, doc="Last peak equity value")
    max_relative_drawdown = Column(Float, nullable=False, default=0.0, doc="Maximum relative drawdown")
    
    # Table constraints and indexes
    __table_args__ = (
        # Compound primary key
        PrimaryKeyConstraint('trading_account_login', 'closed_at', name='pk_indicators'),
        
        # Index on trading_account_login for fast lookups
        Index('idx_indicators_account_login', 'trading_account_login'),
        
        # Index on last_trade_at for sorting (since table will be sorted by this column)
        Index('idx_indicators_last_trade_at', 'last_trade_at'),
        
        # Composite index for common queries (account + time range)
        Index('idx_indicators_account_closed_at', 'trading_account_login', 'closed_at'),
        
        # Index for time-based queries
        Index('idx_indicators_closed_at', 'closed_at'),
        
        # Set default sort order by last_trade_at
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'},
    )
    
    def __repr__(self) -> str:
        """String representation of the Indicator model"""
        return (
            f"<Indicator(account={self.trading_account_login}, "
            f"closed_at={self.closed_at}, "
            f"win_ratio={self.win_ratio:.4f}, "
            f"profit_factor={self.profit_factor:.4f})>"
        )
    
    def to_dict(self) -> dict:
        """Convert model instance to dictionary"""
        return {
            'trading_account_login': self.trading_account_login,
            'closed_at': self.closed_at,
            'hft': self.hft,
            'win_ratio': self.win_ratio,
            'profit_factor': self.profit_factor,
            'sl_percent': self.sl_percent,
            'short_trades_total': self.short_trades_total,
            'unique_short_timestamps': self.unique_short_timestamps,
            'layered_trade_count': self.layered_trade_count,
            'last_trade_at': self.last_trade_at,
            'last_equity': self.last_equity,
            'last_peak': self.last_peak,
            'max_relative_drawdown': self.max_relative_drawdown,
        } 