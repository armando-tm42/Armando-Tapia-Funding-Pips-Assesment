"""
Trade Service - Uses Polars to read trade data from CSV files
Provides trade data access functionality with data validation
"""
import os
from pathlib import Path
from typing import Optional, List
from datetime import date

import polars as pl
from dotenv import load_dotenv

INITIAL_EQUITY = 100_000

class TradeServiceError(Exception):
    """Custom exception for trade service errors"""
    pass

class TradeService:
    """Service for reading and querying trade data using Polars"""
    
    def __init__(self):
        """Initialize trade service with data validation"""
        self._data_path = self._validate_and_get_data_path()
        self._trades_file = self._validate_trades_file()
    
    def _validate_and_get_data_path(self) -> Path:
        """
        Validate DATA_DIR environment variable and return resolved path
        
        Returns:
            Path: Validated data directory path
            
        Raises:
            TradeServiceError: If DATA_DIR is not set or path doesn't exist
        """
        # Load environment variables
        # Try to find .env file in project root (assuming service is in app/services/)
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent
        env_file = project_root / ".env"
        
        if env_file.exists():
            load_dotenv(env_file)
        
        # Get DATA_DIR from environment
        data_dir = os.getenv("DATA_DIR")
        if not data_dir:
            raise TradeServiceError(
                "DATA_DIR environment variable is not set. "
                "Please add DATA_DIR to your .env file (e.g., DATA_DIR=./data)"
            )
        
        # Resolve DATA_DIR path relative to project root if it's relative
        if data_dir.startswith('./') or not os.path.isabs(data_dir):
            if data_dir.startswith('./'):
                relative_path = data_dir[2:]
            else:
                relative_path = data_dir
            
            data_path = project_root / relative_path
        else:
            data_path = Path(data_dir)
        
        # Convert to absolute path and validate existence
        data_path = data_path.resolve()
        
        if not data_path.exists():
            raise TradeServiceError(
                f"DATA_DIR path does not exist: {data_path} "
                f"(resolved from: {data_dir})"
            )
        
        if not data_path.is_dir():
            raise TradeServiceError(
                f"DATA_DIR is not a directory: {data_path}"
            )
        
        return data_path
    
    def _validate_trades_file(self) -> Path:
        """
        Validate that trades_db.csv exists in the data directory
        
        Returns:
            Path: Path to trades_db.csv file
            
        Raises:
            TradeServiceError: If trades_db.csv doesn't exist
        """
        trades_file = self._data_path / "trades_db.csv"
        
        if not trades_file.exists():
            raise TradeServiceError(
                f"trades_db.csv not found in DATA_DIR: {trades_file}. "
                f"Please ensure the file exists or run preprocessing script first."
            )
        
        if not trades_file.is_file():
            raise TradeServiceError(
                f"trades_db.csv is not a file: {trades_file}"
            )
        
        return trades_file
    
    def get_data_path(self) -> Path:
        """Get the validated data directory path"""
        return self._data_path
    
    def get_trades_file_path(self) -> Path:
        """Get the path to the trades CSV file"""
        return self._trades_file

    def create_rolling_window_lazy_frame(
        self, 
        account_login: int, 
        period: str = "1m",
        start_date: Optional[date] = None
    ) -> Optional[pl.LazyFrame]:
        """
        Create a dynamic group-by lazy frame for trades using Polars

        Args:
            account_login: MT login
            period: time-based grouping window (e.g. "1h", "1d")
            start_date: Optional filter for start date

        Returns:
            pl.LazyFrame with dynamic group_by ready to be aggregated
        """
        try:
            lazy_frame = pl.scan_csv(self._trades_file)

            #convert datetime columns to datetime
            lazy_frame = lazy_frame.with_columns([
                pl.col("opened_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
                pl.col("closed_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f")
            ])

            lazy_frame = lazy_frame.filter(
                pl.col("trading_account_login") == account_login
            )

            #compute equity curve
            lazy_frame = lazy_frame.with_columns([
                (pl.lit(INITIAL_EQUITY) + pl.col("profit").cum_sum() + pl.col("swap").cum_sum() + pl.col("commission").cum_sum()).alias("equity")
            ])

            #compute peak equity
            lazy_frame = lazy_frame.with_columns([
                pl.col("equity").cum_max().alias("peak")
            ])

            #compute relative drawdown
            lazy_frame = lazy_frame.with_columns([
                ((pl.col("equity") - pl.col('peak')) / pl.col('peak')).alias("relative_drawdown")
            ])

            if start_date:
                lazy_frame = lazy_frame.filter(
                    pl.col("opened_at") >= start_date.isoformat()
                )

            # Crear una ventana dinámica (rolling tipo bucket)
            lazy_window = lazy_frame.group_by_dynamic(
                index_column="closed_at",
                every="1m",
                period=period,
                include_boundaries=True,
                closed="left"
            )

            return lazy_window

        except Exception as e:
            print(f"Error creating rolling window lazy frame: {e}")
            return None
    
def get_hft_count_filters() -> List[pl.Expr]:
    """
    Calculate HFT count by counting trades with duration less than 60 seconds
    
    Returns:
        List[pl.Expr]: Polars expressions to calculate HFT count
    """
    return [
        pl.when(
            (pl.col("closed_at") - pl.col("opened_at")).dt.total_seconds() < 60
        ).then(1).otherwise(0).sum().alias("hft")
    ]

def get_win_ratio_filters() -> List[pl.Expr]:
    """
    Calculate the win rate for a given lazy frame
    """
    return [
        (pl.when(pl.col("profit") > 0).then(1).otherwise(0).sum() / pl.count()).alias("win_ratio")
    ]

def get_profit_factor_filters() -> List[pl.Expr]:
    """
    Calculate the profit factor for a given lazy frame
    Handles edge cases: returns 0 if no winning trades or no losing trades
    """
    gross_profit = pl.when(pl.col("profit") > 0).then(pl.col("profit")).otherwise(0).sum()
    gross_loss = pl.when(pl.col("profit") < 0).then(pl.col("profit").abs()).otherwise(0).sum()
    
    return [
        pl.when(gross_profit == 0)
        .then(0)  # No winning trades → profit factor = 0
        .when(gross_loss == 0)
        .then(0)  # No losing trades → return 0 instead of inf
        .otherwise(gross_profit / gross_loss)  # Normal case
        .alias("profit_factor")
    ]

def get_max_relative_drawdown_filter() -> List[pl.Expr]:
    """
    Calculate the equity curve for a given lazy frame
    """
    return [
        pl.col("commission").sum().alias("total_commission"),
        pl.col("swap").sum().alias("total_swap"),
        pl.col("profit").sum().alias("total_profit"),
        pl.col("relative_drawdown").min().alias("max_relative_drawdown"),
        pl.col("identifier").count().alias("total_trades")
    ]

def get_weighted_sl_percent_filters() -> List[pl.Expr]:
    """
    Calculate the weighted stop loss percentage based on position size
    
    Logic:
    - If price_sl is not null: compute sl% using open_price and price_sl
    - If price_sl is null: infer sl% using open_price and close_price
    - Position size = lot_size * contract_size * open_price
    - Action: 0 = BUY, 1 = SELL
    - Weighted sl% = sum(sl_percent * position_size) / sum(position_size)
    """
    
    # Calculate position size
    position_size = pl.col("lot_size") * pl.col("contract_size") * pl.col("open_price")
    
    # Calculate sl_percent based on whether price_sl is null or not
    # For BUY trades (action = 0): sl% = (open_price - sl_price) / open_price  
    # For SELL trades (action = 1): sl% = (sl_price - open_price) / open_price
    
    sl_percent = (
        pl.when(pl.col("price_sl").is_not_null())
        .then(
            # Use price_sl when available
            pl.when(pl.col("action") == 0)  # BUY
            .then((pl.col("open_price") - pl.col("price_sl")) / pl.col("open_price"))
            .otherwise((pl.col("price_sl") - pl.col("open_price")) / pl.col("open_price"))  # SELL
        )
        .otherwise(
            # Infer from close_price when price_sl is null
            pl.when(pl.col("action") == 0)  # BUY
            .then((pl.col("open_price") - pl.col("close_price")) / pl.col("open_price"))
            .otherwise((pl.col("close_price") - pl.col("open_price")) / pl.col("open_price"))  # SELL
        )
    )
    
    # Calculate weighted sl percentage
    weighted_sl_percent = (sl_percent * position_size).sum() / position_size.sum()
    
    return [
        weighted_sl_percent.alias("sl_percent")
    ]

def get_layered_trade_filters() -> List[pl.Expr]:
    """
    Calculate layered trade count for trades that:
    1. Have duration < 60 seconds  
    2. Open at the exact same timestamp (grouped by opened_at)
    3. Count only trades where multiple trades happen at same time
    
    Layered trades are short-duration trades that happen simultaneously,
    indicating potential high-frequency trading strategies.
    
    Approach: Create a helper column with opened_at only for short trades,
    then count unique timestamps and compare with total short trades.
    """
    # Define the condition for short trades (< 60 seconds duration)
    is_short_duration = (pl.col("closed_at") - pl.col("opened_at")).dt.total_seconds() < 60
    
    return [
        # Total short trades in window
        pl.when(is_short_duration)
        .then(1)
        .otherwise(0)
        .sum()
        .alias("short_trades_total"),
        
        # Unique opened_at timestamps for short trades
        pl.when(is_short_duration)
        .then(pl.col("opened_at"))
        .otherwise(None)
        .n_unique()
        .alias("unique_short_timestamps"),
        
        # Layered trades = short_trades_total - unique_timestamps  
        # If we have more trades than unique timestamps, the difference is layered trades
        # Handle edge case: if no short trades exist, return 0
        pl.when(
            pl.when(is_short_duration).then(1).otherwise(0).sum() == 0
        )
        .then(0)  # No short trades = no layered trades
        .otherwise(
            pl.when(is_short_duration).then(1).otherwise(0).sum() - 
            pl.when(is_short_duration).then(pl.col("opened_at")).otherwise(None).drop_nulls().n_unique()
        )
        .clip(lower_bound=0)  # Ensure non-negative
        .alias("layered_trade_count")
    ]

def get_last_trade_filters() -> List[pl.Expr]:
    """
    Get the last trade information for each rolling window
    
    Returns expressions to capture the last trade's key metrics
    based on the most recent opened_at timestamp in the window
    """
    return [
        # Last trade's opening time
        pl.col("opened_at").last().alias("last_trade_at"),
        
    ]

def create_trade_service() -> TradeService:
    """
    Create and return a TradeService instance
    
    Returns:
        TradeService: Configured trade service instance
        
    Raises:
        TradeServiceError: If service initialization fails
    """
    return TradeService() 