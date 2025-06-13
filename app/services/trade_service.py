"""
Trade Service - Uses Polars to read trade data from CSV files
Provides trade data access functionality with data validation
"""
import os
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

import polars as pl
from polars.lazyframe.group_by import LazyGroupBy
from polars import LazyFrame, col, when, lit
from dotenv import load_dotenv

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

    def compute_max_relative_drawdown(
        self,
        lazy_frame: LazyFrame, 
        initial_equity: float,
        starting_equity: Optional[float] = None,
        starting_peak: Optional[float] = None
    ) -> LazyFrame:
        """
        Compute the global max relative drawdown for a given lazy frame.
        The result will have 'equity', 'global_peak', and 'global_drawdown' columns.
        """
        # Determine starting values
        base_equity = starting_equity if starting_equity is not None else initial_equity
        base_peak = starting_peak if starting_peak is not None else initial_equity

        # Compute equity curve (incremental from base_equity)
        lazy_frame = lazy_frame.with_columns([
            (lit(base_equity) + col("profit").cum_sum() + col("swap").cum_sum() + col("commission").cum_sum()).alias("equity")
        ])

        # Compute global peak equity (cumulative max)
        lazy_frame = lazy_frame.with_columns([
            col("equity").cum_max().clip(lower_bound=base_peak).alias("global_peak")
        ])

        # Compute global relative drawdown
        lazy_frame = lazy_frame.with_columns([
            ((col("equity") - col('global_peak')) / col('global_peak')).alias("global_drawdown")
        ])

        return lazy_frame

    def get_most_recent_trade_timestamp(self, account_login: int) -> Optional[datetime]:
        """
        Get the most recent trade timestamp (closed_at) for a specific account login
        
        Args:
            account_login: The trading account login ID to filter by
            
        Returns:
            The most recent closed_at timestamp for the account, or None if no trades found
            
        Raises:
            TradeServiceError: If there's an error reading or processing the trades data
        """
        try:
            # Create lazy frame and filter by account login
            lazy_frame = pl.scan_csv(self._trades_file)
            
            # Convert closed_at to datetime and filter by account
            result = (
                lazy_frame
                .with_columns([
                    pl.col("closed_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f")
                ])
                .filter(pl.col("trading_account_login") == account_login)
                .select(pl.col("closed_at").max().alias("latest_trade"))
                .collect()
            )
            
            # Extract the result
            if result.is_empty() or result["latest_trade"][0] is None:
                return None
                
            return result["latest_trade"][0]
            
        except Exception as e:
            raise TradeServiceError(f"Error getting most recent trade timestamp for login {account_login}: {str(e)}")

    def create_rolling_window_lazy_frame(
        self, 
        account_login: int, 
        period: str = "1m",
        period_seconds: int = 60,
        equity: float = 100_000,
        resume_from_equity: Optional[float] = None,
        resume_from_peak: Optional[float] = None,
        resume_from_datetime: Optional[datetime] = None
    ) -> Optional[LazyGroupBy]: 
        """
        Create a dynamic group-by lazy frame for trades using Polars
        Can resume computation from a previous state for incremental processing

        Args:
            account_login: MT login
            period: time-based grouping window (e.g. "1h", "1d")
            equity: Initial equity for fresh calculations
            resume_from_equity: Last known equity value (for resuming)
            resume_from_peak: Last known peak equity value (for resuming)
            resume_from_datetime: Last computed window datetime (for filtering new trades)

        Returns:
            LazyGroupBy with dynamic group_by ready to be aggregated
        """
        try:
            lazy_frame = pl.scan_csv(self._trades_file)

            #convert datetime columns to datetime
            lazy_frame = lazy_frame.with_columns([
                pl.col("opened_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
                pl.col("closed_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f")
            ])

            #filter by account login
            lazy_frame = lazy_frame.filter(
                pl.col("trading_account_login") == account_login
            )

            # Filter for incremental processing if resuming from previous state
            if resume_from_datetime:

                next_window_start = resume_from_datetime + timedelta(seconds=period_seconds)
                
                lazy_frame = lazy_frame.filter(
                    pl.col("closed_at") >= next_window_start
                )


            #compute max relative drawdown (with optional resumable state)
            lazy_frame = self.compute_max_relative_drawdown(
                lazy_frame, 
                equity,
                starting_equity=resume_from_equity,
                starting_peak=resume_from_peak
            )

            # Create a rolling window by using group_by_dynamic
            lazy_window = lazy_frame.group_by_dynamic(
                index_column="closed_at",
                every="1m",
                period="1m",
                include_boundaries=True,
                closed="left"
            )

            return lazy_window

        except Exception as e:
            print(f"Error creating rolling window lazy frame: {e}")
            return None
    
    def count_trades_for_account(self, account_login: int, closed_at: Optional[datetime] = None) -> int:
        """
        Count the number of trade records for a given account login, optionally filtered by closed_at (datetime).
        If closed_at is provided, only count records with closed_at strictly greater than the given value.
        Uses Polars lazy API for efficient streaming/counting.

        Args:
            account_login: The trading account login ID to filter by
            closed_at: Optional closed_at value to filter by (must be a datetime)

        Returns:
            Number of matching records (int)
        """
        try:
            lazy_frame = pl.scan_csv(self._trades_file)
            lf = lazy_frame.filter(pl.col("trading_account_login") == account_login)
            if closed_at is not None:
                # Parse closed_at column as datetime for comparison
                lf = lf.with_columns([
                    pl.col("closed_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f").alias("closed_at_dt")
                ])
                lf = lf.filter(pl.col("closed_at_dt") > closed_at)
            result = lf.select(pl.count()).collect()
            return int(result[0, 0])
        except Exception as e:
            raise TradeServiceError(f"Failed to count trades for account {account_login}: {str(e)}")

def create_trade_service() -> TradeService:
    """
    Create and return a TradeService instance
    
    Returns:
        TradeService: Configured trade service instance
        
    Raises:
        TradeServiceError: If service initialization fails
    """
    return TradeService() 