"""
Trading Indicators Service - Polars expressions for calculating trading metrics
Provides reusable expressions for various trading performance indicators
"""
from typing import List, Optional, Dict, Any
from datetime import (
    datetime,
    timedelta
)

import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from polars import (
    Expr,
    col,
    lit,
    when,
    count,
    read_parquet,
    scan_csv,
    DataFrame
)       
from sqlalchemy import select
from app.enums.action import Action
from app.models.trades_summary import TradesSummaryRecord
from app.services.trade_service import TradeService
from app.services.account_service import create_account_service
from app.db.database import get_sync_db
from os import cpu_count

# Time threshold to consider analysis up-to-date (in minutes)
ANALYSIS_UPTODATE_THRESHOLD_MINUTES = 1

class IndicatorsServiceError(Exception):
    """Custom exception for indicators service errors"""
    pass


class IndicatorsService:
    """Service for managing trading indicators and accessing TradesSummaryRecord data"""
    
    def __init__(self, trade_service: Optional['TradeService'] = None):
        """
        Initialize indicators service with optional trade service
        
        Args:
            trade_service: Optional TradeService instance (will create one if not provided)
        """
        self._trade_service = trade_service
    
    @property
    def trade_service(self) -> 'TradeService':
        """Get or create trade service instance (lazy initialization)"""
        if self._trade_service is None:
            from app.services.trade_service import create_trade_service
            self._trade_service = create_trade_service()
        return self._trade_service
    
    def get_trades_summary_by_login_sync(self, account_login: int) -> Optional[TradesSummaryRecord]:
        """
        Get the most recent trades summary record for an account (thread-safe sync version)
        
        Args:
            account_login: The trading account login ID
            
        Returns:
            The most recent TradesSummaryRecord or None if not found
        """
        try:
            session = get_sync_db()
            try:
                result = session.execute(
                    select(TradesSummaryRecord)
                    .where(TradesSummaryRecord.trading_account_login == account_login)
                    .order_by(TradesSummaryRecord.closed_at.desc())
                    .limit(1)
                )
                return result.scalar_one_or_none()
            finally:
                session.close()
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to get trades summary for login {account_login}: {str(e)}")

    def get_most_recent_trade_timestamp(self, account_login: int) -> Optional[datetime]:
        """
        Get the timestamp of the most recent trade for an account using TradeService
        
        Args:
            account_login: The trading account login ID
            
        Returns:
            The timestamp of the most recent trade or None if no trades found
        """
        try:
            return self.trade_service.get_most_recent_trade_timestamp(account_login)
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to get most recent trade timestamp for login {account_login}: {str(e)}")

    def is_analysis_uptodate(
        self, 
        last_record_timestamp: datetime, 
        latest_trade_timestamp: Optional[datetime],
        threshold_minutes: int = ANALYSIS_UPTODATE_THRESHOLD_MINUTES
    ) -> bool:
        """
        Check if the analysis is up-to-date based on timestamp comparison
        
        Args:
            last_record_timestamp: Timestamp from the last summary record
            latest_trade_timestamp: Timestamp of the most recent trade
            threshold_minutes: Time threshold in minutes to consider analysis up-to-date
            
        Returns:
            True if analysis is up-to-date, False otherwise
        """
        if latest_trade_timestamp is None:
            # If no trades found, consider analysis up-to-date
            return True
            
        time_difference = latest_trade_timestamp - last_record_timestamp
        threshold_delta = timedelta(minutes=threshold_minutes)
        
        return time_difference <= threshold_delta

    def _insert_records_sync(self, records: List[TradesSummaryRecord]) -> int:
        """
        Thread-safe synchronous method to insert records to database
        Assumes records are already filtered for new data only (no duplicates expected)
        
        Args:
            records: List of TradesSummaryRecord objects to insert
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
            
        try:
            session = get_sync_db()
            try:
                # Direct bulk insert - no duplicate checking needed
                # The resumable computation logic should ensure no duplicates
                session.add_all(records)
                session.commit()
                return len(records)
                
            except Exception as e:
                session.rollback()
                # If we get a unique constraint error, it means our resumable logic has a bug
                if "UNIQUE constraint failed" in str(e):
                    raise IndicatorsServiceError(
                        f"Duplicate record detected - this indicates a bug in resumable computation logic. "
                        f"Error: {str(e)}"
                    )
                raise
            finally:
                session.close()
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to insert records: {str(e)}")

    def _get_all_account_logins(self) -> List[int]:
        """
        Get all account logins from the account service
        
        Returns:
            List of all unique account login IDs
        """
        try:
            account_service = create_account_service()
            return account_service.get_all_unique_logins()
        except Exception as e:
            raise IndicatorsServiceError(f"Error getting account logins: {str(e)}")

    def determine_analysis_strategy(self, account_login: int) -> Dict[str, Any]:
        """
        Determine whether to run rolling window analysis from scratch or resume it
        
        Args:
            account_login: The trading account login ID
            
        Returns:
            Dict containing analysis strategy decision with following keys:
            - strategy: "from_scratch" | "resume" | "skip"
            - reason: Human-readable explanation of the decision
            - last_record: TradesSummaryRecord if exists, None otherwise
            - last_trade_timestamp: Timestamp of most recent trade if exists
            - needs_update: Boolean indicating if analysis needs to be run
            - period_seconds: Number of seconds in the analysis period (only for resume strategy)
            - use_batch_strategy: Boolean indicating if batch processing should be used (only for resume/from_scratch)
        """
        try:
            print(f"🔍 Analyzing strategy for account {account_login}...")
            
            # Step 1: Check if we have any existing trades summary records
            last_record = self.get_trades_summary_by_login_sync(account_login)
            
            # Step 2: Get the most recent trade timestamp
            latest_trade_timestamp = self.get_most_recent_trade_timestamp(account_login)
            
            # Step 3: Count number of records for this account
            record_count = self.trade_service.count_trades_for_account(account_login)
            use_batch_strategy = record_count >= 1000
            
            # Step 4: Make decision based on available data
            if last_record is None:
                # No previous analysis exists
                if latest_trade_timestamp is None:
                    # No trades exist for this account
                    return {
                        "strategy": "skip",
                        "reason": "No trades found for this account",
                        "last_record": None,
                        "last_trade_timestamp": None,
                        "needs_update": False
                    }
                else:
                    # Trades exist but no analysis done yet
                    return {
                        "strategy": "from_scratch",
                        "reason": "No previous analysis found, starting fresh analysis",
                        "last_record": None,
                        "last_trade_timestamp": latest_trade_timestamp,
                        "needs_update": True,
                        "use_batch_strategy": use_batch_strategy
                    }
            else:
                # Previous analysis exists
                if latest_trade_timestamp is None:
                    # This shouldn't happen (we have analysis but no trades)
                    # But handle gracefully
                    return {
                        "strategy": "skip",
                        "reason": "Analysis exists but no trades found (data inconsistency)",
                        "last_record": last_record,
                        "last_trade_timestamp": None,
                        "needs_update": False
                    }
                else:
                    # Check if analysis is up-to-date
                    is_uptodate = self.is_analysis_uptodate(
                        last_record.closed_at, 
                        latest_trade_timestamp
                    )
                    
                    # Calculate time difference between window boundaries
                    time_difference = (last_record._upper_boundary - last_record._lower_boundary).total_seconds()
                    
                    if is_uptodate:
                        # Analysis is current, no update needed
                        return {
                            "strategy": "skip",
                            "reason": f"Analysis is up-to-date (last: {last_record.closed_at}, latest trade: {latest_trade_timestamp})",
                            "last_record": last_record,
                            "last_trade_timestamp": latest_trade_timestamp,
                            "needs_update": False
                        }
                    else:
                        # Analysis needs update, resume from last point
                        return {
                            "strategy": "resume",
                            "reason": f"Analysis outdated, resuming from {last_record.closed_at} (latest trade: {latest_trade_timestamp})",
                            "last_record": last_record,
                            "last_trade_timestamp": latest_trade_timestamp,
                            "needs_update": True,
                            "period_seconds": time_difference,  # Use actual period from last record
                            "use_batch_strategy": use_batch_strategy
                        }
                        
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to determine analysis strategy for account {account_login}: {str(e)}")

    def _process_trades_summary_records(self, trades_summary_records: DataFrame, account_login: int) -> None:
        """
        Process and insert trades summary records into the database
        
        Args:
            trades_summary_records: Polars DataFrame containing the summary records
            account_login: The trading account login ID
        """
        if not trades_summary_records.is_empty():
            records_to_insert = []
            for row in trades_summary_records.iter_rows(named=True):
                record = TradesSummaryRecord(
                    _lower_boundary=row["_lower_boundary"],
                    _upper_boundary=row["_upper_boundary"],
                    trading_account_login=account_login,
                    closed_at=row["closed_at"],
                    hft=row["hft"],
                    win_ratio=row["win_ratio"],
                    profit_factor=row["profit_factor"],
                    sl_percent=row["sl_percent"],
                    layered_trade_count=row["layered_trade_count"],
                    last_trade_at=row["last_trade_at"],
                    last_equity=row["last_equity"],
                    last_peak=row["last_peak"],
                    max_relative_drawdown=row["max_relative_drawdown"]
                )
                records_to_insert.append(record)
            self._insert_records_sync(records_to_insert)

    def _process_from_scratch(self, account_login: int, batch_strategy: bool) -> None:
        """
        Process account analysis from scratch
        
        Args:
            account_login: The trading account login ID
            batch_strategy: Whether to use batch processing
        """
        group_by = self.trade_service.create_rolling_window_lazy_frame(
            account_login=account_login,
            period="1m",
            equity=100_000
        )

        if group_by is None:
            print(f"❌ fail to create rolling window lazy frame for account {account_login}")
            return
        
        indicators = get_all_trading_indicators()
        trades_summary_records = group_by.agg(indicators)
        
        if batch_strategy:
            # TODO: Implement batch processing for from_scratch
            pass
        else:
            trades_summary_records = trades_summary_records.collect()
            self._process_trades_summary_records(trades_summary_records, account_login)

    def _process_resume(self, account_login: int, strategy_info: Dict[str, Any], batch_strategy: bool) -> None:
        """
        Resume account analysis from last point
        
        Args:
            account_login: The trading account login ID
            strategy_info: Dictionary containing strategy information
            batch_strategy: Whether to use batch processing
        """
        group_by = self.trade_service.create_rolling_window_lazy_frame(
            account_login=account_login,
            period="1m",
            period_seconds=strategy_info["period_seconds"],
            resume_from_equity=strategy_info["last_record"].last_equity,
            resume_from_peak=strategy_info["last_record"].last_peak,
            resume_from_datetime=strategy_info["last_record"].closed_at
        )

        if group_by is None:
            print(f"❌ fail to create rolling window lazy frame for account {account_login}")
            return
        
        indicators = get_all_trading_indicators()

        trades_summary_records = group_by.agg(indicators)

        if batch_strategy:
            # TODO: Implement batch processing for resume
            pass
        else:
            trades_summary_records = trades_summary_records.collect()
            self._process_trades_summary_records(trades_summary_records, account_login)

    def process_account_by_strategy(self, account_login: int) -> Dict[str, Any]:
        """
        Process an account based on the analysis strategy decision.
        Uses a match statement over the 'strategy' key from determine_analysis_strategy.
        
        Args:
            account_login: The trading account login ID
            
        Returns:
            Dict containing the strategy information and processing status
        """
        try:
            strategy_info = self.determine_analysis_strategy(account_login)
            strategy = strategy_info["strategy"]
            reason = strategy_info["reason"]
            batch_strategy = strategy_info.get("use_batch_strategy", False)

            match strategy:
                case "skip":
                    print(f"[SKIP] Account {account_login}: {reason}")
                case "from_scratch":
                    print(f"[FROM_SCRATCH] Account {account_login}: {reason}")
                    self._process_from_scratch(account_login, batch_strategy)
                case "resume":
                    print(f"[RESUME] Account {account_login}: {reason}")
                    self._process_resume(account_login, strategy_info, batch_strategy)
                case _:
                    print(f"[UNKNOWN STRATEGY] Account {account_login}: {reason}")
                    
            return strategy_info
                    
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to process account {account_login}: {str(e)}")

    def execute_bulk_analysis(
        self,
        account_logins: Optional[List[int]] = None,
        period: str = "1m",
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Execute bulk analysis for multiple accounts in parallel using ThreadPoolExecutor
        
        Args:
            account_logins: List of account logins to process. If None, process all accounts
            period: Time period for analysis (e.g. "1m", "1h", "1d")
            max_workers: Maximum number of worker threads to use
            
        Returns:
            Dict containing analysis results with following keys:
            - processed_accounts: Number of accounts processed
            - skipped_accounts: Number of accounts skipped
            - failed_accounts: Number of accounts that failed processing
            - errors: List of error messages for failed accounts
        """
        try:
            # Get list of accounts to process
            if account_logins is None:
                # Get all account logins from account service
                account_service = create_account_service()
                account_logins = account_service.get_all_unique_logins()[:20]
            
            print(f"🔄 Starting bulk analysis for {len(account_logins)} accounts...")
            
            # Initialize counters and error list
            processed = 0
            skipped = 0
            failed = 0
            errors = []
            
            # Create thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all accounts for processing
                future_to_account = {
                    executor.submit(self.process_account_by_strategy, account_login): account_login
                    for account_login in account_logins
                }
                
                # Process results as they complete
                for future in as_completed(future_to_account):
                    account_login = future_to_account[future]
                    try:
                        # Process the account and get the strategy info
                        strategy_info = future.result()
                        strategy = strategy_info.get("strategy")
                        
                        if strategy == "skip":
                            skipped += 1
                            print(f"⏭️  Skipped account {account_login}: {strategy_info.get('reason', 'No reason provided')}")
                        else:
                            processed += 1
                            print(f"✅ Processed account {account_login}: {strategy_info.get('reason', 'No reason provided')}")
                            
                    except Exception as e:
                        failed += 1
                        error_msg = f"Account {account_login} failed: {str(e)}"
                        errors.append(error_msg)
                        print(f"❌ {error_msg}")
            
            print(f"✅ Bulk analysis completed:")
            print(f"   - Processed: {processed} accounts")
            print(f"   - Skipped: {skipped} accounts")
            print(f"   - Failed: {failed} accounts")
            
            return {
                "processed_accounts": processed,
                "skipped_accounts": skipped,
                "failed_accounts": failed,
                "errors": errors
            }
            
        except Exception as e:
            raise IndicatorsServiceError(f"Failed to execute bulk analysis: {str(e)}")


def create_indicators_service(trade_service: Optional[TradeService] = None) -> IndicatorsService:
    """
    Create and return an IndicatorsService instance
    
    Args:
        trade_service: Optional TradeService instance (will create one if not provided)
        
    Returns:
        IndicatorsService: Configured indicators service instance
        
    Raises:
        IndicatorsServiceError: If service initialization fails
    """
    return IndicatorsService(trade_service)


def get_hft_count_filters() -> List[Expr]:
    """
    Calculate HFT count by counting trades with duration less than 60 seconds
    
    Returns:
        List[pl.Expr]: Polars expressions to calculate HFT count
    """
    return [
        when(
            (col("closed_at") - col("opened_at")).dt.total_seconds() < 60
        ).then(1).otherwise(0).sum().alias("hft")
    ]


def get_win_ratio_filters() -> List[Expr]:
    """
    Calculate the win rate for a given lazy frame
    """
    return [
        (when(col("profit") > 0).then(1).otherwise(0).sum() / count()).alias("win_ratio")
    ]


def get_profit_factor_filters() -> List[Expr]:
    """
    Calculate the profit factor for a given lazy frame
    Handles edge cases: returns 0 if no winning trades or no losing trades
    """
    gross_profit = when(col("profit") > 0).then(col("profit")).otherwise(0).sum()
    gross_loss = when(col("profit") < 0).then(col("profit").abs()).otherwise(0).sum()
    
    return [
        when(gross_profit == 0)
        .then(0)  # No winning trades → profit factor = 0
        .when(gross_loss == 0)
        .then(0)  # No losing trades → return 0 instead of inf
        .otherwise(gross_profit / gross_loss)  # Normal case
        .alias("profit_factor")
    ]


def get_max_relative_drawdown_filter() -> List[Expr]:
    """
    Calculate the equity curve and related metrics for a given lazy frame
    """
    return [
        col("equity").last().alias("last_equity"),
        col("global_peak").last().alias("last_peak"),
        col("global_drawdown").min().abs().round(5).alias("max_relative_drawdown") * 100
    ]


def get_weighted_sl_percent_filters() -> List[Expr]:
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
    position_size = col("lot_size") * col("contract_size") * col("open_price")
    
    # Calculate sl_percent based on whether price_sl is null or not
    # For BUY trades (action = 0): sl% = (open_price - sl_price) / open_price  
    # For SELL trades (action = 1): sl% = (sl_price - open_price) / open_price
    
    sl_percent = (
        when(col("price_sl").is_not_null())
        .then(
            # Use price_sl when available
            when(col("action") == Action.BUY.value)  # BUY 
            .then((col("open_price") - col("price_sl")) / col("open_price"))
            .otherwise((col("price_sl") - col("open_price")) / col("open_price"))  # SELL
        )
        .otherwise(
            # Infer from close_price when price_sl is null
            when(col("action") == Action.BUY.value)  # BUY     
            .then((col("open_price") - col("close_price")) / col("open_price"))
            .otherwise((col("close_price") - col("open_price")) / col("open_price"))  # SELL
        )
    )
    
    # Calculate weighted sl percentage
    weighted_sl_percent = (sl_percent * position_size).sum() / position_size.sum()
    
    return [
        weighted_sl_percent.alias("sl_percent")
    ]


def get_layered_trade_filters() -> List[Expr]:
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
    is_short_duration = (col("closed_at") - col("opened_at")).dt.total_seconds() < 60
    
    return [
        
        # Layered trades = short_trades_total - unique_timestamps  
        # If we have more trades than unique timestamps, the difference is layered trades
        # Handle edge case: if no short trades exist, return 0
        when(
            when(is_short_duration).then(1).otherwise(0).sum() == 0
        )
        .then(0)  # No short trades = no layered trades
        .otherwise(
            when(is_short_duration).then(1).otherwise(0).sum() -     
            when(is_short_duration).then(col("opened_at")).otherwise(None).drop_nulls().n_unique()
        )
        .clip(lower_bound=0)  # Ensure non-negative
        .alias("layered_trade_count")
    ]


def get_last_trade_filters() -> List[Expr]:
    """
    Get the last trade information for each rolling window
    
    Returns expressions to capture the last trade's key metrics
    based on the most recent opened_at timestamp in the window
    """
    return [
        # Last trade's opening time
        col("opened_at").last().alias("last_trade_at"),
    ]


def get_all_trading_indicators() -> List[Expr]:
    """
    Convenience function to get all trading indicator expressions
    
    Returns:
        List[pl.Expr]: All trading indicator expressions combined
    """
    expressions = []
    expressions.extend(get_hft_count_filters())
    expressions.extend(get_win_ratio_filters())
    expressions.extend(get_profit_factor_filters())
    expressions.extend(get_weighted_sl_percent_filters())
    expressions.extend(get_layered_trade_filters())
    expressions.extend(get_last_trade_filters())
    expressions.extend(get_max_relative_drawdown_filter())
    
    return expressions

