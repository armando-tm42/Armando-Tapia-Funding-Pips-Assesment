from sys import path
path.append("..")

from app.services.trade_service import TradeService, get_profit_factor_filters, get_win_ratio_filters, get_hft_count_filters, get_weighted_sl_percent_filters, get_layered_trade_filters, get_max_relative_drawdown_filter
import polars as pl

if __name__ == "__main__":
    trade_service = TradeService()
    lazy_frame: pl.LazyFrame = trade_service.create_rolling_window_lazy_frame(account_login=10486272, period="1m")
    hft_count_filters = get_hft_count_filters()
    win_ratio_filters = get_win_ratio_filters()
    profit_factor_filters = get_profit_factor_filters()
    weighted_sl_percent_filters = get_weighted_sl_percent_filters()
    layered_trade_filters = get_layered_trade_filters()
    max_relative_drawdown_filter = get_max_relative_drawdown_filter()
    lazy_frame = lazy_frame.agg(
        hft_count_filters + 
        win_ratio_filters + 
        profit_factor_filters + 
        weighted_sl_percent_filters + 
        layered_trade_filters +
        max_relative_drawdown_filter
    )
    
    df = lazy_frame.collect()

    df.write_csv("test.csv")