from sys import path
path.append(".")

from app.services.indicators_service import create_indicators_service, get_all_trading_indicators, get_max_relative_drawdown_filter
from app.services.trade_service import create_trade_service

if __name__ == "__main__":

    # indicators_service = create_indicators_service()

    # indicators_service.execute_bulk_analysis()

    trade_service = create_trade_service()

    group_by = trade_service.create_rolling_window_lazy_frame(
        account_login=100716975,
        period="1m",
        equity=100_000
    )

    indicators = get_all_trading_indicators()
    


    trades_summary_records = group_by.agg(get_max_relative_drawdown_filter()).collect()

    trades_summary_records.write_csv("trades_summary_records.csv")