"""
Background task for indicators analysis
"""
from app.services.indicators_service import create_indicators_service

def run_bulk_indicators_analysis() -> None:
    """
    Background task to run bulk indicators analysis for all accounts
    This function will be executed every 5 minutes by the scheduler
    """
    try:
        # Create indicators service
        indicators_service = create_indicators_service()
        
        # Execute bulk analysis for all accounts
        result = indicators_service.execute_bulk_analysis(
            account_logins=None,  # Process all accounts
            period="1m",
            max_workers=4
        )
        
    except Exception as e:
        # Silently handle errors for now
        pass 