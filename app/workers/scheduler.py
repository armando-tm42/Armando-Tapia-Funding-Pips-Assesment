"""
Scheduler configuration and management for background tasks
"""
from apscheduler.schedulers.background import BackgroundScheduler

from app.workers.tasks.indicators_task import run_bulk_indicators_analysis

def create_scheduler() -> BackgroundScheduler:
    """Create and configure the scheduler with all background jobs"""
    scheduler = BackgroundScheduler()
    
    # Add indicators analysis job - runs every 5 minutes
    scheduler.add_job(
        run_bulk_indicators_analysis,
        'interval',
        minutes=1,
        id='bulk_indicators_analysis',
        name='Bulk Indicators Analysis'
    )
    
    return scheduler

def start_scheduler(scheduler: BackgroundScheduler) -> None:
    """Start the provided scheduler"""
    scheduler.start()

def shutdown_scheduler(scheduler: BackgroundScheduler) -> None:
    """Shutdown the provided scheduler gracefully"""
    scheduler.shutdown() 