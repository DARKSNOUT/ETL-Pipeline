# app/core/scheduler.py
import uuid
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.core.logging import get_logger
from app.services import etl
# --- Import the new config manager ---
from app.core.config_manager import get_config, save_config

logger = get_logger(__name__)
SCHEDULED_JOB_ID = "full_sync_job" # A unique ID for our scheduled job

def scheduled_full_sync_cycle():
    """This function is called by the scheduler to run the full sync."""
    logger.info("Scheduler is starting a new synchronous FULL sync cycle...")
    try:
        task_id = str(uuid.uuid4())
        etl.run_full_etl_sync(task_id=task_id)
        logger.info(f"Scheduled FULL sync cycle for task {task_id} has completed successfully.")
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred in the scheduled FULL sync cycle: {e}", exc_info=True)

# Create an instance of the scheduler
scheduler = AsyncIOScheduler()

def reschedule_job(new_interval_minutes: int):
    """
    Removes the existing job and adds a new one with the updated interval.
    """
    logger.info(f"Attempting to reschedule job with new interval: {new_interval_minutes} minutes.")
    try:
        # First, remove the existing job if it's there
        if scheduler.get_job(SCHEDULED_JOB_ID):
            scheduler.remove_job(SCHEDULED_JOB_ID)
            logger.info(f"Removed existing job with ID: {SCHEDULED_JOB_ID}")
        
        # Add the job back with the new interval
        scheduler.add_job(
            scheduled_full_sync_cycle,
            trigger='interval',
            minutes=new_interval_minutes,
            id=SCHEDULED_JOB_ID # Assign a static ID so we can find it later
        )
        logger.info(f"Successfully rescheduled job '{SCHEDULED_JOB_ID}' to run every {new_interval_minutes} minutes.")
        
        # Also save this new interval to the config file so it persists after a restart
        current_config = get_config()
        current_config['scheduler']['interval_minutes'] = new_interval_minutes
        save_config(current_config)

    except Exception as e:
        logger.error(f"Failed to reschedule job: {e}", exc_info=True)

# --- Initial Scheduling on Application Startup ---
# Get the initial interval from the config file
initial_config = get_config()
initial_interval = initial_config.get("scheduler", {}).get("interval_minutes", 60)

# Add the job for the first time
scheduler.add_job(
    scheduled_full_sync_cycle,
    trigger='interval',
    minutes=initial_interval,
    id=SCHEDULED_JOB_ID
)

logger.info(f"Scheduler initially configured to run FULL ETL sync every {initial_interval} minutes.")
