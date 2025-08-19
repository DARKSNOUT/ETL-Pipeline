
import uuid
import time
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.core.logging import get_logger
from app.services import etl
from app.core.config_manager import get_config, save_config
# --- Import the mssql service to execute the procedure ---
from app.db import mssql

logger = get_logger(__name__)
SCHEDULED_JOB_ID = "full_sync_job" # A unique ID for our scheduled job

# --- NEW: Add path to the refresh.sql file ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    REFRESH_SQL_FILE = PROJECT_ROOT / "app" / "query" / "refresh.sql"
except NameError:
    PROJECT_ROOT = Path.cwd()
    REFRESH_SQL_FILE = PROJECT_ROOT / "app" / "query" / "refresh.sql"

def scheduled_full_sync_cycle():
    logger.info("Scheduler is starting a new full cycle...")

    # --- Step 1: Execute the stored procedure from the .sql file ---
    try:
        logger.info(f"Loading data refresh command from {REFRESH_SQL_FILE}...")
        refresh_command = REFRESH_SQL_FILE.read_text()
        
        if not refresh_command.strip():
            logger.warning("The refresh.sql file is empty. Skipping data refresh step.")
        else:
            logger.info(f"Executing source data refresh procedure: '{refresh_command.strip()}'...")
            refresh_success = mssql.execute_raw_sql(refresh_command)
            
            if not refresh_success:
                logger.error("Failed to execute data refresh procedure. Aborting this sync cycle.")
                # We stop here to prevent syncing potentially stale data
                return
            
    except FileNotFoundError:
        logger.error(f"Refresh SQL file not found at {REFRESH_SQL_FILE}. Aborting this sync cycle.")
        return
    except Exception as e:
        logger.critical(f"An unexpected error occurred while trying to refresh data: {e}", exc_info=True)
        return


    # --- Step 2: Wait for 10 seconds ---
    logger.info("Data refresh procedure executed. Waiting for 10 seconds for changes to propagate...")
    time.sleep(10)
    
    try:
        task_id = str(uuid.uuid4())
        etl.run_full_etl_sync(task_id=task_id)
        logger.info(f"Scheduled FULL sync cycle for task {task_id} has completed successfully.")
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred in the scheduled FULL sync cycle: {e}", exc_info=True)


# Create an instance of the scheduler
scheduler = AsyncIOScheduler()

def reschedule_job(new_interval_minutes: int):
    """Removes the existing job and adds a new one with the updated interval."""
    logger.info(f"Attempting to reschedule job with new interval: {new_interval_minutes} minutes.")
    try:
        if scheduler.get_job(SCHEDULED_JOB_ID):
            scheduler.remove_job(SCHEDULED_JOB_ID)
            logger.info(f"Removed existing job with ID: {SCHEDULED_JOB_ID}")
        
        scheduler.add_job(
            scheduled_full_sync_cycle,
            trigger='interval',
            minutes=new_interval_minutes,
            id=SCHEDULED_JOB_ID
        )
        logger.info(f"Successfully rescheduled job '{SCHEDULED_JOB_ID}' to run every {new_interval_minutes} minutes.")
        
        current_config = get_config()
        current_config['scheduler']['interval_minutes'] = new_interval_minutes
        save_config(current_config)

    except Exception as e:
        logger.error(f"Failed to reschedule job: {e}", exc_info=True)

# --- Initial Scheduling on Application Startup ---
initial_config = get_config()
initial_interval = initial_config.get("scheduler", {}).get("interval_minutes", 60)

scheduler.add_job(
    scheduled_full_sync_cycle,
    trigger='interval',
    minutes=initial_interval,
    id=SCHEDULED_JOB_ID
)

logger.info(f"Scheduler initially configured to run the REFRESH & SYNC cycle every {initial_interval} minutes.")
