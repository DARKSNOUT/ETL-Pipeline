# app/api/v1/endpoints.py
import uuid
import json
from pathlib import Path
from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.core.logging import get_logger
from app.services import etl

logger = get_logger(__name__)
router = APIRouter()

# --- Path to the single log file ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"
except NameError:
    PROJECT_ROOT = Path.cwd()
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"


@router.post("/trigger-etl", status_code=202)
def trigger_etl_pipeline(background_tasks: BackgroundTasks):
    """Triggers the ETL pipeline and returns a unique task_id."""
    logger.info("Received request to trigger ETL pipeline.")
    try:
        task_id = str(uuid.uuid4())
        background_tasks.add_task(etl.run_etl_pipeline, task_id=task_id)
        
        return {
            "message": "ETL pipeline triggered successfully.",
            "task_id": task_id
        }
    except Exception as e:
        logger.critical(f"Failed to schedule ETL pipeline task: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to trigger ETL pipeline.")


@router.get("/etl-status/latest")
def get_latest_etl_status():
    """Retrieves the status and result of the most recently logged ETL task."""
    logger.info("Checking status for the latest ETL task.")
    try:
        if not TASKS_LOG_FILE.is_file():
            raise HTTPException(status_code=404, detail="No tasks have been run yet.")
            
        with open(TASKS_LOG_FILE, 'r') as f:
            log_data = json.load(f)
        
        tasks = log_data.get('tasks')
        if not tasks:
            raise HTTPException(status_code=404, detail="Task log is empty.")
            
        latest_task_id = list(tasks.keys())[-1]
        latest_task_info = tasks[latest_task_id]
        
        return { "latest_task_id": latest_task_id, **latest_task_info }

    except json.JSONDecodeError:
        logger.error("Could not parse tasks_log.json")
        raise HTTPException(status_code=500, detail="Failed to read status file.")
    except (KeyError, IndexError):
        raise HTTPException(status_code=404, detail="Could not find any tasks in the log.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking latest status: {e}")
        raise HTTPException(status_code=500, detail="An internal error occurred.")
