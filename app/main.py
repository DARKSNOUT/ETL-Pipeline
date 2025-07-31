# main.py
import uvicorn
import uuid
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
# --- Add FileResponse to the imports ---
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from contextlib import asynccontextmanager
from pathlib import Path

# --- Add these imports for the scheduler and config ---
from app.core.scheduler import scheduler, reschedule_job
from app.core.logging import get_logger
from app.services import etl
from app.core.config_manager import get_config, save_config

# Updated import to be more direct
from app.api.v1.endpoints import router as api_v1_router


# Initialize a logger for startup messages
logger = get_logger(__name__)


# --- Lifespan function to manage the scheduler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on application startup
    logger.info("Application startup...")
    try:
        scheduler.start()
        logger.info("Scheduler started successfully.")
    except Exception as e:
        logger.critical(f"Failed to start the scheduler: {e}", exc_info=True)
    
    yield # The application is now running
    
    # Code to run on application shutdown
    logger.info("Application shutdown...")
    try:
        scheduler.shutdown()
        logger.info("Scheduler shut down successfully.")
    except Exception as e:
        logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)


# --- Update the FastAPI instance to use the lifespan function ---
app = FastAPI(
    title="ETL Pipeline API",
    description="API to trigger and monitor the data pipeline.",
    version="1.0.0",
    lifespan=lifespan
)

# --- Include existing API routers ---
app.include_router(api_v1_router, prefix="/api/v1", tags=["ETL"])

# --- UPDATED: Configuration Panel Endpoint ---

@app.get("/config", response_class=HTMLResponse, tags=["Configuration"])
async def get_config_page():
    """Serves the HTML page for the configuration panel using a robust FileResponse."""
    # Build the absolute path to the config.html file
    html_file_path = Path(__file__).parent / "config.html"
    
    if not html_file_path.is_file():
        logger.error(f"Configuration HTML file not found at path: {html_file_path}")
        return HTMLResponse(
            content="<h1>Error: config.html not found</h1><p>Please ensure the file exists in the project root directory.</p>",
            status_code=404
        )
    
    # Use FileResponse for a more reliable way to serve the static file
    return FileResponse(html_file_path)

@app.get("/api/v1/config", tags=["Configuration"])
def get_current_config():
    """API endpoint to get the current application configuration."""
    logger.info("Fetching current configuration.")
    return JSONResponse(content=get_config())

@app.post("/api/v1/config", tags=["Configuration"])
async def update_config(request: Request):
    """API endpoint to update the application configuration."""
    try:
        new_config_data = await request.json()
        logger.info(f"Received request to update configuration with: {new_config_data}")
        
        save_config(new_config_data)
        
        new_interval = int(new_config_data.get("scheduler", {}).get("interval_minutes", 60))
        reschedule_job(new_interval)
        
        return JSONResponse(content={"message": "Configuration updated and scheduler rescheduled successfully."})
        
    except Exception as e:
        logger.error(f"Failed to update configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update configuration.")


# --- Existing Endpoints ---

@app.post("/api/v1/trigger-full-sync", status_code=202, tags=["ETL Full Sync"])
def trigger_full_sync(background_tasks: BackgroundTasks):
    """Triggers a continuous ETL sync that runs in a loop until no new data is found."""
    logger.info("Received request to trigger FULL ETL sync.")
    try:
        task_id = str(uuid.uuid4())
        background_tasks.add_task(etl.run_full_etl_sync, task_id=task_id)
        return { "message": "Full ETL sync triggered successfully.", "task_id": task_id }
    except Exception as e:
        logger.critical(f"Failed to schedule FULL ETL sync task: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to trigger full ETL sync.")

@app.get("/", tags=["Health Check"])
def read_root():
    """A simple health check endpoint to confirm the API is running."""
    logger.info("Health check endpoint was called.")
    return {"status": "ok", "message": "Welcome to the ETL Pipeline API"}

if __name__ == "__main__":
    logger.info("Starting Uvicorn server from main.py...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
