# app/services/exporter.py
import datetime
import pandas as pd
from pathlib import Path
from app.core.logging import get_logger
from app.db import sqlite_manager # It needs to connect to the DB

logger = get_logger(__name__)

# --- Configuration for export paths ---
try:
    # Calculate path relative to this file's location
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    EXPORTS_DIR = PROJECT_ROOT / "exports"
except NameError:
    # Fallback for different environments
    PROJECT_ROOT = Path.cwd()
    EXPORTS_DIR = PROJECT_ROOT / "exports"

# --- NEW: Define a common, fixed filename for the export ---
COMMON_EXPORT_FILENAME = "etl_master_export.xlsx"

def export_data_to_excel() -> str | None:
    """
    Reads all data from the SQLite cache and exports it to a common Excel file,
    overwriting it if it already exists.
    Returns the path to the exported file on success, otherwise None.
    """
    logger.info(f"Starting export of SQLite data to '{COMMON_EXPORT_FILENAME}'...")
    try:
        # Ensure the exports directory exists
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        
        # --- Use the common filename instead of a timestamped one ---
        export_path = EXPORTS_DIR / COMMON_EXPORT_FILENAME

        # Connect to the SQLite database and read data into a pandas DataFrame
        with sqlite_manager.get_connection() as conn:
            df = pd.read_sql_query("SELECT * FROM cache_data", conn)
        
        if df.empty:
            logger.warning("SQLite cache is empty. Nothing to export.")
            # We can optionally delete the old file if no new data exists
            if export_path.is_file():
                export_path.unlink()
                logger.info(f"Deleted old export file at {export_path} as there is no new data.")
            return None
            
        # Save the DataFrame to an Excel file.
        # The 'to_excel' function will automatically overwrite the existing file.
        df.to_excel(export_path, index=False, engine='openpyxl')
        
        logger.info(f"Successfully exported and refreshed {len(df)} rows to {export_path}")
        return str(export_path)

    except Exception as e:
        logger.error(f"Failed to export data to Excel: {e}", exc_info=True)
        return None
