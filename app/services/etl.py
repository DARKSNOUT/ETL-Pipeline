# app/services/etl.py
import datetime
import json
import time
from pathlib import Path
from app.core.logging import get_logger
from app.db import mssql, sqlite_manager
from app.services import hashing
from app.core.config_manager import get_config
# --- NEW: Import the new exporter service ---
from app.services import exporter

# --- Configuration ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    OFFSET_FILE = PROJECT_ROOT / "database" / "offset.json"
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"
    SQL_QUERY_FILE = PROJECT_ROOT / "app" / "query" / "fetch_chunk.sql"
    # EXPORTS_DIR is no longer needed here, it's managed by the exporter service
except NameError:
    PROJECT_ROOT = Path.cwd()
    OFFSET_FILE = PROJECT_ROOT / "database" / "offset.json"
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"
    SQL_QUERY_FILE = PROJECT_ROOT / "app" / "query" / "fetch_chunk.sql"

ID_COLUMN = "reg_no"
RESET_REG_NO = "MUM/T(A)/13/000367"
KEY_MAPPING = { "Reg_no": "reg_no", "RegDate": "reg_date", "Report_Release_Date": "report_release_date", "Released": "released", "Test_End_Date": "test_end_date", "Invoicing_Type": "invoicing_type", "Test_Report_Stage": "test_report_stage", "InvoiceDate": "invoice_date", "Buyer": "buyer", "InvoiceNo": "invoice_no", "modifieddt": "modifieddt" }
logger = get_logger(__name__)

def _load_sql_query(filepath: Path) -> str:
    """Loads a SQL query from a file using a Path object."""
    if not filepath.is_file():
        logger.error(f"SQL query file not found at: {filepath}")
        raise FileNotFoundError(f"SQL query file not found at: {filepath}")
    logger.info(f"Loading SQL query from: {filepath}")
    return filepath.read_text()

def _process_and_hash_data(data: list[dict]) -> list[dict]:
    """Remaps keys and calculates a hash for each row."""
    processed_data = []
    for row in data:
        remapped_row = {KEY_MAPPING.get(k, k): v for k, v in row.items()}
        remapped_row['hash_value'] = hashing.calculate_row_hash(remapped_row)
        processed_data.append(remapped_row)
    logger.info(f"Processed and hashed {len(processed_data)} rows.")
    return processed_data

def _update_task_in_log(task_id: str, result: dict):
    """Updates the status of a task in the central log file."""
    log_data = {}
    TASKS_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    if TASKS_LOG_FILE.is_file():
        with open(TASKS_LOG_FILE, 'r') as f:
            try: log_data = json.load(f)
            except json.JSONDecodeError: logger.warning("tasks_log.json is corrupted, starting fresh.")
    if 'tasks' not in log_data or not isinstance(log_data['tasks'], dict):
        log_data['tasks'] = {}
    log_data['tasks'][task_id] = result
    with open(TASKS_LOG_FILE, 'w') as f:
        json.dump(log_data, f, indent=4)
    logger.info(f"Successfully updated status for task {task_id}.")

# This function remains for single-chunk runs if needed, but is not used by the scheduler
def run_etl_pipeline(task_id: str):
    """Executes ONE cycle of the ETL pipeline and is now config-aware."""
    start_time = datetime.datetime.now()
    logger.info(f"Starting SINGLE ETL cycle for task_id: {task_id}...")
    _update_task_in_log(task_id, {"status": "running", "start_time": str(start_time)})
    
    config = get_config()
    chunk_size = config.get("etl", {}).get("chunk_size", 1000)
    logger.info(f"Using chunk size of {chunk_size} from configuration for single run.")

    last_id = mssql.load_last_id(str(OFFSET_FILE))
    if last_id is None: last_id = ''
    
    try:
        query_template = _load_sql_query(SQL_QUERY_FILE)
        formatted_query = query_template.format(id_column=ID_COLUMN)
    except Exception as e:
        result = {"status": "error", "message": f"Failed to load/format SQL query: {e}"}
        _update_task_in_log(task_id, result)
        return
        
    params = {"last_id": last_id, "chunk_size": chunk_size}
    source_data = mssql.fetch_data_as_dict(formatted_query, params=params)
    
    if source_data is None:
        result = {"status": "error", "message": "Failed to fetch data from MSSQL"}
        _update_task_in_log(task_id, result)
        return
        
    if not source_data:
        logger.info("No new data found. Resetting offset.")
        mssql.save_last_id(RESET_REG_NO, str(OFFSET_FILE))
        result = {"status": "complete", "message": "No new data. Offset reset.", "rows_received": 0}
        _update_task_in_log(task_id, result)
        return
        
    processed_data = _process_and_hash_data(source_data)
    updated_count = sqlite_manager.upsert_rows(processed_data)
    new_last_id = processed_data[-1][ID_COLUMN]
    mssql.save_last_id(new_last_id, str(OFFSET_FILE))
    end_time = datetime.datetime.now()
    final_result = { "status": "success", "rows_received": len(source_data), "rows_updated_in_cache": updated_count, "last_offset_processed": new_last_id, "start_time": str(start_time), "end_time": str(end_time) }
    _update_task_in_log(task_id, final_result)


def run_full_etl_sync(task_id: str):
    """
    Runs the ETL process in a continuous loop and exports the result to Excel.
    """
    overall_start_time = datetime.datetime.now()
    logger.info(f"Starting FULL ETL sync for task_id: {task_id}...")
    
    config = get_config()
    chunk_size = config.get("etl", {}).get("chunk_size", 1000)
    logger.info(f"Using chunk size of {chunk_size} from configuration.")

    logger.info(f"Resetting offset to '{RESET_REG_NO}' to begin a true full sync.")
    mssql.save_last_id(RESET_REG_NO, str(OFFSET_FILE))
    
    _update_task_in_log(task_id, {"status": "running", "start_time": str(overall_start_time), "message": "Full sync started, offset has been reset."})
    
    total_rows_received = 0
    total_rows_updated = 0
    
    while True:
        logger.info(f"Full sync task {task_id}: Starting a new chunk.")
        last_id = mssql.load_last_id(str(OFFSET_FILE))
        if last_id is None: last_id = ''
        
        try:
            query_template = _load_sql_query(SQL_QUERY_FILE)
            formatted_query = query_template.format(id_column=ID_COLUMN)
        except Exception as e:
            logger.error(f"Full sync task {task_id}: Failed to load SQL query. Aborting. Error: {e}")
            break

        params = {"last_id": last_id, "chunk_size": chunk_size}
        source_data = mssql.fetch_data_as_dict(formatted_query, params=params)

        if source_data is None:
            logger.error(f"Full sync task {task_id}: Failed to fetch data from source. Aborting.")
            break
        
        if not source_data:
            logger.info(f"Full sync task {task_id}: No new data found. Sync complete.")
            break
        
        rows_in_chunk = len(source_data)
        total_rows_received += rows_in_chunk
        processed_data = _process_and_hash_data(source_data)
        updated_count = sqlite_manager.upsert_rows(processed_data)
        total_rows_updated += updated_count
        new_last_id = processed_data[-1][ID_COLUMN]
        mssql.save_last_id(new_last_id, str(OFFSET_FILE))
        logger.info(f"Full sync task {task_id}: Processed chunk of {rows_in_chunk} rows. New offset is {new_last_id}.")
        time.sleep(1)

    # --- UPDATED: Call the export function from the new exporter service ---
    exported_file_path = exporter.export_data_to_excel()

    overall_end_time = datetime.datetime.now()
    final_result = {
        "status": "complete",
        "message": "Full sync finished.",
        "total_rows_received": total_rows_received,
        "total_rows_updated_in_cache": total_rows_updated,
        "exported_file": exported_file_path, # Add the file path to the log
        "start_time": str(overall_start_time),
        "end_time": str(overall_end_time)
    }
    _update_task_in_log(task_id, final_result)
    logger.info(f"Full sync task {task_id} has completed.")
