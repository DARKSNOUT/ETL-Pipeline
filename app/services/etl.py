# app/services/etl.py
import datetime
import json
import time
from pathlib import Path
from app.core.logging import get_logger
from app.db import mssql, sqlite_manager
from app.services import hashing
from app.core.config_manager import get_config
from app.services import exporter

# --- Configuration ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"
    SQL_QUERY_FILE = PROJECT_ROOT / "app" / "query" / "fetch_chunk.sql"
except NameError:
    PROJECT_ROOT = Path.cwd()
    TASKS_LOG_FILE = PROJECT_ROOT / "database" / "tasks_log.json"
    SQL_QUERY_FILE = PROJECT_ROOT / "app" / "query" / "fetch_chunk.sql"

# --- Define the column to order by for pagination ---
# This will be inserted into the {id_column} placeholder in your .sql file
ORDER_BY_COLUMN = "reg_no"

KEY_MAPPING = {
    "reg_no": "reg_no",
    "reg_date": "reg_date",
    "report_release_date": "report_release_date",
    "released": "released",
    "test_end_date": "test_end_date",
    "invoicing_type": "invoicing_type",
    "test_report_stage": "test_report_stage",
    "invoice_date": "invoice_date",
    "buyer": "buyer",
    "invoice_no": "invoice_no",
    "modifieddt": "modifieddt"
}
logger = get_logger(__name__)

def _load_sql_query(filepath: Path) -> str:
    if not filepath.is_file():
        logger.error(f"SQL query file not found at: {filepath}")
        raise FileNotFoundError(f"SQL query file not found at: {filepath}")
    logger.info(f"Loading SQL query from: {filepath}")
    return filepath.read_text()

def _process_and_hash_data(data: list[dict]) -> list[dict]:
    """
    Processes a chunk of data by remapping keys and calculating a hash.
    """
    processed_data = []
    for row in data:
        # Remap the keys of the data intended for storage
        remapped_row = {KEY_MAPPING.get(k, k): v for k, v in row.items() if k in KEY_MAPPING}
        
        # Calculate the hash from the clean, remapped data
        remapped_row['hash_value'] = hashing.calculate_row_hash(remapped_row)
        
        processed_data.append(remapped_row)
        
    logger.info(f"Processed and hashed {len(processed_data)} rows.")
    return processed_data

def _update_task_in_log(task_id: str, result: dict):
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

# This function remains for single-chunk runs if needed
def run_etl_pipeline(task_id: str):
    pass

def run_full_etl_sync(task_id: str):
    """
    Runs the ETL process in a loop using OFFSET-based pagination.
    """
    overall_start_time = datetime.datetime.now()
    logger.info(f"Starting FULL ETL sync for task_id: {task_id} using OFFSET pagination...")
    
    config = get_config()
    chunk_size = config.get("etl", {}).get("chunk_size", 1000)
    logger.info(f"Using chunk size of {chunk_size} from configuration.")

    _update_task_in_log(task_id, {"status": "running", "start_time": str(overall_start_time), "message": "Full sync started."})
    
    total_rows_received = 0
    total_rows_updated = 0
    current_offset = 0
    
    try:
        query_template = _load_sql_query(SQL_QUERY_FILE)
        # --- FIX: Format the query to insert the ORDER BY column name ---
        # This is safe because ORDER_BY_COLUMN is a hardcoded variable.
        formatted_query = query_template.format(id_column=ORDER_BY_COLUMN)
    except FileNotFoundError:
        result = {"status": "error", "message": f"SQL query file not found at {SQL_QUERY_FILE}"}
        _update_task_in_log(task_id, result)
        return
    except KeyError:
        result = {"status": "error", "message": "The SQL query in fetch_chunk.sql is missing the {id_column} placeholder."}
        _update_task_in_log(task_id, result)
        return

    while True:
        logger.info(f"Full sync task {task_id}: Fetching chunk with OFFSET {current_offset}.")
        
        params = {"offset": current_offset, "chunk_size": chunk_size}
        source_data = mssql.fetch_data_as_dict(formatted_query, params=params)

        if source_data is None:
            logger.error(f"Full sync task {task_id}: Failed to fetch data from source. Aborting.")
            break
        
        if not source_data:
            logger.info(f"Full sync task {task_id}: No more data found. Sync complete.")
            break
        
        rows_in_chunk = len(source_data)
        total_rows_received += rows_in_chunk
        
        processed_data = _process_and_hash_data(source_data)
        updated_count = sqlite_manager.upsert_rows(processed_data)
        total_rows_updated += updated_count
        
        # Increment the offset for the next loop
        current_offset += rows_in_chunk
        
        logger.info(f"Full sync task {task_id}: Processed chunk of {rows_in_chunk} rows. Total processed: {total_rows_received}.")
        time.sleep(1)

    exported_file_path = exporter.export_data_to_excel()

    overall_end_time = datetime.datetime.now()
    final_result = {
        "status": "complete",
        "message": "Full sync finished.",
        "total_rows_received": total_rows_received,
        "total_rows_updated_in_cache": total_rows_updated,
        "exported_file": exported_file_path,
        "start_time": str(overall_start_time),
        "end_time": str(overall_end_time)
    }
    _update_task_in_log(task_id, final_result)
    logger.info(f"Full sync task {task_id} has completed.")
