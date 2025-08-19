# app/db/sqlite_manager.py
import sqlite3
from pathlib import Path
from contextlib import contextmanager
from app.core.logging import get_logger

logger = get_logger(__name__)

# --- Configuration ---
try:
    # Corrected path calculation to point to the TOP-LEVEL project folder
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    DB_PATH = PROJECT_ROOT / "database" / "data_cache.db"
    logger.info(f"Absolute path to SQLite database set to: {DB_PATH}")
except NameError:
    DB_PATH = Path("database/data_cache.db")
    logger.warning(f"Could not determine absolute path. Using relative path: {DB_PATH}")


@contextmanager
def get_connection():
    """Provides a database connection using a context manager."""
    db_dir = DB_PATH.parent
    db_dir.mkdir(parents=True, exist_ok=True)
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        logger.debug("SQLite connection opened.")
        yield conn
    except sqlite3.Error as e:
        logger.error(f"SQLite connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("SQLite connection closed.")

def ensure_table_exists():
    """Creates the cache_data table if it doesn't already exist."""
    logger.info("Ensuring 'cache_data' table exists in SQLite.")
    try:
        with get_connection() as conn:
            # --- CORRECTED: Removed the 'row_num' column ---
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_data (
                    reg_no TEXT PRIMARY KEY,
                    reg_date TEXT,
                    report_release_date TEXT,
                    released TEXT,
                    test_end_date TEXT,
                    invoicing_type TEXT,
                    test_report_stage TEXT,
                    invoice_date TEXT,
                    buyer TEXT,
                    invoice_no TEXT,
                    modifieddt TEXT,
                    hash_value INTEGER NOT NULL
                )
            """)
            conn.commit()
            logger.info("'cache_data' table is ready.")
    except Exception as e:
        logger.error(f"Failed to ensure table exists because of an upstream error: {e}")

def upsert_rows(rows: list[dict]) -> int:
    """
    Inserts or updates rows in the SQLite cache.
    """
    if not rows:
        logger.info("No rows to upsert into SQLite.")
        return 0
        
    updated_count = 0
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            for row in rows:
                cursor.execute("SELECT hash_value FROM cache_data WHERE reg_no = ?", (row['reg_no'],))
                result = cursor.fetchone()
                
                if result is None or result['hash_value'] != row['hash_value']:
                    # --- CORRECTED: Removed 'row_num' from the INSERT OR REPLACE statement ---
                    cursor.execute("""
                        INSERT OR REPLACE INTO cache_data (
                            reg_no, reg_date, report_release_date, released, test_end_date,
                            invoicing_type, test_report_stage, invoice_date, buyer,
                            invoice_no, modifieddt, hash_value
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['reg_no'], row['reg_date'], row['report_release_date'],
                        row['released'], row['test_end_date'], row['invoicing_type'],
                        row['test_report_stage'], row['invoice_date'], row['buyer'],
                        row['invoice_no'], row['modifieddt'], row['hash_value']
                    ))
                    updated_count += 1
            
            conn.commit()
            logger.info(f"Upsert complete. Rows affected: {updated_count}")
            
    except Exception as e:
        logger.error(f"An error occurred during SQLite upsert: {e}", exc_info=True)
        
    return updated_count

# Call this once on application startup
ensure_table_exists()
