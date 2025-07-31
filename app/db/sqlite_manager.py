# app/db/sqlite_manager.py
import sqlite3
from contextlib import contextmanager
from app.core.logging import get_logger
from pathlib import Path

# --- Configuration ---

APP_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = APP_ROOT / "database" / "data_cache.db"

# Initialize logger for this module
logger = get_logger(__name__)

@contextmanager
def get_connection():
    """Provides a database connection using a context manager."""
    # Ensure the database directory exists
    import os
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row # Allows accessing columns by name
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
    except sqlite3.Error as e:
        logger.error(f"Failed to create SQLite table: {e}")

def upsert_rows(rows: list[dict]) -> int:
    """
    Inserts or updates rows in the SQLite cache.
    Returns the number of rows that were inserted or updated.
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
                
                # If the row doesn't exist, or if the hash has changed, upsert it.
                if result is None or result['hash_value'] != row['hash_value']:
                    # Use INSERT OR REPLACE for a clean upsert operation
                    cursor.execute("""
                        INSERT OR REPLACE INTO cache_data (
                            reg_no, reg_date, report_release_date, released, test_end_date,
                            invoicing_type, test_report_stage, invoice_date, buyer,
                            invoice_no, modifieddt, hash_value
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, tuple(row.values()))
                    updated_count += 1
            
            conn.commit()
            logger.info(f"Upsert complete. Rows affected: {updated_count}")
            
    except sqlite3.Error as e:
        logger.error(f"An error occurred during SQLite upsert: {e}", exc_info=True)
        # Depending on requirements, you might want to handle rollback here
    return updated_count

# Call this once on application startup to make sure the table is there
ensure_table_exists()
