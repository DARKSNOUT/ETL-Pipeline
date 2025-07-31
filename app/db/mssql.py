# app/db/mssql.py
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from app.core.config import settings
from app.core.logging import get_logger

# Initialize logger for this module
logger = get_logger(__name__)

try:
    engine = create_engine(
        settings.database_url,
        pool_size=5,
        pool_pre_ping=True,    # Handles stale connections
        fast_executemany=True, # Bulk parameters optimization
        echo=False             # Set to True to log all SQL statements
    )
    logger.info("Database engine created successfully.")
except Exception as e:
    logger.critical(f"Failed to create database engine: {e}")
    # Depending on the strategy, you might want to exit here

#--------------------------------------------------------------------#
# These functions handle reading/writing the last processed ID (offset)
#--------------------------------------------------------------------#

def load_last_id(filename: str) -> str | None:
    """Loads the last processed ID from a JSON file."""
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            last_id = data.get("last_id")
            logger.info(f"Successfully loaded last_id '{last_id}' from {filename}")
            return last_id
    except FileNotFoundError:
        logger.warning(f"Offset file '{filename}' not found. Will start from the beginning.")
        return None
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Error reading or parsing offset file '{filename}': {e}")
        return None

def save_last_id(last_id: str, filename: str):
    """Saves the last processed ID to a JSON file."""
    try:
        with open(filename, "w") as f:
            json.dump({"last_id": last_id}, f, indent=4)
        logger.info(f"Successfully saved last_id '{last_id}' to {filename}")
    except IOError as e:
        logger.error(f"Could not write to offset file '{filename}': {e}")

#--------------------------------------------------------------------#

def fetch_data_as_dict(query: str, params: dict | None = None) -> list[dict] | None:
    """
    Fetches data from the database using a given query and returns a list of dictionaries.
    """
    logger.info(f"Executing query to fetch data...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            rows = result.fetchall()
            columns = result.keys()
            
            # Use a list comprehension for efficiency
            data = [dict(zip(columns, row)) for row in rows]
            
            logger.info(f"Query executed successfully. Rows fetched: {len(data)}")
            return data
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching data: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during data fetch: {e}", exc_info=True)
        return None
