# app/services/hashing.py
import zlib
from app.core.logging import get_logger

# Initialize logger for this module
logger = get_logger(__name__)

def calculate_row_hash(row: dict) -> int:
    """
    Calculates a CRC32 hash for a dictionary row to detect changes.
    Returns:
        An integer representing the calculated hash value.
    """
    if not isinstance(row, dict):
        logger.warning(f"Input to hash function was not a dict, but {type(row)}. Returning 0.")
        return 0
        
    try:
        # Create a consistent string representation of the dictionary.
        # - Sort by keys to ensure order doesn't affect the hash.
        # - Convert all values to strings.
        concatenated_string = "".join(
            str(row[key]) for key in sorted(row.keys())
        )
        # Encode the string to bytes before hashing
        encoded_string = concatenated_string.encode('utf-8')
        # Calculate and return the CRC32 hash
        hash_value = zlib.crc32(encoded_string)
        return hash_value
        
    except Exception as e:
        logger.error(f"Could not calculate hash for row: {row}. Error: {e}", exc_info=True)
        # Return a default/error hash value
        return 0
