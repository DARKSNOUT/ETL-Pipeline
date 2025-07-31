import json
from pathlib import Path
from .logging import get_logger

logger = get_logger(__name__)

# --- Configuration ---
try:
    # Path to the top-level project folder
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    CONFIG_FILE = PROJECT_ROOT / "app_config.json"
except NameError:
    # Fallback for different environments
    PROJECT_ROOT = Path.cwd()
    CONFIG_FILE = PROJECT_ROOT / "app_config.json"

# --- Default Settings ---
# These settings will be used if the config file doesn't exist yet.
DEFAULT_CONFIG = {
    "scheduler": {
        "interval_minutes": 60
    },
    "etl": {
        "chunk_size": 1000
    }
}

def get_config() -> dict:
    """
    Reads the app_config.json file and returns its content.
    If the file doesn't exist, it creates it with default values.
    """
    if not CONFIG_FILE.is_file():
        logger.warning(f"Configuration file not found at {CONFIG_FILE}. Creating a default one.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
        return config_data
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error reading config file: {e}. Returning default config as a fallback.")
        return DEFAULT_CONFIG

def save_config(config_data: dict):
    """
    Saves the given dictionary to the app_config.json file.
    """
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        logger.info(f"Successfully saved new configuration to {CONFIG_FILE}")
    except IOError as e:
        logger.error(f"Failed to save configuration file: {e}")

# --- Initialize the config file on startup ---
# This ensures the file exists when the application starts.
get_config()
