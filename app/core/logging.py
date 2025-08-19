import logging
import sys
from logging.handlers import TimedRotatingFileHandler

# --- Configuration ---
LOG_FILE = "logs/app.log"
LOG_LEVEL = logging.INFO

# --- Formatter ---
# Create a more detailed formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)

def get_console_handler():
    """Returns a console handler."""    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    return console_handler

def get_file_handler(log_file: str):
    """Returns a file handler that rotates logs daily."""
    # Use TimedRotatingFileHandler to create a new log file every day
    # backupCount=7 means it will keep the last 7 days of logs
    file_handler = TimedRotatingFileHandler(log_file, when="midnight", backupCount=7)
    file_handler.setFormatter(formatter)
    return file_handler

def get_logger(logger_name: str):
    """
    Configures and returns a logger.
    - It logs to the console.
    - It logs to a file that rotates daily.
    """
    # Get a logger with the specified name
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)
    # Prevent log messages from being duplicated in parent loggers
    logger.propagate = False
    # Add handlers only if they haven't been added already
    if not logger.handlers:
        logger.addHandler(get_console_handler())
        # Make sure the logs directory exists
        import os
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        logger.addHandler(get_file_handler(LOG_FILE))

    return logger

# --- Create a default logger for easy import ---
logger = get_logger("ETL_Pipeline")
logger.info("Logger initialized.")