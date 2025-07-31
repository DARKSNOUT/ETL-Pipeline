# app/core/config.py
import os
import sys
from dotenv import load_dotenv
from app.core.logging import get_logger

# Initialize logger for this module
logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()

class Settings:
    """
    Main application settings.
    Reads environment variables and constructs the database connection URL.
    """
    DB_SERVER: str | None = os.getenv("DB_SERVER")
    DB_NAME: str | None = os.getenv("DB_NAME")
    DB_USER: str | None = os.getenv("DB_USER")
    DB_PASSWORD: str | None = os.getenv("DB_PASSWORD")
    # It's good practice to specify the driver in your config
    ODBC_DRIVER: str = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

    def __init__(self):
        # Validate that essential database settings are present
        required_vars = ["DB_SERVER", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing_vars = [var for var in required_vars if not getattr(self, var)]
        if missing_vars:
            msg = f"CRITICAL: Missing essential environment variables: {', '.join(missing_vars)}. Please check your .env file."
            logger.critical(msg)
            sys.exit(f"Error: {msg}") # Stop the application if config is missing
            
    """
        Returns the full database connection URL.
        Using @property allows you to access it like an attribute (settings.database_url)
    """
    @property
    def database_url(self) -> str:
        
        driver_enc = self.ODBC_DRIVER.replace(" ", "+")
        return (
            f"mssql+pyodbc://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_SERVER}/{self.DB_NAME}?driver={driver_enc}"
        )

class Sharepoint:
    """SharePoint related settings - optional."""
    SP_SITE_URL: str = os.getenv("SP_SITE_URL", "")
    SP_CLIENT_ID: str = os.getenv("SP_CLIENT_ID", "")
    SP_CLIENT_SECRET: str = os.getenv("SP_CLIENT_SECRET", "")
    SP_LIBRARY: str = os.getenv("SP_LIBRARY", "")

    # Note: The 'database_url' method was removed from here as it was incorrect.
    # SharePoint connection logic would be different.

# --- Instantiate settings objects for easy import ---
settings = Settings()
sharepoint = Sharepoint()

logger.info("Configuration loaded successfully.")
