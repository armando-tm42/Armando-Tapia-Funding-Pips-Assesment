"""
Account Service - Uses Polars to read account data from CSV files
Provides account lookup functionality with data validation
"""
import os
from pathlib import Path
from typing import Optional, Dict, Any

import polars as pl
from dotenv import load_dotenv

class AccountServiceError(Exception):
    """Custom exception for account service errors"""
    pass

class AccountService:
    """Service for reading and querying account data using Polars"""
    
    def __init__(self):
        """Initialize account service with data validation"""
        self._data_path = self._validate_and_get_data_path()
        self._accounts_file = self._validate_accounts_file()
    
    def _validate_and_get_data_path(self) -> Path:
        """
        Validate DATA_DIR environment variable and return resolved path
        
        Returns:
            Path: Validated data directory path
            
        Raises:
            AccountServiceError: If DATA_DIR is not set or path doesn't exist
        """
        # Load environment variables
        # Try to find .env file in project root (assuming service is in app/services/)
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent
        env_file = project_root / ".env"
        
        if env_file.exists():
            load_dotenv(env_file)
        
        # Get DATA_DIR from environment
        data_dir = os.getenv("DATA_DIR")
        if not data_dir:
            raise AccountServiceError(
                "DATA_DIR environment variable is not set. "
                "Please add DATA_DIR to your .env file (e.g., DATA_DIR=./data)"
            )
        
        # Resolve DATA_DIR path relative to project root if it's relative
        if data_dir.startswith('./') or not os.path.isabs(data_dir):
            if data_dir.startswith('./'):
                relative_path = data_dir[2:]
            else:
                relative_path = data_dir
            
            data_path = project_root / relative_path
        else:
            data_path = Path(data_dir)
        
        # Convert to absolute path and validate existence
        data_path = data_path.resolve()
        
        if not data_path.exists():
            raise AccountServiceError(
                f"DATA_DIR path does not exist: {data_path} "
                f"(resolved from: {data_dir})"
            )
        
        if not data_path.is_dir():
            raise AccountServiceError(
                f"DATA_DIR is not a directory: {data_path}"
            )
        
        return data_path
    
    def _validate_accounts_file(self) -> Path:
        """
        Validate that accounts_db.csv exists in the data directory
        
        Returns:
            Path: Path to accounts_db.csv file
            
        Raises:
            AccountServiceError: If accounts_db.csv doesn't exist
        """
        accounts_file = self._data_path / "accounts_db.csv"
        
        if not accounts_file.exists():
            raise AccountServiceError(
                f"accounts_db.csv not found in DATA_DIR: {accounts_file}. "
                f"Please ensure the file exists or run preprocessing script first."
            )
        
        if not accounts_file.is_file():
            raise AccountServiceError(
                f"accounts_db.csv is not a file: {accounts_file}"
            )
        
        return accounts_file
    
    def get_account_by_login(self, login: int) -> Optional[Dict[str, Any]]:
        """
        Get account data by login using Polars lazy scanning
        
        Args:
            login: Account login ID to search for
            
        Returns:
            Dict[str, Any]: Account data if found, None otherwise
            
        Raises:
            AccountServiceError: If there's an error reading the CSV file
        """
        try:
            # Use lazy scanning for efficient filtering
            account_query = (
                pl.scan_csv(self._accounts_file)
                .filter(pl.col("login") == login)
                .limit(1)  # Only need first match since login should be unique
            )
            
            # Execute the lazy query
            result_df = account_query.collect()
            
            # Return None if no account found
            if result_df.is_empty():
                return None
            
            # Convert first row to dictionary
            account_data = result_df.to_dicts()[0]
            return account_data
            
        except pl.exceptions.PolarsError as e:
            raise AccountServiceError(
                f"Error reading accounts data from {self._accounts_file}: {str(e)}"
            )
        except Exception as e:
            raise AccountServiceError(
                f"Unexpected error while querying account data: {str(e)}"
            )
    
    def get_data_path(self) -> Path:
        """Get the validated data directory path"""
        return self._data_path
    
    def get_accounts_file_path(self) -> Path:
        """Get the path to the accounts CSV file"""
        return self._accounts_file
    
def create_account_service() -> AccountService:
    """
    Create and return an AccountService instance
    
    Returns:
        AccountService: Configured account service instance
        
    Raises:
        AccountServiceError: If service initialization fails
    """
    return AccountService()
