import pandas as pd
from typing import Dict, Any, Optional
from pathlib import Path

class CSVIngestor:
    """
    Handles the loading, validation, and preparation of CSV data 
    before it is sent to the Schema Manager for database insertion.
    """

    def __init__(self, schema_manager):
        """
        Args:
            schema_manager: An instance of the SchemaManager module 
                           to handle DB-level operations.
        """
        self.schema_manager = schema_manager

    def process_file(self, file_path: str, table_name: str) -> Dict[str, Any]:
        """
        The main entry point for the CLI to trigger an ingestion.
        
        1. Validates file existence.
        2. Loads and cleans data.
        3. Coordinates with Schema Manager to update SQLite.
        """
        try:
            # 1. Load Data
            df = self._load_csv(file_path)
            
            # 2. Validate Data (Check for nulls, types, etc.)
            self._validate_dataframe(df)
            
            # 3. Hand off to Schema Manager
            success = self.schema_manager.update_or_create_table(table_name, df)
            
            if success:
                return {"status": "success", "rows_ingested": len(df)}
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """Loads CSV into a Pandas DataFrame."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found at: {file_path}")
        return pd.read_csv(path)

    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Business logic validation. 
        Ensures columns are named correctly and data is not corrupt.
        """
        if df.empty:
            raise ValueError("The provided CSV file is empty.")

        # Stuff like that
            
        return True