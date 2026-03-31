import pandas as pd
import sqlite3
from typing import Dict, Any
from pathlib import Path

class CSVIngestor:
    """
    Handles the loading, validation, and preparation of CSV data.
    Coordinates with the Schema Manager to ensure compatibility, 
    and manually executes the SQL to insert the data into SQLite.
    """

    def __init__(self, db_conn: sqlite3.Connection, schema_manager):
        """
        Args:
            db_conn: A connection to the SQLite database to execute queries.
            schema_manager: An instance of the SchemaManager module 
                            to handle schema discovery and generation.
        """
        self.db_conn = db_conn
        self.schema_manager = schema_manager

    def process_file(self, file_path: str, table_name: str) -> Dict[str, Any]:
        """
        The main entry point for the CLI to trigger an ingestion.
        
        1. Validates file existence and loads data.
        2. Validates data integrity.
        3. Coordinates with Schema Manager to check table existence and get DDL.
        4. Executes table creation (if new).
        5. Manually inserts records into the database.
        """
        try:
            # 1. Load Data
            df = self._load_csv(file_path)
            
            # 2. Validate Data (Check for nulls, types, etc.)
            self._validate_dataframe(df)
            
            # 3. Hand off to Schema Manager for schema checking
            # The schema manager determines if we append or create, 
            # but it does NOT execute the SQL itself.
            schema_action = self.schema_manager.check_schema_compatibility(table_name, df)
            
            if schema_action.get("action") == "create":
                # Schema manager provides the CREATE TABLE statement
                create_stmt = schema_action.get("create_statement")
                self._execute_sql(create_stmt)
            elif schema_action.get("action") == "conflict":
                # Handle conflict (overwrite, rename, skip) based on your logic
                return {"status": "error", "message": "Schema conflict detected."}
            
            # 4. Insert Data (Must be done manually, no df.to_sql())
            self._insert_data(table_name, df)
            
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
            Logic validation and data normalization. 
            Ensures columns are named consistently and data is prepared for manual SQL insertion.
            """
            if df.empty:
                raise ValueError("The provided CSV file is empty.")

            # Normalize column names to improve schema matching reliability
            df.columns = [str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]

            # Translate missing values to SQL NULL values.
            df.fillna(value=pd.NA, inplace=True)
            df.replace({pd.NA: None}, inplace=True)
                
            return True

    def _execute_sql(self, query: str) -> None:
        """Executes a DDL or DML statement directly."""
        cursor = self.db_conn.cursor()
        cursor.execute(query)
        self.db_conn.commit()

    def _insert_data(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Manually inserts data row by row. 
        Must NOT use df.to_sql() per project restrictions.
        """
        cursor = self.db_conn.cursor()
        
        # Create a parameterized query string for the manual insertion
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Iterate over the dataframe and insert
        for row in df.itertuples(index=False, name=None):
            cursor.execute(insert_query, row)
            
        self.db_conn.commit()