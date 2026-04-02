import pandas as pd
import sqlite3
from typing import Dict, Any, Optional
from pathlib import Path

class CSVIngestor:
    """
    Handles the loading, validation, and preparation of CSV data
    """

    def __init__(self, db_conn: sqlite3.Connection, schema_manager):
        """
        Args:
            db_conn: A connection to the SQLite database to execute queries.
            schema_manager: An instance of the SchemaManager module
        """
        self.db_conn = db_conn
        self.schema_manager = schema_manager

    def process_file(
            self, 
            file_path: str, 
            table_name: str, 
            conflict_resolution: Optional[str] = None,
            new_table_name: Optional[str] = None
        ) -> Dict[str, Any]:
            """
            The main entry point for the CLI to trigger an ingestion.
            - Validates file existence and loads data.
            - Validates data integrity.
            - Coordinates with Schema Manager to check table existence.
            - Executes table creation (if new).
            - Manually inserts records into the database.
            """
            try:
                df = self._load_csv(file_path)
                
                # Validate Data
                self._validate_dataframe(df)
                
                # Go to Schema Manager for schema checking
                schema_action = self.schema_manager.check_schema_compatibility(table_name, df)
                
                # Apply user's conflict resolution choice if a conflict was previously detected
                if conflict_resolution == "skip":
                    return {"status": "success", "message": "Ingestion skipped by user."}
                
                elif conflict_resolution == "rename" and new_table_name:
                    table_name = new_table_name
                    # Re-check compatibility with the newly chosen table name
                    schema_action = self.schema_manager.check_schema_compatibility(table_name, df)
                    
                elif conflict_resolution == "overwrite":
                    # Drop existing table and force a create action
                    self._execute_sql(f"DROP TABLE IF EXISTS {table_name}")
                    schema = self.schema_manager.infer_schema_from_df(df)
                    create_stmt = self.schema_manager.generate_create_table_ddl(table_name, schema)
                    schema_action = {"action": "create", "create_statement": create_stmt}

                # Execute table creation if required by the schema action
                if schema_action.get("action") == "create":
                    self._execute_sql(schema_action.get("create_statement"))
                    
                elif schema_action.get("action") == "conflict" and not conflict_resolution:
                    # Pause ingestion and return a conflict status to the CLI so it can prompt the user
                    return {"status": "conflict", "message": "Schema conflict detected."}
                
                # Insert data
                self._insert_data(table_name, df)
                
                return {"status": "success", "rows_ingested": len(df)}
                
            except Exception as e:
                return {"status": "error", "message": str(e)}

    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """Loads CSV into a Pandas DataFrame"""
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
            """
            Executes a DDL statement directly.
            """
            # Create a cursor object to act as the intermediary for sending commands to the database
            cursor = self.db_conn.cursor()
            
            # Send the raw SQL string to the SQLite engine for processing
            cursor.execute(query)
            
            # Commit the transaction to ensure all changes are permanently saved to the .sqlite file
            self.db_conn.commit()

    def _insert_data(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Manually inserts data row by row
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