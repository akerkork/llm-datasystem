import sqlite3
import pandas as pd
import logging
from typing import Dict, List, Any

class SchemaManager:
    """
    The Schema Manager is responsible for understanding the structure of the database.
    It represents table schemas as structured objects, compares schemas for compatibility, 
    and provides schema context to other components.
    """

    def __init__(self, db_conn: sqlite3.Connection):
        """
        Args:
            db_conn: A connection to the SQLite database used strictly for reading 
                     schema metadata.
        """
        self.db_conn = db_conn
        self._setup_logging()

    def _setup_logging(self) -> None:
        """
        Configures error logging to a file named 'error_log.txt'.
        """
        logging.basicConfig(
            filename='error_log.txt',
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_existing_tables(self) -> List[str]:
        """
        Discovers existing tables in the database.
        
        Returns:
            A list of table names currently in the SQLite database.
        """
        cursor = self.db_conn.cursor()
        # Query the sqlite_master table to find all user-created tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Filter out internal SQLite tables like 'sqlite_sequence'
        return [table[0] for table in tables if table[0] != 'sqlite_sequence']

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Uses PRAGMA table_info() to detect an existing table's schema.
        
        Args:
            table_name: The name of the table to inspect.
            
        Returns:
            A dictionary mapping column names to their SQL data types.
        """
        cursor = self.db_conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # PRAGMA table_info returns tuples in the format: (cid, name, type, notnull, dflt_value, pk)
        # We extract the name (index 1) and type (index 2)
        schema = {}
        for col in columns:
            schema[col[1]] = col[2]
            
        return schema

    def infer_schema_from_df(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Inspects column names and data types from a pandas DataFrame and maps 
        them to SQLite data types (TEXT, INTEGER, REAL).
        
        Args:
            df: The pandas DataFrame to inspect.
            
        Returns:
            A dictionary mapping DataFrame column names to SQL data types.
        """
        schema = {}
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                schema[col_name] = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                schema[col_name] = "REAL"
            else:
                # Default to TEXT for strings, dates, and other objects
                schema[col_name] = "TEXT"
                
        return schema

    def generate_create_table_ddl(self, table_name: str, schema: Dict[str, str]) -> str:
        """
        Generates a CREATE TABLE statement dynamically using Python string formatting.
        Automatically adds an 'id INTEGER PRIMARY KEY AUTOINCREMENT' column.
        
        Args:
            table_name: The name of the new table.
            schema: A dictionary of column names and their SQL types.
            
        Returns:
            The raw SQL DDL string for creating the table.
        """
        # Initialize with the required primary key
        columns_ddl = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        
        for col_name, col_type in schema.items():
            columns_ddl.append(f"{col_name} {col_type}")
            
        columns_str = ", ".join(columns_ddl)
        return f"CREATE TABLE {table_name} ({columns_str});"

    def check_schema_compatibility(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compares the DataFrame schema to the existing database table schema to 
        determine compatibility.
        
        A match occurs if column names match (after normalization) and data types 
        match exactly. 
        
        Returns a dictionary indicating the action:
        - "append": If schemas match exactly.
        - "create": If the table does not exist.
        - "conflict": If the table exists but schemas do not match, requiring user 
                      prompting (overwrite, rename, or skip).
        """
        existing_tables = self.get_existing_tables()
        
        if table_name not in existing_tables:
            return {"action": "create"}
            
        db_schema = self.get_table_schema(table_name)
        df_schema = self.infer_schema_from_df(df)
        
        # Strip out the auto-generated 'id' column from the DB schema so we can fairly compare it to the incoming CSV data
        db_schema_no_id = {k: v for k, v in db_schema.items() if k.lower() != 'id'}
        
        # Compare schemas exactly
        if db_schema_no_id == df_schema:
            return {"action": "append"}
        else:
            self.log_error(
                f"Schema conflict detected for '{table_name}'. "
                f"DB expects: {db_schema_no_id}, CSV provided: {df_schema}"
            )
            return {"action": "conflict"}

    def get_database_schema_context(self) -> str:
        """
        Provides comprehensive schema information to other components, primarily 
        the Query Service and LLM.
        
        Returns:
            A formatted string describing all tables, their columns, and data types 
            to be injected into the LLM prompt.
        """
        tables = self.get_existing_tables()
        if not tables:
            return "The database is currently empty."
            
        context_parts = []
        for table in tables:
            schema = self.get_table_schema(table)
            # table_name (col1 type, col2 type)
            cols_str = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
            context_parts.append(f"- {table} ({cols_str})")
            
        return "The database uses SQLite and contains the following tables:\n" + "\n".join(context_parts)
        
    def log_error(self, message: str) -> None:
        """
        Helper method to log schema conflicts or validation errors to the error file.
        """
        self.logger.error(message)