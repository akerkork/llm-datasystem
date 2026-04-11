import sqlite3
from typing import Dict, Any, List

class QueryService:
    """
    Orchestrates the query flow by acting as the intermediary between the CLI, 
    the LLM Adapter, the SQL Validator, and the SQLite Database.
    """

    def __init__(
        self, 
        db_conn: sqlite3.Connection, 
        schema_manager: Any, 
        sql_validator: Any, 
        llm_adapter: Any
    ):
        self.db_conn = db_conn
        self.schema_manager = schema_manager
        self.sql_validator = sql_validator
        self.llm_adapter = llm_adapter

    def process_direct_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        Handles direct SQL input from the user.
        Validates the SQL code against the database schema before executing it.
        """
        try:
            # Validate the user's direct SQL input to protect the database
            self.sql_validator.validate_query(sql_query)
            
            # If validation passes, safely execute the query
            results, columns = self._execute_query(sql_query)
            
            return {
                "status": "success", 
                "columns": columns, 
                "results": results, 
                "query": sql_query
            }
        except ValueError as ve:
            # Validation errors
            return {"status": "error", "message": f"Validation Error: {str(ve)}"}
        except Exception as e:
            return {"status": "error", "message": f"Execution Error: {str(e)}"}

    def process_natural_language_query(self, user_query: str) -> Dict[str, Any]:
        """
        Handles natural language input.
        
        1. Gets the database schema from the Schema Manager.
        2. Passes the query and schema to the LLM Adapter to generate SQL.
        3. Passes the generated SQL to the SQL Validator.
        4. Executes the SQL if valid.
        5. Formats and returns the response.
        """
        try:
            # Get the database schema context
            schema_context = self.schema_manager.get_database_schema_context()
            
            # Pass to the LLM Adapter
            llm_response = self.llm_adapter.generate_sql(user_query, schema_context)
            generated_sql = llm_response.get("sql")
            explanation = llm_response.get("explanation", "No explanation provided.")
            
            
            # Validate the generated SQL
            self.sql_validator.validate_query(generated_sql)
            
            # Execute the SQL safely
            results, columns = self._execute_query(generated_sql)
            
            # Format the response
            return {
                "status": "success", 
                "columns": columns,
                "results": results, 
                "query": generated_sql,
                "explanation": explanation
            }
        except ValueError as ve:
            return {
                "status": "error", 
                "message": f"Validator rejected LLM output: {str(ve)}",
                "query": generated_sql if 'generated_sql' in locals() else None
            }
        except Exception as e:
            return {"status": "error", "message": f"An error occurred: {str(e)}"}


    def _execute_query(self, valid_sql: str) -> List[tuple]:
        """
        Safely executes a validated SELECT query against the SQLite database 
        and fetches the results.
        """
        cursor = self.db_conn.cursor()
        cursor.execute(valid_sql)
        results = cursor.fetchall()
        
        # Extract column names from the cursor description to make formatting easier in the CLI
        columns = [description[0] for description in cursor.description] if cursor.description else []
        
        return results, columns

    def get_table_listing(self) -> List[str]:
        """
        Retrieves a list of available tables using sqlite_master to provide 
        listing functionality to the user.
        """
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence';")
        tables = cursor.fetchall()
        return [table[0] for table in tables]