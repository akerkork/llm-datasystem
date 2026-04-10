from typing import List, Any
import re

class SQLValidator:
    """
    Validates SQL queries before execution to ensure database security.
    Operates at the query structure level rather than performing full SQL parsing.
    """

    def __init__(self, schema_manager: Any):
        """
        Initializes the validator with access to the Schema Manager to verify 
        table and column existence.
        """
        self.schema_manager = schema_manager

    def validate_query(self, sql_query: str) -> bool:
        """
        The main validation entry point. 
        Runs all security and structural checks on the generated SQL.
        
        Returns True if the query is safe to execute, raises a ValueError otherwise.
        """
        if not sql_query or not sql_query.strip():
            raise ValueError("Query cannot be empty.")

        # Check for destructive commands
        if not self._is_select_only(sql_query):
            raise ValueError("Only SELECT queries are allowed. Destructive operations are forbidden.")

        # Extract tables and columns
        tables = self._extract_tables(sql_query)
        columns = self._extract_columns(sql_query)

        if not tables:
            raise ValueError("No valid table references found in the query.")

        # Validate against the database schema
        self._validate_tables_and_columns(tables, columns)

        return True

    def _is_select_only(self, sql_query: str) -> bool:
        """
        Ensures the query is strictly a SELECT statement and does not contain 
        destructive operations like DROP, DELETE, UPDATE, or INSERT.
        """
        query_upper = sql_query.strip().upper()
        
        if not query_upper.startswith("SELECT"):
            return False
            
        forbidden_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", 
            "CREATE", "TRUNCATE", "EXEC", "PRAGMA"
        ]
        
        # Check for forbidden keywords as standalone words
        for keyword in forbidden_keywords:
            if re.search(r'\b' + keyword + r'\b', query_upper):
                return False
                
        return True


    def _extract_tables(self, sql_query: str) -> List[str]:
        """
        Parses the query structure to extract all referenced table names.
        """
        # Find all words immediately following FROM or JOIN
        matches = re.findall(r'\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', sql_query, re.IGNORECASE)
        
        # Return a list of unique tables in lowercase
        return list(set([match.lower() for match in matches]))

    def _extract_columns(self, sql_query: str) -> List[str]:
        """
        Parses the query structure to extract all referenced column names.
        """
        columns = []
        
        # Extract the string between SELECT and FROM
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            cols_str = select_match.group(1)
            
            # Split by comma to get individual column declarations
            for part in cols_str.split(','):
                part = part.strip()
                
                # Remove AS aliases
                part = re.sub(r'\s+AS\s+\w+', '', part, flags=re.IGNORECASE)
                
                # Take the first word to drop implicit aliases without AS
                core_col = part.split()[0]
                
                # Extract the column name from inside basic SQL functions like SUM() or COUNT()
                func_match = re.search(r'\((.*?)\)', core_col)
                if func_match:
                    core_col = func_match.group(1)

                # Remove table prefixes
                col_name = core_col.split('.')[-1].strip('() ')
                                
                # Ignore asterisks, literal numbers, and empty strings
                if col_name != '*' and not col_name.isnumeric() and col_name:
                    columns.append(col_name.lower())
                    
        return list(set(columns))
    
    def _validate_tables_and_columns(self, tables: List[str], columns: List[str]) -> bool:
        """
        Cross-references the extracted tables and columns with the Schema Manager 
        to ensure they actually exist in the database.
        """
        # Get existing tables directly from the Schema Manager
        existing_tables_raw = self.schema_manager.get_existing_tables()
        existing_tables = {t.lower(): t for t in existing_tables_raw}
        
        valid_columns_in_query = set()
        
        # Validate Tables
        for t in tables:
            if t not in existing_tables:
                raise ValueError(f"Unknown table referenced: '{t}'")
                
            # Accumulate all valid columns for the requested tables
            actual_table_name = existing_tables[t]
            schema = self.schema_manager.get_table_schema(actual_table_name)
            valid_columns_in_query.update([c.lower() for c in schema.keys()])
            
        # Validate Columns
        for c in columns:
            # We ignore SQL constants like COUNT which might get caught in simple extraction
            if c not in valid_columns_in_query and c.upper() not in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                raise ValueError(f"Unknown column referenced: '{c}'")
                
        return True