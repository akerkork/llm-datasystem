import sqlite3
import os

from src.csv_handler import CSVIngestor
from src.schema_manager import SchemaManager
from src.sql_validator import SQLValidator
from src.llm_adapter import LLMAdapter
from src.query_service import QueryService
from src.cli import CLI

def main():
    """
    The main entry point for the Data System application.
    Wires up all the dependencies and starts the CLI.
    """
    # Establish the database connection
    db_path = "data/database.sqlite"
    
    # Ensure the data directory exists
    os.makedirs("data", exist_ok=True)
    db_conn = sqlite3.connect(db_path)

    # Information and validation modules
    schema_manager = SchemaManager(db_conn)
    sql_validator = SQLValidator(schema_manager)
    
    # Instantiate the LLM Adapter (ADD API KEY HERE)
    llm_adapter = LLMAdapter(api_key="API_KEY")
    
    # Data and orchestration modules
    csv_ingestor = CSVIngestor(db_conn, schema_manager)
    query_service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)
    
    # Start the CLI
    cli = CLI(query_service, csv_ingestor)
    
    print("Starting Building Data Systems CLI...")
    cli.start()

if __name__ == "__main__":
    main()