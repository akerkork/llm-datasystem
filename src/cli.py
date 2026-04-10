import sys
from typing import Any

class CLI:
    """
    The Command Line Interface for the Data System.
    Acts as the entry point for user interaction, managing flows 
    through the Query Service and Data Loader layers without directly accessing 
    the database.
    """

    def __init__(self, query_service: Any, csv_ingestor: Any):
        """
        Initializes the CLI with the necessary service layers.
        There is no direct SQLite database connection.
        """
        self.query_service = query_service
        self.csv_ingestor = csv_ingestor

    def start(self) -> None:
        """
        The main application loop. Uses input() to simulate a chatbot-like 
        interaction.
        """
        while True:
            self._display_menu()
            choice = input("\nSelect an option (1-4): ").strip()
            
            if choice == "1":
                self._handle_load_csv()
            elif choice == "2":
                self._handle_run_query()
            elif choice == "3":
                self._handle_list_tables()
            elif choice == "4":
                print("Exiting...")
                sys.exit(0)
            else:
                print("Invalid choice. Please try again.")

    def _display_menu(self) -> None:
        """
        Prints the available options to the user.
        """
        print("\n--- Data System Menu ---")
        print("1. Load CSV")
        print("2. Ask a Question")
        print("3. List Tables")
        print("4. Exit")

    def _handle_load_csv(self) -> None:
        """
        Prompts the user for a file path and a target table name, then passes 
        them to the CSVIngestor.
        """
        file_path = input("Enter the path to the CSV file: ").strip()
        table_name = input("Enter the desired table name: ").strip()
        
        result = self.csv_ingestor.process_file(file_path, table_name)
        
        if result.get("status") == "conflict":
            print(f"\nSchema conflict detected for table '{table_name}'.")
            print("Options: [skip], [rename], [overwrite]")
            resolution = input("Choose a resolution: ").strip().lower()
            
            new_table_name = None
            if resolution == "rename":
                new_table_name = input("Enter new table name: ").strip()
            
            # Retry with user's resolution strategy
            result = self.csv_ingestor.process_file(
                file_path, 
                table_name, 
                conflict_resolution=resolution,
                new_table_name=new_table_name
            )
            
        elif result.get("status") == "append":
            print(f"\n{result.get('message')}")
            print("Options: [append], [overwrite], [cancel]")
            resolution = input("Choose a resolution: ").strip().lower()
            
            # Retry with user's append/overwrite/cancel choice
            result = self.csv_ingestor.process_file(
                file_path, 
                table_name, 
                conflict_resolution=resolution
            )
            
        if result.get("status") == "success":
            msg = result.get("message", f"Success! {result.get('rows_ingested', 0)} rows ingested.")
            print(msg)
        else:
            print(f"Error: {result.get('message', 'Unknown error.')}")

    def _handle_run_query(self) -> None:
        """
        Prompts the user for a natural language query, 
        passes it to the Query Service, and prints the formatted results or errors.
        """
        question = input("\nAsk a question about your data: ").strip()
        if not question:
            print("Question cannot be empty.")
            return
            
        print("Thinking...")
        result = self.query_service.process_natural_language_query(question)
        
        if result.get("status") == "success":
            print("\n--- Generated SQL ---")
            print(result.get("query", ""))
            print("\n--- Explanation ---")
            print(result.get("explanation", ""))
            print("\n--- Results ---")
            
            columns = result.get("columns", [])
            rows = result.get("results", [])
            
            if not rows:
                print("No results found.")
            else:
                # Basic string formatting to create a readable table structure in the console
                header = " | ".join(str(c) for c in columns)
                print(header)
                print("-" * len(header))
                for row in rows:
                    print(" | ".join(str(item) for item in row))
        else:
            print(f"Error: {result.get('message', 'Unknown error.')}")

    def _handle_list_tables(self) -> None:
        """
        Requests the list of available tables from the Query Service and 
        displays them to the user.
        """
        tables = self.query_service.get_table_listing()
        if not tables:
            print("\nNo tables found in the database.")
        else:
            print("\n--- Available Tables ---")
            for table in tables:
                print(f"- {table}")