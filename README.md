# LLM Data System: Natural Language to SQL Engine

## Overview
The LLM Data System is a modular, CLI-based application that allows users to interact with structured data using plain English. It seamlessly ingests CSV files into a local SQLite database, dynamically infers and manages database schemas, and utilizes the Google Gemini API to translate natural language questions into safe, executable SQL queries.

## Key Features
* **Natural Language Querying**: Ask questions about your data in plain English and get formatted tabular results.
* **Intelligent CSV Ingestion**: Automatically infers SQL data types from CSVs, generates `CREATE TABLE` statements, and securely loads data.
* **Schema Conflict Resolution**: Detects schema mismatches when loading new data into existing tables and prompts the user for safe resolution, such as Append, Overwrite, Rename, or Cancel.
* **SQL Validation & Security**: Intercepts LLM-generated SQL and strictly validates it to ensure only non-destructive `SELECT` statements are executed against the database.
* **Modular Architecture**: Built with clean separation of concerns, utilizing Dependency Injection to easily swap out components.

## Prerequisites
* **Python**: 3.10 or 3.11.
* **API Key**: A [Google AI Studio](https://aistudio.google.com/) API key for Gemini.

## Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/akerkork/llm-datasystem
   cd llm-datasystem
   ```

2. **Create and activate a virtual environment**
   * **Windows**:
     ```powershell
     python -m venv venv
     .\venv\Scripts\Activate.ps1
     ```
   * **macOS/Linux**:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set your Gemini API Key**
   Export your API key as an environment variable so the application can authenticate with Google's servers.
   * **Windows (PowerShell)**:
     ```powershell
     $env:GEMINI_API_KEY="your_api_key_here"
     ```
   * **macOS/Linux**:
     ```bash
     export GEMINI_API_KEY="your_api_key_here"
     ```

## Usage

Start the interactive CLI:
```bash
python main.py
```

### Example Workflow
1. **Load Data**: Choose Option `1` to ingest a CSV. Provide the path to your CSV and name the target table.
2. **List Tables**: Choose Option `3` to verify your table was created successfully.
3. **Ask a Question**: Choose Option `2` and ask a question like: *"What is the average salary for each department?"* The system will generate the SQL, explain its reasoning, and return the formatted data.
4. **Exit**: Choose Option `4` to safely close the application.

## Architecture Overview

The system is built on a modular architecture to enforce separation of concerns:

* **CLI (`src/cli.py`)**: The main entry point and presentation layer. It manages user input/output and orchestrates workflows.
* **CSV Ingestor (`src/csv_handler.py`)**: Handles data loading, normalization, and insertion. It coordinates with the Schema Manager to handle structural conflicts.
* **Schema Manager (`src/schema_manager.py`)**: The source of truth for the database structure. It extracts DDL from pandas DataFrames, compares schemas, and provides context to the LLM.
* **Query Service (`src/query_service.py`)**: The orchestration layer for querying. It acts as the middleman between the CLI, the LLM, the SQL Validator, and the SQLite engine.
* **LLM Adapter (`src/llm_adapter.py`)**: The interface to the Gemini API (`google-genai`). It handles prompt engineering, context injection, and parsing responses.
* **SQL Validator (`src/sql_validator.py`)**: A security layer that structurally analyzes generated SQL to reject destructive operations and verify schema existence.

## Testing
This project uses `pytest` for unit testing, featuring mocks for the LLM Adapter and Database connections.

To run the test suite locally:
```bash
pytest -v
```
The repository is equipped with a GitHub Actions CI pipeline (`.github/workflows/ci.yml`) that automatically runs tests on push and pull requests.

## Gen AI Usage
I used Gen AI, especially in debugging and reviewing the code. I also used it to learn concepts that I never used before, such as SQLite and Gemini API. Also, I used it to build the README file :)
