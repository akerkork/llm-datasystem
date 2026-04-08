import sqlite3
from unittest.mock import MagicMock
import pytest
from src.query_service import QueryService

@pytest.fixture
def db_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE people (name TEXT, age INTEGER)")
    conn.executemany(
        "INSERT INTO people (name, age) VALUES (?, ?)",
        [("Alice", 25), ("Bob", 30)],
    )
    conn.commit()
    yield conn
    conn.close()

@pytest.fixture
def dependencies():
    return MagicMock(), MagicMock(), MagicMock()

def test_process_direct_sql_success(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    result = service.process_direct_sql("SELECT name, age FROM people ORDER BY age")

    assert result["status"] == "success"
    assert result["columns"] == ["name", "age"]
    assert result["results"] == [("Alice", 25), ("Bob", 30)]
    sql_validator.validate_query.assert_called_once()

def test_process_direct_sql_validation_error(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    sql_validator.validate_query.side_effect = ValueError("Only SELECT queries are allowed")
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    result = service.process_direct_sql("DELETE FROM people")

    assert result["status"] == "error"
    assert "Validation Error" in result["message"]

def test_process_natural_language_query_success(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    schema_manager.get_database_schema_context.return_value = "- people (name TEXT, age INTEGER)"
    llm_adapter.generate_sql.return_value = {
        "sql": "SELECT name FROM people ORDER BY name",
        "explanation": "Lists all names.",
    }
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    result = service.process_natural_language_query("show all names")
    assert result["status"] == "success"
    assert result["columns"] == ["name"]
    assert result["results"] == [("Alice",), ("Bob",)]
    assert result["explanation"] == "Lists all names."

def test_process_natural_language_query_handles_missing_sql(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    schema_manager.get_database_schema_context.return_value = "schema"
    llm_adapter.generate_sql.return_value = {"sql": "", "explanation": "failed"}
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    result = service.process_natural_language_query("show all names")
    assert result["status"] == "error"
    assert "failed to generate" in result["message"].lower()

def test_process_natural_language_query_reports_validator_rejection(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    schema_manager.get_database_schema_context.return_value = "schema"
    llm_adapter.generate_sql.return_value = {
        "sql": "SELECT does_not_exist FROM people",
        "explanation": "Bad query",
    }
    sql_validator.validate_query.side_effect = ValueError("Unknown column referenced")
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    result = service.process_natural_language_query("bad query")
    assert result["status"] == "error"
    assert "Validator rejected" in result["message"]
    assert result["query"] == "SELECT does_not_exist FROM people"

def test_get_table_listing_returns_user_tables(db_conn, dependencies):
    schema_manager, sql_validator, llm_adapter = dependencies
    service = QueryService(db_conn, schema_manager, sql_validator, llm_adapter)

    tables = service.get_table_listing()
    assert tables == ["people"]
