import sqlite3
import pandas as pd
import pytest
from src.schema_manager import SchemaManager

@pytest.fixture
def db_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE people (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, salary REAL)"
    )
    conn.commit()
    yield conn
    conn.close()

@pytest.fixture
def schema_manager(db_conn):
    return SchemaManager(db_conn)

def test_get_existing_tables(schema_manager):
    tables = schema_manager.get_existing_tables()
    assert "people" in tables
    assert "sqlite_sequence" not in tables

def test_get_table_schema(schema_manager):
    schema = schema_manager.get_table_schema("people")
    assert schema["id"] == "INTEGER"
    assert schema["name"] == "TEXT"
    assert schema["age"] == "INTEGER"
    assert schema["salary"] == "REAL"

def test_infer_schema_from_df(schema_manager):
    df = pd.DataFrame({"name": ["Alice"], "age": [25], "salary": [10.5]})
    schema = schema_manager.infer_schema_from_df(df)
    assert schema == {"name": "TEXT", "age": "INTEGER", "salary": "REAL"}

def test_generate_create_table_ddl(schema_manager):
    ddl = schema_manager.generate_create_table_ddl(
        "employees", {"name": "TEXT", "age": "INTEGER"}
    )
    assert ddl == "CREATE TABLE employees (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);"

def test_check_schema_compatibility_create(schema_manager):
    df = pd.DataFrame({"name": ["Alice"], "age": [25]})
    result = schema_manager.check_schema_compatibility("employees", df)
    assert result == {"action": "create"}

def test_check_schema_compatibility_append(schema_manager):
    df = pd.DataFrame({"name": ["Alice"], "age": [25], "salary": [10.0]})
    result = schema_manager.check_schema_compatibility("people", df)
    assert result == {"action": "append"}

def test_check_schema_compatibility_conflict(schema_manager):
    df = pd.DataFrame({"name": ["Alice"], "age": [25]})
    result = schema_manager.check_schema_compatibility("people", df)
    assert result == {"action": "conflict"}

def test_get_database_schema_context(schema_manager):
    context = schema_manager.get_database_schema_context()
    assert "The database uses SQLite" in context
    assert "people" in context
    assert "name TEXT" in context
