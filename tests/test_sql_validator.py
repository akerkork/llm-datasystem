from unittest.mock import MagicMock
import pytest
from src.sql_validator import SQLValidator

@pytest.fixture
def mock_schema_manager():
    manager = MagicMock()
    manager.get_existing_tables.return_value = ["people", "orders"]
    manager.get_table_schema.side_effect = lambda table: {
        "people": {"id": "INTEGER", "name": "TEXT", "age": "INTEGER"},
        "orders": {"id": "INTEGER", "person_id": "INTEGER", "total": "REAL"},
    }[table]
    return manager

@pytest.fixture
def validator(mock_schema_manager):
    return SQLValidator(mock_schema_manager)

def test_validate_query_accepts_valid_select(validator):
    assert validator.validate_query("SELECT name, age FROM people") is True

def test_validate_query_rejects_empty_query(validator):
    with pytest.raises(ValueError, match="cannot be empty"):
        validator.validate_query("   ")

def test_validate_query_rejects_non_select(validator):
    with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
        validator.validate_query("DELETE FROM people")

def test_validate_query_rejects_unknown_table(validator):
    with pytest.raises(ValueError, match="Unknown table"):
        validator.validate_query("SELECT name FROM missing_table")

def test_validate_query_rejects_unknown_column(validator):
    with pytest.raises(ValueError, match="Unknown column"):
        validator.validate_query("SELECT missing_column FROM people")

def test_extract_tables_finds_from_and_join_tables(validator):
    tables = validator._extract_tables(
        "SELECT people.name, orders.total FROM people JOIN orders ON people.id = orders.person_id"
    )
    assert set(tables) == {"people", "orders"}

def test_extract_columns_handles_functions_and_aliases(validator):
    columns = validator._extract_columns(
        "SELECT people.name AS customer_name, COUNT(orders.id) AS order_count FROM people JOIN orders ON people.id = orders.person_id"
    )
    assert set(columns) == {"name", "id"}
