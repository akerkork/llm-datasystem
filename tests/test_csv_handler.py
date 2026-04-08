import sqlite3
from unittest.mock import MagicMock
import pandas as pd
import pytest
from src.csv_handler import CSVIngestor



@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def mock_schema_manager():
    return MagicMock()


@pytest.fixture
def sample_csv_path(tmp_path):
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
            "City ": ["New York", "Boston"],
        }
    )
    file_path = tmp_path / "test_data.csv"
    df.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture
def csv_ingestor(in_memory_db, mock_schema_manager):
    return CSVIngestor(in_memory_db, mock_schema_manager)


def test_load_csv_reads_existing_file(csv_ingestor, sample_csv_path):
    df = csv_ingestor._load_csv(sample_csv_path)
    assert list(df.columns) == ["Name", "Age", "City "]
    assert len(df) == 2


def test_load_csv_raises_for_missing_file(csv_ingestor):
    with pytest.raises(FileNotFoundError):
        csv_ingestor._load_csv("does_not_exist.csv")



def test_validate_dataframe_normalizes_columns_and_missing_values(csv_ingestor):
    df = pd.DataFrame({"First Name": ["Alice", None], "Last-Name": ["Smith", "Jones"]})

    result = csv_ingestor._validate_dataframe(df)

    assert result is True
    assert list(df.columns) == ["first_name", "last_name"]
    assert df.iloc[1, 0] is None



def test_validate_dataframe_rejects_empty_dataframe(csv_ingestor):
    with pytest.raises(ValueError):
        csv_ingestor._validate_dataframe(pd.DataFrame())



def test_process_file_returns_conflict_status(csv_ingestor, mock_schema_manager, sample_csv_path):
    mock_schema_manager.check_schema_compatibility.return_value = {"action": "conflict"}

    result = csv_ingestor.process_file(sample_csv_path, "people")

    assert result == {"status": "conflict", "message": "Schema conflict detected."}



def test_process_file_skip_returns_success(csv_ingestor, sample_csv_path):
    result = csv_ingestor.process_file(sample_csv_path, "people", conflict_resolution="skip")
    assert result["status"] == "success"
    assert "skipped" in result["message"].lower()



def test_process_file_append_inserts_rows(in_memory_db, mock_schema_manager, sample_csv_path):
    in_memory_db.execute("CREATE TABLE people (name TEXT, age INTEGER, city TEXT)")
    in_memory_db.commit()

    mock_schema_manager.check_schema_compatibility.return_value = {"action": "append"}
    ingestor = CSVIngestor(in_memory_db, mock_schema_manager)

    result = ingestor.process_file(sample_csv_path, "people")

    assert result["status"] == "success"
    assert result["rows_ingested"] == 2

    rows = in_memory_db.execute("SELECT name, age, city FROM people ORDER BY name").fetchall()
    assert rows == [("Alice", 25, "New York"), ("Bob", 30, "Boston")]



def test_process_file_overwrite_drops_and_recreates_table(
    in_memory_db, mock_schema_manager, sample_csv_path
):
    in_memory_db.execute("CREATE TABLE people (old_col TEXT)")
    in_memory_db.execute("INSERT INTO people (old_col) VALUES ('stale')")
    in_memory_db.commit()

    mock_schema_manager.check_schema_compatibility.return_value = {"action": "conflict"}
    mock_schema_manager.infer_schema_from_df.return_value = {
        "name": "TEXT",
        "age": "INTEGER",
        "city": "TEXT",
    }
    mock_schema_manager.generate_create_table_ddl.return_value = (
        "CREATE TABLE people (name TEXT, age INTEGER, city TEXT)"
    )

    ingestor = CSVIngestor(in_memory_db, mock_schema_manager)
    result = ingestor.process_file(sample_csv_path, "people", conflict_resolution="overwrite")

    assert result["status"] == "success"
    rows = in_memory_db.execute("SELECT name, age, city FROM people ORDER BY name").fetchall()
    assert rows == [("Alice", 25, "New York"), ("Bob", 30, "Boston")]
