import pytest
import sqlite3
import pandas as pd
from unittest.mock import MagicMock
import os
from src.csv_handler import CSVIngestor

@pytest.fixture
def in_memory_db():
    """Provides a fresh in-memory SQLite database for each test."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()

@pytest.fixture
def mock_schema_manager():
    """Provides a fake SchemaManager that we can control in our tests."""
    return MagicMock()

@pytest.fixture
def sample_csv_path(tmp_path):
    """Creates a temporary CSV file with sample data for testing."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Age": [25, 30],
        "City ": ["New York", "Boston"] # Intentional trailing space
    })
    
    file_path = tmp_path / "test_data.csv"
    df.to_csv(file_path, index=False)
    return str(file_path)

@pytest.fixture
def csv_ingestor(in_memory_db, mock_schema_manager):
    """Provides a fresh CSVIngestor instance mapped to our mock dependencies."""
    return CSVIngestor(in_memory_db, mock_schema_manager)