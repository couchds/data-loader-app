import pytest
import os
from unittest.mock import patch
from data_loader.loader import DataLoader

# Two pytest markers used:
# * Unit - these are to be run in CI process
# * Integration - these are meant (FOR NOW) to be run locally

@pytest.fixture(scope="function")
def reset_singleton():
    """DataLoader should be re-instantiated after every test"""
    DataLoader._instance = None # reset singleton

@patch.dict(os.environ, {
    "DB_TYPE": "mysql",
    "DB_HOST": "localhost",
    "DB_PORT": "3306",
    "DB_NAME": "test_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_pass"
})
@pytest.mark.unit
def test_singleton_instance(reset_singleton):
    """Test that DataLoader follows the Singleton pattern."""
    loader1 = DataLoader()
    loader2 = DataLoader()
    assert loader1 is loader2, "DataLoader should be a singleton"

@patch.dict(os.environ, {
    "DB_TYPE": "mysql",
    "DB_HOST": "localhost",
    "DB_PORT": "3306",
    "DB_NAME": "test_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_pass"
})
@pytest.mark.unit
def test_database_url(reset_singleton):
    """Test that the database URL is generated correctly."""
    loader = DataLoader()
    db_url = loader.get_db_url()
    assert "mysql" in db_url or "postgresql" in db_url, "DB URL should be for MySQL or PostgreSQL"

@patch.dict(os.environ, {
    "DB_TYPE": "mysql",
    "DB_HOST": "localhost",
    "DB_PORT": "3306",
    "DB_NAME": "test_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_pass"
})
@pytest.mark.integration
def test_database_connection(reset_singleton):
    """Test that DataLoader initializes a database connection."""
    loader = DataLoader()
    assert loader.engine is not None, "Database engine should be initialized"

    # Ensure engine can connect (without executing queries)
    with loader.engine.connect() as conn:
        assert conn.closed is False, "Database connection should be open"