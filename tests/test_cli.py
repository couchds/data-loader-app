import os
import pytest
import subprocess
import pymysql

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER", "test_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "test_pass")
DB_NAME = os.getenv("DB_NAME", "test_db")

@pytest.mark.integration
def test_cli_data_loader():
    """Test CLI to ensure data is loaded correctly into MySQL."""
    test_csv_path = "tests/data/raw/sales.csv"
    mappings_path = "tests/data/mappings/sales.json"

    # Ensure database is clean before running test
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM sales_table")
    conn.commit()
    conn.close()

    # cli command
    result = subprocess.run(
        ["data_loader", "--data_path", test_csv_path, "--mappings", mappings_path],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0, f"CLI execution failed: {result.stderr}"

    # Verify that data was inserted correctly
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM sales_table")
        row_count = cursor.fetchone()[0]
    conn.close()

    assert row_count == 3, f"Expected 3 rows in database, found {row_count}"