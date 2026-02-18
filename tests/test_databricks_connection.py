"""Test Databricks connection using credentials from .env file"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file only
load_dotenv(override=True)


def test_databricks_connection():
    """Test the Databricks connection."""
    # Get credentials from .env file
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")

    try:
        from databricks import sql

        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )

        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM system.information_schema.catalogs LIMIT 5"
        )
        result = cursor.fetchall()

        print("\nQuerying: system.information_schema.catalogs")
        print(f"Found {len(result)} catalogs:")
        for row in result:
            print(f" - {row[0]}")

        cursor.close()
        connection.close()

        print("\n✅ Connection successful")
        return True

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    success = test_databricks_connection()
    sys.exit(0 if success else 1)


def test_table_exists():
    """Test the table_exists method."""
    from src.databricks_client import DatabricksClient
    
    client = DatabricksClient(mock_mode=False)
    
    # Test common tables
    print("\nTesting table existence checks:")
    
    tables_to_check = [
        "system.billing.usage",
        "system.billing.account_prices",
        "system.billing.list_prices",
        "system.compute.clusters",
    ]
    
    for table in tables_to_check:
        exists = client.table_exists(table)
        status = "✅ EXISTS" if exists else "❌ NOT FOUND"
        print(f"  {table}: {status}")
    
    client.close()
    print("\n✅ Table existence checks completed")


if __name__ == "__main__":
    test_databricks_connection()
    print("\n" + "=" * 60)
    test_table_exists()
    sys.exit(0 if success else 1)
