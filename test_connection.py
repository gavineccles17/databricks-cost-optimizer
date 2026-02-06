#!/usr/bin/env python
import os
from databricks import sql

try:
    print("Testing Databricks connection...")
    print(f"Host: {os.getenv('DATABRICKS_HOST')}")
    print(f"Path: {os.getenv('DATABRICKS_HTTP_PATH')}")
    
    conn = sql.connect(
        server_hostname=os.getenv('DATABRICKS_HOST'),
        http_path=os.getenv('DATABRICKS_HTTP_PATH'),
        access_token=os.getenv('DATABRICKS_TOKEN'),
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT 1 as test")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    print("✓ Connection successful! Query returned:", result)
except Exception as e:
    print(f"✗ Connection failed: {e}")
