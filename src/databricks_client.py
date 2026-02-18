"""
Databricks client with SQL warehouse and REST API support.
Handles secure authentication and query execution.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from databricks import sql
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class DatabricksConnectionConfig(BaseModel):
    """Configuration for Databricks connection."""
    
    host: str
    token: str
    http_path: str
    user_agent: str = "databricks-cost-optimizer/1.0"


class DatabricksClient:
    """
    Client for querying Databricks with SQL warehouse preference.
    Falls back to REST API if needed.
    """
    
    def __init__(self, mock_mode: bool = False):
        """
        Initialize Databricks client.
        
        Args:
            mock_mode: If True, use synthetic data instead of connecting.
        """
        self.mock_mode = mock_mode
        self.config: Optional[DatabricksConnectionConfig] = None
        self.conn: Optional[sql.Connection] = None
        self._table_exists_cache: Dict[str, bool] = {}  # Cache table existence checks
        
        if not mock_mode:
            self._load_config()
            self._establish_connection()
    
    def _load_config(self) -> None:
        """Load Databricks credentials from environment variables."""
        required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        self.config = DatabricksConnectionConfig(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        )
    
    def _establish_connection(self) -> None:
        """Establish connection to Databricks SQL warehouse."""
        if not self.config:
            return
        
        try:
            self.conn = sql.connect(
                server_hostname=self.config.host,
                http_path=self.config.http_path,
                access_token=self.config.token,
            )
            logger.info("Connected to Databricks SQL warehouse")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {str(e)}")
            raise
    
    def verify_connection(self) -> bool:
        """Verify connection to Databricks is working."""
        if self.mock_mode:
            logger.info("Mock mode: skipping connection verification")
            return True
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            logger.info("Databricks connection verified")
            return True
        except Exception as e:
            logger.error(f"Connection verification failed: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the workspace.
        Results are cached to avoid repeated queries.
        
        Args:
            table_name: Fully qualified table name (e.g., 'system.billing.account_prices')
        
        Returns:
            True if table exists, False otherwise
        """
        # Check cache first
        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]
        
        if self.mock_mode:
            # In mock mode, only account_prices might not exist
            exists = table_name != "system.billing.account_prices"
            self._table_exists_cache[table_name] = exists
            return exists
        
        try:
            # Try to query just 1 row to check existence
            query = f"SELECT 1 FROM {table_name} LIMIT 1"
            cursor = self.conn.cursor()
            cursor.execute(query)
            cursor.fetchone()
            cursor.close()
            
            self._table_exists_cache[table_name] = True
            logger.debug(f"Table {table_name} exists")
            return True
        except Exception as e:
            # Table doesn't exist or no permissions
            self._table_exists_cache[table_name] = False
            logger.debug(f"Table {table_name} not accessible: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query against Databricks.
        
        Args:
            query: SQL query string
        
        Returns:
            List of result rows as dictionaries
        """
        if self.mock_mode:
            return self._get_mock_data(query)
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all results
            rows = cursor.fetchall()
            cursor.close()
            
            # Convert to list of dicts
            results = [dict(zip(columns, row)) for row in rows]
            
            logger.debug(f"Query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}\nQuery: {query}")
            raise
    
    def _get_mock_data(self, query: str) -> List[Dict[str, Any]]:
        """
        Return synthetic data for testing.
        
        Args:
            query: SQL query (used to identify what data is requested)
        
        Returns:
            Mock data matching the query
        """
        query_lower = query.lower()
        
        # Billing queries with pricing join - match the query output columns
        if "system.billing.usage" in query_lower and ("account_prices" in query_lower or "list_prices" in query_lower):
            return [
                {
                    "usage_date": "2025-12-01",
                    "sku_name": "PREMIUM_ALL_PURPOSE_DBU",
                    "billing_origin_product": "ALL_PURPOSE",
                    "workspace_id": 123456,
                    "usage_quantity": 50.0,
                    "usage_unit": "DBU",
                    "cluster_id": "cluster-1",
                    "job_id": None,
                    "warehouse_id": None,
                    "pipeline_id": None,
                    "endpoint_name": None,
                    "run_as_user": "user@example.com",
                    "is_serverless": False,
                    "is_photon": True,
                    "dollar_cost": 25.50,
                },
                {
                    "usage_date": "2025-12-01",
                    "sku_name": "PREMIUM_SQL_SERVER_DBU",
                    "billing_origin_product": "SQL",
                    "workspace_id": 123456,
                    "usage_quantity": 30.0,
                    "usage_unit": "DBU",
                    "cluster_id": None,
                    "job_id": None,
                    "warehouse_id": "wh-1",
                    "pipeline_id": None,
                    "endpoint_name": None,
                    "run_as_user": "analyst@example.com",
                    "is_serverless": True,
                    "is_photon": False,
                    "dollar_cost": 18.00,
                },
                {
                    "usage_date": "2025-12-02",
                    "sku_name": "PREMIUM_JOBS_DBU",
                    "billing_origin_product": "JOBS",
                    "workspace_id": 123456,
                    "usage_quantity": 20.0,
                    "usage_unit": "DBU",
                    "cluster_id": "cluster-2",
                    "job_id": "job-1",
                    "warehouse_id": None,
                    "pipeline_id": None,
                    "endpoint_name": None,
                    "run_as_user": "etl@example.com",
                    "is_serverless": False,
                    "is_photon": False,
                    "dollar_cost": 8.00,
                },
            ]
        elif "system.billing.account_prices" in query_lower:
            return [{"cnt": 1}]  # Account prices exist
        elif "system.compute.warehouses" in query_lower:
            return [
                {
                    "warehouse_id": "wh-1",
                    "name": "SQL Warehouse",
                    "size": "Small",
                    "cluster_size": "Small",
                    "warehouse_type": "PRO",
                    "auto_stop_mins": 10,
                    "state": "RUNNING",
                    "creator_name": "admin@example.com",
                },
                {
                    "warehouse_id": "wh-2", 
                    "name": "Analytics Warehouse",
                    "size": "Medium",
                    "cluster_size": "Medium",
                    "warehouse_type": "CLASSIC",
                    "auto_stop_mins": 0,  # No auto-stop - issue
                    "state": "STOPPED",
                    "creator_name": "admin@example.com",
                },
            ]
        elif "system.compute.warehouse_events" in query_lower:
            return []
        elif "system.compute.clusters" in query_lower:
            return [
                {"cluster_id": "cluster-1", "cluster_name": "prod-cluster", "num_workers": 4},
                {"cluster_id": "cluster-2", "cluster_name": "dev-cluster", "num_workers": 2},
            ]
        elif "system.lakeflow.jobs" in query_lower or "system.jobs.jobs" in query_lower:
            return [
                {"job_id": "job-1", "name": "daily-job", "created_time": 1234567890},
            ]
        elif "system.query.history" in query_lower:
            return [
                {
                    "user_name": "analyst@example.com",
                    "query_count": 25,
                    "avg_duration_seconds": 12.5,
                    "total_bytes_read": 500000000,
                },
                {
                    "user_name": "etl@example.com",
                    "query_count": 10,
                    "avg_duration_seconds": 45.0,
                    "total_bytes_read": 2000000000,
                },
            ]
        
        return []
    
    def close(self) -> None:
        """Close Databricks connection."""
        if self.conn:
            self.conn.close()
            logger.info("Databricks connection closed")
    
    def __del__(self):
        """Ensure connection is closed on deletion."""
        self.close()
