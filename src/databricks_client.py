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
        # Return empty list or minimal synthetic data based on query
        if "system.billing.usage" in query:
            return [
                {"dbu_type": "COMPUTE", "workspace_id": 123456, "usage_quantity": 100.0},
                {"dbu_type": "SQL", "workspace_id": 123456, "usage_quantity": 50.0},
            ]
        elif "system.compute.clusters" in query:
            return [
                {"cluster_id": "cluster-1", "cluster_name": "prod-cluster", "num_workers": 4},
                {"cluster_id": "cluster-2", "cluster_name": "dev-cluster", "num_workers": 2},
            ]
        elif "system.jobs.jobs" in query:
            return [
                {"job_id": 1, "settings": {"name": "daily-job"}, "created_time": 1234567890},
            ]
        elif "system.query.history" in query:
            return [
                {"query_id": "q1", "query_text": "SELECT 1", "duration_ms": 100},
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
