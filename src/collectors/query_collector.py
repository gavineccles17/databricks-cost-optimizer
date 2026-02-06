"""Collects Databricks SQL query history and patterns."""

import logging
from datetime import datetime
from typing import Any, Dict

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class QueryCollector:
    """Collects SQL query history and execution patterns."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize query collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect query history for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing query data
        """
        logger.info("Collecting query history")
        
        query = f"""
        SELECT
            query_id,
            user_id,
            query_text,
            duration_ms,
            statement_type,
            rows_produced,
            executed_as_user_name,
            statement_start_time,
            execution_status
        FROM system.query.history
        WHERE statement_start_time >= '{start_date.isoformat()}'
            AND statement_start_time <= '{end_date.isoformat()}'
        ORDER BY statement_start_time DESC
        LIMIT 100000
        """
        
        queries = self.client.execute_query(query)
        
        return {
            "queries": queries,
            "query_count": len(queries),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
