"""Collects Databricks usage and billing data."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class UsageCollector:
    """Collects usage data from system.billing.usage table."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """
        Initialize usage collector.
        
        Args:
            client: Databricks client instance
            config: Configuration dictionary
        """
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect usage data for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing usage data
        """
        logger.info(f"Collecting usage data from {start_date.date()} to {end_date.date()}")
        
        query = f"""
        SELECT
            usage_date,
            dbu_type,
            workspace_id,
            CAST(usage_quantity AS DECIMAL(18, 2)) as usage_quantity
        FROM system.billing.usage
        WHERE usage_date >= '{start_date.date()}'
            AND usage_date <= '{end_date.date()}'
        ORDER BY usage_date DESC, dbu_type
        """
        
        results = self.client.execute_query(query)
        
        # Aggregate usage by type
        usage_by_type = {}
        total_dbus = 0.0
        
        for row in results:
            dbu_type = row.get("dbu_type", "UNKNOWN")
            quantity = float(row.get("usage_quantity", 0))
            
            if dbu_type not in usage_by_type:
                usage_by_type[dbu_type] = 0.0
            
            usage_by_type[dbu_type] += quantity
            total_dbus += quantity
        
        return {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": (end_date - start_date).days,
            },
            "total_dbus": total_dbus,
            "by_type": usage_by_type,
            "raw_data": results,
        }
