"""Collects Databricks cluster configuration and event data."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class ClusterCollector:
    """Collects cluster metadata and event data."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize cluster collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect cluster data for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing cluster data
        """
        logger.info("Collecting cluster data")
        
        # Get cluster configurations
        clusters_query = """
        SELECT
            cluster_id,
            cluster_name,
            creator_user_name,
            spark_version,
            driver_node_type_id,
            node_type_id,
            num_workers,
            autotermination_minutes,
            enable_elastic_disk,
            init_scripts_safe_mode,
            cluster_source,
            created_timestamp
        FROM system.compute.clusters
        """
        
        clusters = self.client.execute_query(clusters_query)
        
        # Get cluster events to assess uptime
        events_query = f"""
        SELECT
            cluster_id,
            timestamp,
            event_type,
            details
        FROM system.compute.cluster_events
        WHERE timestamp >= '{start_date.isoformat()}'
            AND timestamp <= '{end_date.isoformat()}'
        ORDER BY timestamp DESC
        """
        
        events = self.client.execute_query(events_query)
        
        return {
            "clusters": clusters,
            "events": events,
            "cluster_count": len(clusters),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
