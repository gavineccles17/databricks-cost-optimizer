"""Collects Databricks cluster configuration and event data."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class ClusterCollector:
    """Collects cluster metadata and usage attribution."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize cluster collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect cluster data with cost attribution for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing cluster data
        """
        logger.info("Collecting cluster data")
        
        # Get cluster configurations from system.compute.clusters
        clusters_query = """
        SELECT
            cluster_id,
            cluster_name,
            owned_by,
            create_time,
            delete_time,
            driver_node_type,
            worker_node_type,
            worker_count,
            min_autoscale_workers,
            max_autoscale_workers,
            auto_termination_minutes,
            enable_elastic_disk,
            cluster_source,
            dbr_version,
            change_time
        FROM system.compute.clusters
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
        """
        
        clusters = []
        try:
            clusters = self.client.execute_query(clusters_query)
            logger.info(f"Cluster query returned {len(clusters)} clusters")
            if clusters:
                logger.info(f"Sample cluster: {clusters[0]}")
        except Exception as e:
            logger.warning(f"Could not fetch cluster data: {str(e)}")
        
        # Get cluster cost attribution from billing
        cluster_costs_query = f"""
        SELECT
            u.usage_metadata.cluster_id as cluster_id,
            c.cluster_name,
            c.owned_by as owner,
            SUM(u.usage_quantity) as total_dbus,
            SUM(u.usage_quantity * lp.pricing.effective_list.default) as total_cost
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp ON lp.sku_name = u.sku_name
        LEFT JOIN (
            SELECT cluster_id, cluster_name, owned_by, change_time
            FROM system.compute.clusters
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
        ) c ON u.usage_metadata.cluster_id = c.cluster_id
        WHERE u.usage_metadata.cluster_id IS NOT NULL
            AND u.usage_end_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
            AND u.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        GROUP BY 1, 2, 3
        ORDER BY total_cost DESC
        LIMIT 50
        """
        
        cluster_costs = []
        try:
            cluster_costs = self.client.execute_query(cluster_costs_query)
            logger.info(f"Cluster costs query returned {len(cluster_costs)} clusters with usage")
        except Exception as e:
            logger.warning(f"Could not fetch cluster costs: {str(e)}")
        
        return {
            "clusters": clusters,
            "cluster_costs": cluster_costs,
            "cluster_count": len(clusters),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
