"""Collects Databricks job and run data."""

import logging
from datetime import datetime
from typing import Any, Dict

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class JobCollector:
    """Collects job metadata and cost attribution."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize job collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect job data with cost attribution for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing job data with costs
        """
        logger.info("Collecting job data")
        
        # Get job cost attribution from billing - join with job names
        job_costs_query = f"""
        WITH jobs AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs 
            QUALIFY rn = 1
        )
        SELECT
            usage.usage_metadata.job_id as job_id,
            COALESCE(usage.usage_metadata.job_name, jobs.name) as job_name,
            jobs.creator_user_name as owner,
            usage.sku_name,
            usage.product_features.is_serverless as is_serverless,
            SUM(usage.usage_quantity) as total_dbus,
            SUM(usage.usage_quantity * lp.pricing.effective_list.default) as total_cost,
            COUNT(DISTINCT usage.usage_metadata.job_run_id) as run_count,
            MIN(usage.usage_start_time) as first_run,
            MAX(usage.usage_end_time) as last_run
        FROM system.billing.usage usage
        JOIN system.billing.list_prices lp ON lp.sku_name = usage.sku_name
        LEFT JOIN jobs ON usage.workspace_id = jobs.workspace_id 
            AND usage.usage_metadata.job_id = jobs.job_id
        WHERE usage.usage_metadata.job_id IS NOT NULL
            AND usage.usage_end_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR usage.usage_end_time < lp.price_end_time)
            AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY total_cost DESC
        LIMIT 100
        """
        
        job_costs = []
        try:
            job_costs = self.client.execute_query(job_costs_query)
            logger.info(f"Job costs query returned {len(job_costs)} jobs with usage")
            if job_costs:
                logger.info(f"Sample job cost record: {job_costs[0]}")
        except Exception as e:
            logger.warning(f"Could not fetch job costs: {str(e)}")
            # Fallback to simpler query without system.lakeflow.jobs join
            try:
                fallback_query = f"""
                SELECT
                    usage.usage_metadata.job_id as job_id,
                    usage.usage_metadata.job_name as job_name,
                    NULL as owner,
                    usage.sku_name,
                    usage.product_features.is_serverless as is_serverless,
                    SUM(usage.usage_quantity) as total_dbus,
                    SUM(usage.usage_quantity * lp.pricing.effective_list.default) as total_cost,
                    COUNT(DISTINCT usage.usage_metadata.job_run_id) as run_count,
                    MIN(usage.usage_start_time) as first_run,
                    MAX(usage.usage_end_time) as last_run
                FROM system.billing.usage usage
                JOIN system.billing.list_prices lp ON lp.sku_name = usage.sku_name
                WHERE usage.usage_metadata.job_id IS NOT NULL
                    AND usage.usage_end_time >= lp.price_start_time
                    AND (lp.price_end_time IS NULL OR usage.usage_end_time < lp.price_end_time)
                    AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY total_cost DESC
                LIMIT 100
                """
                job_costs = self.client.execute_query(fallback_query)
                logger.info(f"Fallback job query returned {len(job_costs)} jobs")
            except Exception as e2:
                logger.warning(f"Fallback job query also failed: {str(e2)}")
        
        return {
            "jobs": job_costs,
            "job_count": len(job_costs),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
