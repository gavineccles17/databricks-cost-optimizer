"""Collects Databricks job and run data with efficiency metrics."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class JobCollector:
    """Collects job metadata, cost attribution, and efficiency metrics."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize job collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect job data with cost attribution and efficiency metrics.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing job data with costs and efficiency metrics
        """
        logger.info("Collecting job data")
        
        job_costs = self._collect_job_costs(start_date, end_date)
        job_run_metrics = self._collect_job_run_metrics(start_date, end_date)
        
        # Enrich jobs with run metrics
        jobs_enriched = self._enrich_jobs_with_metrics(job_costs, job_run_metrics)
        
        return {
            "jobs": jobs_enriched,
            "job_count": len(jobs_enriched),
            "job_run_metrics": job_run_metrics,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
    
    def _collect_job_costs(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get job cost attribution from billing."""
        job_costs_query = f"""
        WITH jobs AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs 
            QUALIFY rn = 1
        ),
        job_clusters AS (
            SELECT 
                cluster_id,
                cluster_name,
                COALESCE(
                    aws_attributes.availability,
                    azure_attributes.availability,
                    gcp_attributes.availability
                ) as availability,
                CASE
                    WHEN aws_attributes.availability IN ('SPOT', 'SPOT_WITH_FALLBACK') THEN true
                    WHEN azure_attributes.availability IN ('SPOT_AZURE', 'SPOT_WITH_FALLBACK_AZURE') THEN true
                    WHEN gcp_attributes.availability IN ('PREEMPTIBLE_GCP', 'PREEMPTIBLE_WITH_FALLBACK_GCP') THEN true
                    ELSE false
                END as uses_spot
            FROM system.compute.clusters
            WHERE cluster_name RLIKE '^job-[0-9]+-run-[0-9]+'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
        )
        SELECT
            usage.usage_metadata.job_id as job_id,
            COALESCE(usage.usage_metadata.job_name, jobs.name) as job_name,
            jobs.creator_user_name as owner,
            usage.sku_name,
            usage.product_features.is_serverless as is_serverless,
            MAX(jc.uses_spot) as uses_spot,
            SUM(usage.usage_quantity) as total_dbus,
            SUM(usage.usage_quantity * lp.pricing.effective_list.default) as total_cost,
            SUM(CASE WHEN jc.uses_spot = true THEN usage.usage_quantity * lp.pricing.effective_list.default ELSE 0 END) as spot_cost,
            SUM(CASE WHEN jc.uses_spot = false OR jc.uses_spot IS NULL THEN usage.usage_quantity * lp.pricing.effective_list.default ELSE 0 END) as on_demand_cost,
            COUNT(DISTINCT usage.usage_metadata.job_run_id) as run_count,
            MIN(usage.usage_start_time) as first_run,
            MAX(usage.usage_end_time) as last_run
        FROM system.billing.usage usage
        JOIN system.billing.list_prices lp ON lp.sku_name = usage.sku_name
        LEFT JOIN jobs ON usage.workspace_id = jobs.workspace_id 
            AND usage.usage_metadata.job_id = jobs.job_id
        LEFT JOIN job_clusters jc ON usage.usage_metadata.cluster_id = jc.cluster_id
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
            job_costs = self._fallback_job_costs(start_date, end_date)
        
        return job_costs
    
    def _fallback_job_costs(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fallback query without system.lakeflow.jobs join."""
        try:
            fallback_query = f"""
            WITH job_clusters AS (
                SELECT 
                    cluster_id,
                    cluster_name,
                    COALESCE(
                        aws_attributes.availability,
                        azure_attributes.availability,
                        gcp_attributes.availability
                    ) as availability,
                    CASE
                        WHEN aws_attributes.availability IN ('SPOT', 'SPOT_WITH_FALLBACK') THEN true
                        WHEN azure_attributes.availability IN ('SPOT_AZURE', 'SPOT_WITH_FALLBACK_AZURE') THEN true
                        WHEN gcp_attributes.availability IN ('PREEMPTIBLE_GCP', 'PREEMPTIBLE_WITH_FALLBACK_GCP') THEN true
                        ELSE false
                    END as uses_spot
                FROM system.compute.clusters
                WHERE cluster_name RLIKE '^job-[0-9]+-run-[0-9]+'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            )
            SELECT
                usage.usage_metadata.job_id as job_id,
                usage.usage_metadata.job_name as job_name,
                NULL as owner,
                usage.sku_name,
                usage.product_features.is_serverless as is_serverless,
                MAX(jc.uses_spot) as uses_spot,
                SUM(usage.usage_quantity) as total_dbus,
                SUM(usage.usage_quantity * lp.pricing.effective_list.default) as total_cost,
                SUM(CASE WHEN jc.uses_spot = true THEN usage.usage_quantity * lp.pricing.effective_list.default ELSE 0 END) as spot_cost,
                SUM(CASE WHEN jc.uses_spot = false OR jc.uses_spot IS NULL THEN usage.usage_quantity * lp.pricing.effective_list.default ELSE 0 END) as on_demand_cost,
                COUNT(DISTINCT usage.usage_metadata.job_run_id) as run_count,
                MIN(usage.usage_start_time) as first_run,
                MAX(usage.usage_end_time) as last_run
            FROM system.billing.usage usage
            JOIN system.billing.list_prices lp ON lp.sku_name = usage.sku_name
            LEFT JOIN job_clusters jc ON usage.usage_metadata.cluster_id = jc.cluster_id
            WHERE usage.usage_metadata.job_id IS NOT NULL
                AND usage.usage_end_time >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR usage.usage_end_time < lp.price_end_time)
                AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY total_cost DESC
            LIMIT 100
            """
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
            return job_costs
        except Exception as e2:
            logger.warning(f"Fallback job query also failed: {str(e2)}")
            return []
    
    def _collect_job_run_metrics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Collect job run-level metrics for efficiency analysis.
        Uses system.lakeflow.job_run_timeline for duration and cluster info.
        """
        job_run_query = f"""
        SELECT
            job_id,
            run_id,
            run_name,
            result_state,
            TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) as duration_seconds,
            compute_ids,
            period_start_time,
            period_end_time
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= '{start_date.date()}'
            AND period_end_time <= '{end_date.date()}'
            AND result_state IS NOT NULL
        ORDER BY job_id, period_start_time DESC
        """
        
        try:
            run_metrics = self.client.execute_query(job_run_query)
            logger.info(f"Job run metrics query returned {len(run_metrics)} runs")
            return run_metrics
        except Exception as e:
            logger.warning(f"Could not fetch job run metrics: {str(e)}")
            return []
    
    def _enrich_jobs_with_metrics(self, job_costs: List[Dict], job_run_metrics: List[Dict]) -> List[Dict]:
        """Enrich job cost data with efficiency metrics from run timeline."""
        
        # Aggregate run metrics by job_id
        job_metrics = {}
        for run in job_run_metrics:
            job_id = str(run.get("job_id"))
            if job_id not in job_metrics:
                job_metrics[job_id] = {
                    "durations": [],
                    "success_count": 0,
                    "failure_count": 0,
                }
            
            duration = run.get("duration_seconds") or 0
            if duration > 0:
                job_metrics[job_id]["durations"].append(duration)
            
            result_state = str(run.get("result_state", "")).upper()
            if result_state == "SUCCESS":
                job_metrics[job_id]["success_count"] += 1
            elif result_state in ("FAILED", "TIMEDOUT", "CANCELED"):
                job_metrics[job_id]["failure_count"] += 1
        
        # Enrich job costs with metrics
        enriched = []
        for job in job_costs:
            job_id = str(job.get("job_id"))
            metrics = job_metrics.get(job_id, {})
            
            durations = metrics.get("durations", [])
            avg_duration = sum(durations) / len(durations) if durations else 0
            min_duration = min(durations) if durations else 0
            max_duration = max(durations) if durations else 0
            
            total_cost = float(job.get("total_cost", 0) or 0)
            run_count = int(job.get("run_count", 0) or 0)
            cost_per_run = total_cost / run_count if run_count > 0 else 0
            
            success_count = metrics.get("success_count", 0)
            failure_count = metrics.get("failure_count", 0)
            total_runs = success_count + failure_count
            failure_rate = failure_count / total_runs if total_runs > 0 else 0
            
            enriched_job = dict(job)
            enriched_job.update({
                "avg_duration_seconds": round(avg_duration, 1),
                "min_duration_seconds": round(min_duration, 1),
                "max_duration_seconds": round(max_duration, 1),
                "cost_per_run": round(cost_per_run, 4),
                "success_count": success_count,
                "failure_count": failure_count,
                "failure_rate": round(failure_rate * 100, 1),
                # Efficiency indicators
                "duration_variance": round(max_duration - min_duration, 1) if durations else 0,
                "short_run": avg_duration < 60 and total_cost > 1,  # Short runs with significant cost = startup overhead
            })
            enriched.append(enriched_job)
        
        return enriched
