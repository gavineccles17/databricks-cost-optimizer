"""Analyzes job execution patterns and efficiency."""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class JobAnalyzer:
    """Identifies inefficient job patterns."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize job analyzer."""
        self.config = config
        self.long_query_threshold = config.get("thresholds", {}).get("long_query_threshold_seconds", 3600)
    
    def analyze(
        self,
        jobs_data: Dict[str, Any],
        usage_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Analyze job execution patterns and costs.
        
        Args:
            jobs_data: Data from job collector (with cost attribution)
            usage_data: Data from usage collector
        
        Returns:
            Job analysis results
        """
        logger.info("Analyzing jobs...")
        
        jobs = jobs_data.get("jobs", [])
        
        # Analyze job cost patterns
        high_cost_jobs = []
        serverless_candidates = []
        
        for job in jobs:
            job_id = job.get("job_id")
            job_name = job.get("job_name") or job_id
            total_cost = job.get("total_cost", 0)
            total_dbus = job.get("total_dbus", 0)
            run_count = job.get("run_count", 0)
            is_serverless = job.get("is_serverless")
            
            # Flag high-cost jobs
            if total_cost > 10:  # More than $10 in the period
                high_cost_jobs.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "total_cost": total_cost,
                    "total_dbus": total_dbus,
                    "run_count": run_count,
                })
            
            # Identify serverless candidates (non-serverless jobs with many runs)
            if not is_serverless and run_count and run_count > 10:
                serverless_candidates.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "run_count": run_count,
                    "total_cost": total_cost,
                })
        
        return {
            "job_count": len(jobs),
            "jobs": jobs,
            "high_cost_jobs": high_cost_jobs,
            "serverless_candidates": serverless_candidates,
        }
