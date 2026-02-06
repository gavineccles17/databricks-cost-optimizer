"""Collects Databricks job and run data."""

import logging
from datetime import datetime
from typing import Any, Dict

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class JobCollector:
    """Collects job metadata and run history."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize job collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect job data for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing job data
        """
        logger.info("Collecting job data")
        
        # Get job definitions
        jobs_query = """
        SELECT
            job_id,
            settings,
            created_time,
            creator_user_name
        FROM system.jobs.jobs
        """
        
        jobs = self.client.execute_query(jobs_query)
        
        # Get job run history
        runs_query = f"""
        SELECT
            job_id,
            run_id,
            start_time,
            end_time,
            state,
            state_message,
            number_in_job
        FROM system.jobs.job_runs
        WHERE start_time >= {int(start_date.timestamp() * 1000)}
            AND start_time <= {int(end_date.timestamp() * 1000)}
        ORDER BY start_time DESC
        """
        
        runs = self.client.execute_query(runs_query)
        
        return {
            "jobs": jobs,
            "runs": runs,
            "job_count": len(jobs),
            "run_count": len(runs),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
