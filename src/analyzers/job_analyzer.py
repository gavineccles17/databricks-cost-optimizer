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
        Analyze job execution patterns.
        
        Args:
            jobs_data: Data from job collector
            usage_data: Data from usage collector
        
        Returns:
            Job analysis results
        """
        logger.info("Analyzing jobs...")
        
        jobs = jobs_data.get("jobs", [])
        runs = jobs_data.get("runs", [])
        
        # Analyze run patterns
        long_running_jobs = []
        failed_runs = []
        
        for run in runs:
            job_id = run.get("job_id")
            start_time = run.get("start_time")
            end_time = run.get("end_time")
            state = run.get("state", "").upper()
            
            # Check for long-running jobs
            if start_time and end_time:
                duration_ms = end_time - start_time
                duration_seconds = duration_ms / 1000
                
                if duration_seconds > self.long_query_threshold:
                    long_running_jobs.append({
                        "job_id": job_id,
                        "duration_seconds": duration_seconds,
                        "severity": "medium",
                    })
            
            # Check for failed runs
            if state == "FAILED":
                failed_runs.append({
                    "job_id": job_id,
                    "state": state,
                    "message": run.get("state_message"),
                })
        
        return {
            "job_count": len(jobs),
            "run_count": len(runs),
            "long_running_jobs": long_running_jobs,
            "failed_runs": failed_runs,
        }
