"""Analyzes job execution patterns and efficiency."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class JobAnalyzer:
    """Identifies inefficient job patterns and resource usage."""
    
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
        Analyze job execution patterns, costs, and efficiency.
        
        Args:
            jobs_data: Data from job collector (with cost attribution and run metrics)
            usage_data: Data from usage collector
        
        Returns:
            Job analysis results with efficiency insights
        """
        logger.info("Analyzing jobs...")
        
        jobs = jobs_data.get("jobs", [])
        
        # Categorize jobs by efficiency issues
        high_cost_jobs = []
        serverless_candidates = []
        efficiency_issues = []
        high_failure_jobs = []
        short_run_overhead = []
        variable_duration_jobs = []
        
        for job in jobs:
            job_id = job.get("job_id")
            job_name = job.get("job_name") or str(job_id)
            total_cost = float(job.get("total_cost", 0) or 0)
            total_dbus = float(job.get("total_dbus", 0) or 0)
            run_count = int(job.get("run_count", 0) or 0)
            is_serverless = job.get("is_serverless")
            
            # Efficiency metrics
            avg_duration = float(job.get("avg_duration_seconds", 0) or 0)
            cost_per_run = float(job.get("cost_per_run", 0) or 0)
            failure_rate = float(job.get("failure_rate", 0) or 0)
            duration_variance = float(job.get("duration_variance", 0) or 0)
            short_run = job.get("short_run", False)
            
            # Flag high-cost jobs
            if total_cost > 10:
                high_cost_jobs.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "total_cost": total_cost,
                    "total_dbus": total_dbus,
                    "run_count": run_count,
                    "avg_duration_seconds": avg_duration,
                    "cost_per_run": cost_per_run,
                })
            
            # Identify serverless candidates (non-serverless jobs with many short runs)
            if not is_serverless and run_count and run_count > 10:
                serverless_candidates.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "run_count": run_count,
                    "total_cost": total_cost,
                    "avg_duration_seconds": avg_duration,
                    "reason": "Frequent runs benefit from serverless instant startup",
                })
            
            # High failure rate - wasting money on failed runs
            if failure_rate > 10 and total_cost > 5:
                high_failure_jobs.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "failure_rate": failure_rate,
                    "total_cost": total_cost,
                    "wasted_cost": round(total_cost * (failure_rate / 100), 2),
                    "run_count": run_count,
                })
                efficiency_issues.append({
                    "type": "high_failure_rate",
                    "job_id": job_id,
                    "job_name": job_name,
                    "severity": "high" if failure_rate > 25 else "medium",
                    "description": f"Job has {failure_rate:.1f}% failure rate - wasting ~${total_cost * (failure_rate / 100):.2f} on failed runs",
                    "failure_rate": failure_rate,
                    "wasted_cost": round(total_cost * (failure_rate / 100), 2),
                })
            
            # Short runs with high overhead - cluster startup cost dominates
            if short_run and cost_per_run > 0.10:
                short_run_overhead.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "avg_duration_seconds": avg_duration,
                    "cost_per_run": cost_per_run,
                    "run_count": run_count,
                    "total_cost": total_cost,
                })
                efficiency_issues.append({
                    "type": "startup_overhead",
                    "job_id": job_id,
                    "job_name": job_name,
                    "severity": "medium",
                    "description": f"Job runs for only {avg_duration:.0f}s but costs ${cost_per_run:.2f}/run - cluster startup overhead dominates",
                    "avg_duration": avg_duration,
                    "cost_per_run": cost_per_run,
                    "recommendation": "Use cluster pools or serverless to reduce startup time",
                })
            
            # Highly variable duration - potential resource contention or data skew
            if duration_variance > 300 and run_count > 5:  # >5 min variance
                variable_duration_jobs.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "min_duration": job.get("min_duration_seconds", 0),
                    "max_duration": job.get("max_duration_seconds", 0),
                    "avg_duration": avg_duration,
                    "variance": duration_variance,
                })
                efficiency_issues.append({
                    "type": "variable_duration",
                    "job_id": job_id,
                    "job_name": job_name,
                    "severity": "low",
                    "description": f"Job duration varies by {duration_variance/60:.0f} minutes - may indicate data skew or resource contention",
                    "min_duration": job.get("min_duration_seconds", 0),
                    "max_duration": job.get("max_duration_seconds", 0),
                })
        
        # Calculate aggregate metrics
        total_job_cost = sum(float(j.get("total_cost", 0) or 0) for j in jobs)
        total_wasted_on_failures = sum(j.get("wasted_cost", 0) for j in high_failure_jobs)
        
        return {
            "job_count": len(jobs),
            "jobs": jobs,
            "high_cost_jobs": sorted(high_cost_jobs, key=lambda x: x["total_cost"], reverse=True),
            "serverless_candidates": serverless_candidates,
            "efficiency_issues": efficiency_issues,
            "high_failure_jobs": high_failure_jobs,
            "short_run_overhead_jobs": short_run_overhead,
            "variable_duration_jobs": variable_duration_jobs,
            # Summary metrics
            "total_job_cost": round(total_job_cost, 2),
            "total_wasted_on_failures": round(total_wasted_on_failures, 2),
            "jobs_with_issues": len(set(i["job_id"] for i in efficiency_issues)),
        }
