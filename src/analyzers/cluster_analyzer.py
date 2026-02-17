"""Analyzes cluster efficiency and configuration."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class ClusterAnalyzer:
    """Identifies inefficient cluster configurations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize cluster analyzer."""
        self.config = config
        self.idle_threshold = config.get("thresholds", {}).get("idle_threshold_minutes", 30)
        self.always_on_threshold = config.get("thresholds", {}).get("always_on_threshold_percent", 80)
    
    def analyze(
        self,
        clusters_data: Dict[str, Any],
        usage_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Analyze cluster efficiency.
        
        Args:
            clusters_data: Data from cluster collector
            usage_data: Data from usage collector
        
        Returns:
            Cluster analysis results
        """
        logger.info("Analyzing clusters...")
        
        clusters = clusters_data.get("clusters", [])
        cluster_costs = clusters_data.get("cluster_costs", [])
        
        # Build cost lookup
        cost_by_cluster = {}
        for cc in cluster_costs:
            cid = cc.get("cluster_id")
            if cid:
                cost_by_cluster[cid] = {
                    "total_cost": float(cc.get("total_cost", 0) or 0),
                    "total_dbus": float(cc.get("total_dbus", 0) or 0),
                    "cluster_name": cc.get("cluster_name"),
                    "owner": cc.get("owner"),
                }
        
        issues = []
        
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id", "unknown")
            cluster_name = cluster.get("cluster_name", cluster_id)
            cost_info = cost_by_cluster.get(cluster_id, {})
            cluster_cost = cost_info.get("total_cost", 0)
            
            # Check for missing auto-termination
            auto_termination_minutes = cluster.get("auto_termination_minutes")
            if auto_termination_minutes is not None:
                if auto_termination_minutes == 0 or str(auto_termination_minutes) == "0":
                    issues.append({
                        "type": "no_autotermination",
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                        "severity": "high",
                        "description": "Cluster has no auto-termination configured - will run indefinitely",
                        "cost": cluster_cost,
                    })
            
            # Check for no autoscaling (fixed worker count)
            worker_count = cluster.get("worker_count")
            min_autoscale = cluster.get("min_autoscale_workers")
            max_autoscale = cluster.get("max_autoscale_workers")
            
            if worker_count and not min_autoscale and not max_autoscale:
                issues.append({
                    "type": "no_autoscaling",
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "severity": "medium",
                    "description": f"Fixed-size cluster with {worker_count} workers - autoscaling would reduce costs during low usage",
                    "cost": cluster_cost,
                    "worker_count": worker_count,
                })
            
            # Check for oversized clusters
            workers = worker_count or max_autoscale or 0
            if workers:
                workers = int(workers)
                if workers >= 20:
                    issues.append({
                        "type": "oversized",
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                        "severity": "high",
                        "description": f"Large cluster with {workers} workers - verify this capacity is utilized",
                        "cost": cluster_cost,
                        "worker_count": workers,
                    })
                elif workers >= 10:
                    issues.append({
                        "type": "oversized",
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                        "severity": "medium",
                        "description": f"Cluster configured with {workers} workers - review utilization",
                        "cost": cluster_cost,
                        "worker_count": workers,
                    })
            
            # Check autoscaling configuration (min too high)
            if min_autoscale and int(min_autoscale) > 2:
                issues.append({
                    "type": "high_min_workers",
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "severity": "low",
                    "description": f"Autoscaling min_workers={min_autoscale} - consider min=1 to save during idle",
                    "cost": cluster_cost,
                    "min_workers": min_autoscale,
                })
        
        # Check for clusters with costs but no config (may be deleted or serverless)
        for cluster_id, cost_info in cost_by_cluster.items():
            if cluster_id not in [c.get("cluster_id") for c in clusters]:
                # This cluster has costs but no config - might be serverless job cluster
                if cost_info.get("total_cost", 0) > 0:
                    logger.debug(f"Cluster {cluster_id} has costs but no config - likely job/serverless cluster")
        
        return {
            "cluster_count": len(clusters),
            "cluster_costs": cluster_costs,
            "issues": issues,
            "issue_count": len(issues),
        }
