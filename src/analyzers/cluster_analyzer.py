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
        
        issues = []
        
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id", "unknown")
            cluster_name = cluster.get("cluster_name", cluster_id)
            
            # Check for missing auto-termination
            autotermination_min = cluster.get("autotermination_minutes")
            if autotermination_min is None or autotermination_min == 0:
                issues.append({
                    "type": "no_autotermination",
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "severity": "high",
                    "description": "Cluster has no auto-termination configured",
                })
            
            # Check for high worker count (potentially over-provisioned)
            num_workers = cluster.get("num_workers", 0)
            if num_workers > 8:
                issues.append({
                    "type": "over_provisioned",
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "severity": "medium",
                    "description": f"Cluster has {num_workers} workers (consider right-sizing)",
                    "current_workers": num_workers,
                })
        
        return {
            "cluster_count": len(clusters),
            "issues": issues,
            "issue_count": len(issues),
        }
