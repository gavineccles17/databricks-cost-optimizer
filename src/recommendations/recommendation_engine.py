"""Generates actionable cost optimization recommendations."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """Generates recommendations from analysis data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize recommendation engine."""
        self.config = config
        self.confidence_factor = config.get("confidence_factor", 0.75)
    
    def generate(
        self,
        cost_analysis: Dict[str, Any],
        cluster_analysis: Dict[str, Any],
        job_analysis: Dict[str, Any],
        sql_analysis: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Generate recommendations from analysis results.
        
        Args:
            cost_analysis: Results from cost analyzer
            cluster_analysis: Results from cluster analyzer
            job_analysis: Results from job analyzer
            sql_analysis: Results from SQL analyzer
        
        Returns:
            List of recommendations
        """
        logger.info("Generating recommendations...")
        
        recommendations = []
        
        # Cluster recommendations
        for issue in cluster_analysis.get("issues", []):
            if issue["type"] == "no_autotermination":
                recommendations.append({
                    "id": f"rec_001_{issue['cluster_id']}",
                    "title": f"Enable auto-termination on {issue['cluster_name']}",
                    "severity": "high",
                    "category": "cluster",
                    "description": "Automatically terminate idle clusters to reduce wasted spend",
                    "estimated_savings": self._estimate_savings(cost_analysis),
                    "steps": [
                        "Open cluster configuration in Databricks UI",
                        "Enable auto-termination (recommend 60 minutes)",
                        "Verify setting is applied",
                    ],
                })
            
            elif issue["type"] == "over_provisioned":
                savings = issue["current_workers"] * 50 * self.confidence_factor
                recommendations.append({
                    "id": f"rec_002_{issue['cluster_id']}",
                    "title": f"Right-size {issue['cluster_name']} - reduce worker count",
                    "severity": "medium",
                    "category": "cluster",
                    "description": f"Cluster has {issue['current_workers']} workers. Consider reducing based on actual usage.",
                    "estimated_savings": round(savings, 2),
                    "steps": [
                        "Monitor cluster utilization for 1 week",
                        "Determine optimal worker count based on workload",
                        "Gradually reduce worker count and validate performance",
                    ],
                })
        
        # SQL optimization recommendations
        pattern_count = sql_analysis.get("pattern_count", 0)
        if pattern_count > 0:
            savings = pattern_count * 20 * self.confidence_factor
            recommendations.append({
                "id": "rec_003",
                "title": "Optimize inefficient SQL queries",
                "severity": "medium",
                "category": "sql",
                "description": f"Found {pattern_count} queries with inefficient patterns (SELECT *, missing WHERE, etc)",
                "estimated_savings": round(savings, 2),
                "steps": [
                    "Review detected query patterns in detailed report",
                    "Add WHERE clauses to full table scans",
                    "Replace SELECT * with specific columns",
                    "Test performance improvements",
                ],
            })
        
        # Cost awareness recommendation
        recommendations.append({
            "id": "rec_004",
            "title": "Implement cost tracking and budgets",
            "severity": "low",
            "category": "governance",
            "description": "Set up workspace alerts and department cost allocation",
            "estimated_savings": 0,
            "steps": [
                "Enable cost tracking in Databricks workspace",
                "Configure budget alerts",
                "Share reports with teams to drive awareness",
            ],
        })
        
        return sorted(recommendations, key=lambda x: x["severity"] != "high")
    
    def _estimate_savings(self, cost_analysis: Dict[str, Any]) -> float:
        """Estimate savings from identified issues."""
        monthly_cost = cost_analysis.get("estimated_monthly_cost", 0)
        # Assume 10-30% savings potential based on confidence factor
        return round(monthly_cost * (0.1 + 0.2 * self.confidence_factor), 2)
