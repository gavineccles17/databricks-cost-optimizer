"""Generates machine-readable JSON reports."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class JsonReport:
    """Generates machine-readable JSON reports."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize JSON report generator."""
        self.config = config
    
    def generate(
        self,
        output_dir: Path,
        cost_analysis: Dict[str, Any],
        cluster_analysis: Dict[str, Any],
        job_analysis: Dict[str, Any],
        sql_analysis: Dict[str, Any],
        recommendations: List[Dict[str, Any]],
    ) -> Path:
        """
        Generate a JSON report.
        
        Args:
            output_dir: Directory to write report
            cost_analysis: Cost analysis results
            cluster_analysis: Cluster analysis results
            job_analysis: Job analysis results
            sql_analysis: SQL analysis results
            recommendations: List of recommendations
        
        Returns:
            Path to generated report
        """
        logger.info("Generating JSON report...")
        
        report_path = output_dir / "optimization_report.json"
        
        report_data = {
            "generated": datetime.now().isoformat(),
            "version": "1.0",
            "cost_analysis": cost_analysis,
            "cluster_analysis": cluster_analysis,
            "job_analysis": job_analysis,
            "sql_analysis": sql_analysis,
            "recommendations": recommendations,
            "summary": {
                "total_recommendations": len(recommendations),
                "estimated_total_savings": sum(r.get("estimated_savings", 0) for r in recommendations),
                "estimated_monthly_cost": cost_analysis.get("estimated_monthly_cost", 0),
            },
        }
        
        with open(report_path, "w") as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"JSON report written to {report_path}")
        return report_path
