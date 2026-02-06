"""Generates client-ready Markdown reports."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class MarkdownReport:
    """Generates professional Markdown reports."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize markdown report generator."""
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
        Generate a Markdown report.
        
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
        logger.info("Generating Markdown report...")
        
        report_path = output_dir / "optimization_report.md"
        
        content = self._build_report(
            cost_analysis, cluster_analysis, job_analysis, sql_analysis, recommendations
        )
        
        report_path.write_text(content)
        logger.info(f"Markdown report written to {report_path}")
        
        return report_path
    
    def _build_report(
        self,
        cost_analysis: Dict[str, Any],
        cluster_analysis: Dict[str, Any],
        job_analysis: Dict[str, Any],
        sql_analysis: Dict[str, Any],
        recommendations: List[Dict[str, Any]],
    ) -> str:
        """Build the complete report content."""
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        monthly_cost = cost_analysis.get("estimated_monthly_cost", 0)
        total_savings = sum(r.get("estimated_savings", 0) for r in recommendations)
        
        lines = [
            "# Databricks Cost & Performance Optimization Report",
            "",
            f"*Generated: {timestamp}*",
            "",
            "## Executive Summary",
            "",
            f"- **Estimated Monthly Spend**: ${monthly_cost:,.2f}",
            f"- **Potential Monthly Savings**: ${total_savings:,.2f}",
            f"- **Optimization Opportunities**: {len(recommendations)}",
            "",
            "## Cost Breakdown",
            "",
            f"### Total DBUs Used (Period): {cost_analysis.get('period_dbus', 0):,.0f}",
            f"### Estimated Monthly DBUs: {cost_analysis.get('estimated_monthly_dbus', 0):,.0f}",
            f"### DBU Price: ${cost_analysis.get('dbu_price', 0)}/DBU",
            "",
            "### Cost by Type",
            "",
        ]
        
        for dbu_type, costs in cost_analysis.get("cost_by_type", {}).items():
            dbus = costs.get("dbus", 0)
            cost = costs.get("cost", 0)
            lines.append(f"- **{dbu_type}**: {dbus:,.0f} DBUs → ${cost:,.2f}/month")
        
        lines.extend([
            "",
            "## Cluster Analysis",
            "",
            f"- **Total Clusters**: {cluster_analysis.get('cluster_count', 0)}",
            f"- **Issues Found**: {cluster_analysis.get('issue_count', 0)}",
            "",
            "### Identified Issues",
            "",
        ])
        
        for issue in cluster_analysis.get("issues", [])[:5]:
            lines.append(f"- [{issue['severity'].upper()}] {issue['cluster_name']}: {issue['description']}")
        
        lines.extend([
            "",
            "## SQL Query Analysis",
            "",
            f"- **Queries Analyzed**: {sql_analysis.get('query_count', 0)}",
            f"- **Inefficient Patterns**: {sql_analysis.get('pattern_count', 0)}",
            "",
            "### Common Issues",
            "",
        ])
        
        pattern_types = {}
        for pattern in sql_analysis.get("inefficient_patterns", []):
            ptype = pattern.get("type", "unknown")
            pattern_types[ptype] = pattern_types.get(ptype, 0) + 1
        
        for ptype, count in pattern_types.items():
            lines.append(f"- {ptype}: {count} queries")
        
        lines.extend([
            "",
            "## Optimization Recommendations",
            "",
        ])
        
        for i, rec in enumerate(recommendations, 1):
            lines.extend([
                f"### {i}. {rec['title']}",
                "",
                f"**Severity**: {rec['severity'].upper()}",
                f"**Estimated Monthly Savings**: ${rec.get('estimated_savings', 0):,.2f}",
                "",
                rec['description'],
                "",
                "**Action Steps**:",
                "",
            ])
            for step in rec.get('steps', []):
                lines.append(f"1. {step}")
            lines.append("")
        
        lines.extend([
            "## Security & Compliance",
            "",
            "- This analysis uses **read-only** Databricks permissions only",
            "- No data is modified or exported",
            "- All analysis runs locally within your Docker container",
            "",
            "## Next Steps",
            "",
            "1. Review recommendations with your data engineering team",
            "2. Prioritize by severity and savings impact",
            "3. Implement changes in a development environment first",
            "4. Monitor performance and cost reduction",
            "5. Re-run analysis monthly to track progress",
            "",
        ])
        
        return "\n".join(lines)
