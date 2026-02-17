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
    
    def _estimate_timeline(self, recommendations: List[Dict[str, Any]]) -> str:
        """Estimate implementation timeline based on recommendation severity."""
        if not recommendations:
            return "N/A (no optimization opportunities found)"
        high_count = len([r for r in recommendations if r.get('severity', '').upper() == 'HIGH'])
        if high_count >= 3:
            return "2-3 weeks"
        elif high_count >= 1:
            return "1-2 weeks"
        else:
            return "2-4 weeks"
    
    def _estimate_effort(self, severity: str) -> str:
        """Estimate implementation effort based on severity."""
        severity_upper = severity.upper() if severity else "MEDIUM"
        if severity_upper == "HIGH":
            return "Low (1-3 hours)"
        elif severity_upper == "MEDIUM":
            return "Medium (3-8 hours)"
        else:
            return "High (8+ hours)"
    
    def _estimate_business_impact(self, annual_savings: float) -> str:
        """Convert annual savings to business impact terms."""
        if annual_savings >= 100000:
            return f"{annual_savings/1000:.0f}K/year (1-2 headcount equivalents)"
        elif annual_savings >= 50000:
            return f"{annual_savings/1000:.0f}K/year (~1 mid-level engineer salary)"
        elif annual_savings >= 20000:
            return f"{annual_savings/1000:.0f}K/year (~3-4 months runway extension)"
        else:
            return f"{annual_savings:,.0f}/year (meaningful cost reduction)"
    
    def _benchmark_assessment(self, waste_percentage: float) -> str:
        """Assess how the company's waste compares to industry benchmark."""
        if waste_percentage >= 35:
            return "**HIGH** (above 35% waste) - Significant optimization opportunity"
        elif waste_percentage >= 20:
            return "**MEDIUM** (20-35% waste) - Solid optimization potential"
        elif waste_percentage >= 10:
            return "**LOW** (10-20% waste) - Well-optimized but room for improvement"
        else:
            return "**VERY LOW** (<10% waste) - Excellent cost management already in place"
    
    def generate(
        self,
        output_dir: Path,
        cost_analysis: Dict[str, Any],
        cluster_analysis: Dict[str, Any],
        job_analysis: Dict[str, Any],
        sql_analysis: Dict[str, Any],
        recommendations: List[Dict[str, Any]],
        warehouses_data: Dict[str, Any] = None,
        queries_data: Dict[str, Any] = None,
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
            warehouses_data: Data from warehouse collector
            queries_data: Data from query collector
        
        Returns:
            Path to generated report
        """
        logger.info("Generating Markdown report...")
        
        report_path = output_dir / "optimization_report.md"
        
        content = self._build_report(
            cost_analysis, cluster_analysis, job_analysis, sql_analysis, recommendations,
            warehouses_data or {}, queries_data or {}
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
        warehouses_data: Dict[str, Any] = None,
        queries_data: Dict[str, Any] = None,
    ) -> str:
        """Build the complete report content."""
        
        warehouses_data = warehouses_data or {}
        queries_data = queries_data or {}
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_cost = cost_analysis.get("total_cost", 0)
        monthly_cost = cost_analysis.get("estimated_monthly_cost", 0)
        total_dbus = cost_analysis.get("total_dbus", 0)
        period_days = cost_analysis.get("period_days", 30)
        total_savings = sum(r.get("estimated_savings", 0) for r in recommendations)
        annual_savings = total_savings * 12
        annual_spend = monthly_cost * 12
        savings_percentage = (total_savings / monthly_cost * 100) if monthly_cost > 0 else 0
        
        lines = [
            "# Databricks Cost & Performance Optimization Report",
            "",
            f"*Generated: {timestamp}*",
            "",
            "## Executive Summary",
            "",
            f"- **Analysis Period**: {period_days} days",
            f"- **Period Spend**: ${total_cost:,.2f}",
            f"- **Estimated Monthly Spend**: ${monthly_cost:,.2f}",
            f"- **Annual Run Rate**: ${annual_spend:,.2f}",
            f"- **Potential Monthly Savings**: ${total_savings:,.2f}",
            f"- **Potential Annual Savings**: ${annual_savings:,.2f}",
            f"- **Optimization Opportunities**: {len(recommendations)}",
            f"- **Savings Potential**: {savings_percentage:.1f}% of current spend",
            "",
            "## Business Impact",
            "",
            f"🎯 **Recommended Actions**: {len([r for r in recommendations if r.get('severity', '').upper() == 'HIGH'])} immediate, {len([r for r in recommendations if r.get('severity', '').upper() == 'MEDIUM'])} medium-term",
            "",
            f"💰 **Implementation Payoff**: If implemented, you'll reduce Databricks costs from ${monthly_cost:,.2f} to ${(monthly_cost - total_savings):,.2f}/month",
            "",
            f"⏱️ **Implementation Timeline**: " + self._estimate_timeline(recommendations) + "",
            "",
            f"📊 **Annual Impact**: ${annual_savings:,.2f}/year = {self._estimate_business_impact(annual_savings)}",
            "",
            "## Industry Benchmark Context",
            "",
            f"- **Your Savings Potential**: {savings_percentage:.1f}%",
            f"- **Industry Average Waste**: 20-40% of Databricks spend",
            f"- **Your Position**: " + self._benchmark_assessment(savings_percentage) + "",
            "",
            "## 12-Month Cost Projection",
            "",
            "| Month | Current Path | Optimized Path | Cumulative Savings |",
            "|-------|--------------|----------------|-------------------|",
        ]
        
        for month in range(1, 13):
            current = monthly_cost * month
            optimized = (monthly_cost - total_savings) * month
            savings = total_savings * month
            lines.append(f"| {month} | ${current:,.0f} | ${optimized:,.0f} | ${savings:,.0f} |")
        
        # Cost breakdown by product
        lines.extend([
            "",
            "## Cost Breakdown by Product",
            "",
            f"**Total DBUs**: {total_dbus:,.2f}",
            f"**Total Cost (Period)**: ${total_cost:,.2f}",
            "",
            "| Product | DBUs | Cost | % of Total |",
            "|---------|------|------|------------|",
        ])
        
        cost_by_product = cost_analysis.get("cost_by_product", {})
        for product, data in sorted(cost_by_product.items(), key=lambda x: x[1].get("cost", 0), reverse=True):
            dbus = data.get("dbus", 0)
            cost = data.get("cost", 0)
            pct = (cost / total_cost * 100) if total_cost > 0 else 0
            lines.append(f"| {product} | {dbus:,.2f} | ${cost:,.2f} | {pct:.1f}% |")
        
        # Serverless vs Classic breakdown
        serverless_cost = cost_analysis.get("serverless_cost", 0)
        classic_cost = cost_analysis.get("classic_cost", 0)
        serverless_dbus = cost_analysis.get("serverless_dbus", 0)
        classic_dbus = cost_analysis.get("classic_dbus", 0)
        serverless_pct = cost_analysis.get("serverless_percentage", 0)
        
        if serverless_cost > 0 or classic_cost > 0:
            lines.extend([
                "",
                "## Serverless vs Classic Compute",
                "",
                "| Compute Type | DBUs | Cost | % of Total |",
                "|--------------|------|------|------------|",
                f"| Serverless | {serverless_dbus:,.2f} | ${serverless_cost:,.2f} | {serverless_pct:.1f}% |",
                f"| Classic | {classic_dbus:,.2f} | ${classic_cost:,.2f} | {100 - serverless_pct:.1f}% |",
                "",
            ])
            
            if serverless_pct < 20 and total_cost > 100:
                lines.append("> 💡 **Tip**: Consider serverless compute for variable workloads to optimize costs.")
                lines.append("")
        
        # Top users by cost
        top_users = cost_analysis.get("top_users", [])
        attributed_cost = sum(u.get("cost", 0) for u in top_users)
        unattributed_cost = total_cost - attributed_cost
        
        lines.extend([
            "",
            "## Cost by User",
            "",
        ])
        
        if unattributed_cost > 0.01:
            lines.append(f"> **Note**: ${unattributed_cost:,.2f} ({unattributed_cost/total_cost*100:.1f}%) of costs are not attributed to users. This includes system costs (storage, networking, predictive optimization) and resources without identity tracking.")
            lines.append("")
        
        lines.extend([
            "| User | DBUs | Cost | % of Total |",
            "|------|------|------|------------|",
        ])
        
        if top_users:
            for user in top_users[:10]:
                user_id = user.get("id", "N/A")
                user_dbus = user.get("dbus", 0)
                user_cost = user.get("cost", 0)
                user_pct = (user_cost / total_cost * 100) if total_cost > 0 else 0
                lines.append(f"| {user_id[:40]} | {user_dbus:,.2f} | ${user_cost:,.2f} | {user_pct:.1f}% |")
        
        if unattributed_cost > 0.01:
            lines.append(f"| *(System/Unattributed)* | - | ${unattributed_cost:,.2f} | {unattributed_cost/total_cost*100:.1f}% |")
        
        lines.append("")
        
        # Top cost drivers - Clusters
        top_clusters = cost_analysis.get("top_clusters", [])
        if top_clusters:
            lines.extend([
                "",
                "## Top Cost Drivers - Clusters",
                "",
                "| Cluster | Name | DBUs | Cost |",
                "|---------|------|------|------|",
            ])
            for cluster in top_clusters[:5]:
                cluster_id = cluster.get('id', 'N/A')[:20]
                cluster_name = cluster.get('name') or 'N/A'
                lines.append(f"| {cluster_id} | {str(cluster_name)[:25]} | {cluster.get('dbus', 0):,.2f} | ${cluster.get('cost', 0):,.2f} |")
        
        # Top cost drivers - Jobs
        top_jobs = cost_analysis.get("top_jobs", [])
        if top_jobs:
            lines.extend([
                "",
                "## Top Cost Drivers - Jobs",
                "",
                "| Job ID | Name | DBUs | Cost |",
                "|--------|------|------|------|",
            ])
            for job in top_jobs[:5]:
                job_id = str(job.get('id', 'N/A'))[:15]
                job_name = job.get('name') or 'N/A'
                lines.append(f"| {job_id} | {str(job_name)[:30]} | {job.get('dbus', 0):,.2f} | ${job.get('cost', 0):,.2f} |")
        
        # Top cost drivers - SQL Warehouses
        top_warehouses = cost_analysis.get("top_warehouses", [])
        if top_warehouses:
            lines.extend([
                "",
                "## Top Cost Drivers - SQL Warehouses",
                "",
                "| Warehouse ID | Name | DBUs | Cost |",
                "|--------------|------|------|------|",
            ])
            for wh in top_warehouses[:5]:
                wh_id = wh.get('id', 'N/A')[:20]
                wh_name = wh.get('name') or 'N/A'
                lines.append(f"| {wh_id} | {str(wh_name)[:25]} | {wh.get('dbus', 0):,.2f} | ${wh.get('cost', 0):,.2f} |")
        
        # Detailed Warehouse Analysis
        warehouses = warehouses_data.get("warehouses", [])
        warehouse_issues = warehouses_data.get("issues", [])
        
        if warehouses:
            lines.extend([
                "",
                "## SQL Warehouse Configuration Analysis",
                "",
                f"**Total Warehouses**: {len(warehouses)}",
                f"**Configuration Issues Found**: {len(warehouse_issues)}",
                "",
                "### Warehouse Details",
                "",
                "| Name | Size | Type | Auto-Stop | State | Period Cost |",
                "|------|------|------|-----------|-------|-------------|",
            ])
            for wh in warehouses:
                name = wh.get("warehouse_name", "N/A")[:25]
                size = wh.get("warehouse_size", "N/A")
                wh_type_raw = str(wh.get("warehouse_type", "")).upper()
                wh_type = "Serverless" if "SERVERLESS" in wh_type_raw else "Classic"
                auto_stop = wh.get("auto_stop_minutes")
                auto_stop_str = f"{auto_stop} min" if auto_stop and auto_stop > 0 else "❌ Disabled"
                state = wh.get("state", "N/A")
                cost = wh.get("total_cost", 0)
                lines.append(f"| {name} | {size} | {wh_type} | {auto_stop_str} | {state} | ${cost:,.2f} |")
            
            # Warehouse issues
            if warehouse_issues:
                lines.extend([
                    "",
                    "### Warehouse Configuration Issues",
                    "",
                ])
                for issue in warehouse_issues:
                    severity = issue.get("severity", "medium").upper()
                    wh_name = issue.get("warehouse_name", "Unknown")
                    desc = issue.get("description", "")
                    savings = issue.get("estimated_savings", 0)
                    lines.append(f"- **[{severity}]** {wh_name}: {desc} (Est. savings: ${savings:,.2f}/month)")
                lines.append("")
        
        lines.extend([
            "",
            "## Implementation Roadmap",
            "",
            "### Phase 1: Quick Wins (Week 1-2)",
            "",
            "High-impact changes that require minimal effort:",
            "",
        ])
        
        quick_wins = [r for r in recommendations if r.get('severity', '').upper() == 'HIGH']
        if quick_wins:
            for rec in quick_wins:
                lines.append(f"- **{rec.get('title', 'N/A')}**: ${rec.get('estimated_savings', 0):,.2f}/month savings")
        else:
            lines.append("- No high-priority issues identified")
        
        lines.extend([
            "",
            "### Phase 2: Strategic Changes (Week 3-8)",
            "",
            "Medium-impact improvements with moderate implementation effort:",
            "",
        ])
        
        medium_recs = [r for r in recommendations if r.get('severity', '').upper() == 'MEDIUM']
        if medium_recs:
            for rec in medium_recs:
                lines.append(f"- **{rec.get('title', 'N/A')}**: ${rec.get('estimated_savings', 0):,.2f}/month savings")
        else:
            lines.append("- No medium-priority issues identified")
        
        lines.extend([
            "",
            "### Phase 3: Long-term Optimization (Ongoing)",
            "",
            "- Establish cost monitoring dashboard",
            "- Quarterly optimization reviews",
            "- Team training on cost-conscious development",
            "",
            "## Cluster Analysis",
            "",
            f"- **Total Clusters**: {cluster_analysis.get('cluster_count', 0)}",
            f"- **Issues Found**: {cluster_analysis.get('issue_count', 0)}",
            "",
        ])
        
        cluster_costs = cluster_analysis.get("cluster_costs", [])
        if cluster_costs:
            lines.extend([
                "### Cluster Cost Attribution",
                "",
                "| Cluster | Owner | DBUs | Cost |",
                "|---------|-------|------|------|",
            ])
            for cc in cluster_costs[:10]:
                name = cc.get("cluster_name") or cc.get("cluster_id", "N/A")
                owner = cc.get("owner", "N/A") or "N/A"
                lines.append(f"| {str(name)[:25]} | {str(owner)[:20]} | {cc.get('total_dbus', 0):,.2f} | ${cc.get('total_cost', 0):,.2f} |")
        
        lines.extend([
            "",
            "### Identified Issues",
            "",
        ])
        
        if cluster_analysis.get("issues"):
            for issue in cluster_analysis.get("issues", [])[:10]:
                lines.append(f"- [{issue['severity'].upper()}] {issue['cluster_name']}: {issue['description']}")
        else:
            lines.append("- No critical cluster configuration issues found")
        
        lines.extend([
            "",
            "## Job Analysis",
            "",
            f"- **Jobs with Usage**: {job_analysis.get('job_count', 0)}",
            "",
        ])
        
        jobs = job_analysis.get("jobs", [])
        if jobs:
            lines.extend([
                "### Top Jobs by Cost",
                "",
                "| Job Name | Runs | DBUs | Cost |",
                "|----------|------|------|------|",
            ])
            for job in jobs[:10]:
                name = job.get("job_name") or job.get("job_id", "N/A")
                lines.append(f"| {str(name)[:30]} | {job.get('run_count', 0)} | {job.get('total_dbus', 0):,.2f} | ${job.get('total_cost', 0):,.2f} |")
        
        lines.extend([
            "",
            "## SQL Query Analysis",
            "",
            f"- **Queries Analyzed**: {sql_analysis.get('query_count', 0)}",
            f"- **Inefficient Patterns Found**: {sql_analysis.get('pattern_count', 0)}",
            "",
        ])
        
        # Expensive queries from queries_data (longest running queries)
        expensive_queries = queries_data.get("expensive_queries", [])
        if expensive_queries:
            lines.extend([
                "### Top Longest Running Queries",
                "",
                "> Use the Query ID to find details in Databricks SQL > Query History",
                "",
                "| Query ID | Duration | Type | Rows Read | User |",
                "|----------|----------|------|-----------|------|",
            ])
            for q in expensive_queries[:5]:
                query_id = q.get("statement_id", "N/A")
                # Show first 20 chars of query ID (they're long UUIDs)
                query_id_short = query_id[:20] + "..." if len(query_id) > 20 else query_id
                
                duration = q.get("duration_seconds", 0)
                if duration >= 60:
                    dur_str = f"{duration/60:.1f} min"
                else:
                    dur_str = f"{duration:.1f}s"
                
                stmt_type = q.get("statement_type", "N/A")
                
                rows_read = q.get("read_rows") or 0
                if rows_read >= 1e9:
                    rows_str = f"{rows_read/1e9:.1f}B"
                elif rows_read >= 1e6:
                    rows_str = f"{rows_read/1e6:.1f}M"
                elif rows_read >= 1e3:
                    rows_str = f"{rows_read/1e3:.1f}K"
                else:
                    rows_str = f"{int(rows_read)}"
                
                user = (q.get("user") or "N/A").split("@")[0][:15]  # Just username part
                lines.append(f"| `{query_id_short}` | {dur_str} | {stmt_type} | {rows_str} | {user} |")
            lines.append("")
        
        # User query stats
        user_stats = queries_data.get("user_stats", [])
        if user_stats:
            lines.extend([
                "### Query Stats by User",
                "",
                "| User | Query Count | Avg Duration | Total Duration |",
                "|------|-------------|--------------|----------------|",
            ])
            for u in user_stats[:10]:
                user = u.get("user", "N/A")[:25]
                count = u.get("query_count", 0)
                avg_dur = u.get("avg_duration_seconds", 0)
                total_dur = u.get("total_duration_seconds", 0)
                
                # Format total duration nicely
                if total_dur >= 3600:
                    total_str = f"{total_dur/3600:.1f} hours"
                elif total_dur >= 60:
                    total_str = f"{total_dur/60:.1f} min"
                else:
                    total_str = f"{total_dur:.0f}s"
                
                lines.append(f"| {user} | {count} | {avg_dur:.1f}s | {total_str} |")
            lines.append("")
        
        lines.append("### Common Issues")
        lines.append("")
        
        # Get pattern info from inefficient_patterns
        inefficient_patterns = sql_analysis.get("inefficient_patterns", [])
        pattern_summary = sql_analysis.get("pattern_summary", {})
        
        if inefficient_patterns:
            for pattern in inefficient_patterns:
                ptype = pattern.get("type", "unknown")
                count = pattern.get("count", 0)
                description = pattern.get("description", "")
                severity = pattern.get("severity", "medium").upper()
                lines.append(f"- **[{severity}] {ptype}**: {count} instances - {description}")
        elif pattern_summary:
            for ptype, count in pattern_summary.items():
                lines.append(f"- **{ptype}**: {count} queries found")
        else:
            lines.append("- No major inefficient query patterns detected")
        
        lines.extend([
            "",
            "## Detailed Optimization Recommendations",
            "",
        ])
        
        if not recommendations:
            lines.append("No specific recommendations at this time. Your Databricks usage appears optimized.")
            lines.append("")
        
        for i, rec in enumerate(recommendations, 1):
            rec_savings = rec.get('estimated_savings', 0)
            rec_savings_annual = rec_savings * 12
            lines.extend([
                f"### {i}. {rec['title']}",
                "",
                f"**Severity**: {rec['severity'].upper()} | **Category**: {rec.get('category', 'general').upper()} | **Est. Savings**: ${rec_savings:,.2f}/month",
                "",
                f"{rec['description']}",
                "",
            ])
            
            # Add insight if present (data-driven context)
            insight = rec.get('insight')
            if insight:
                lines.extend([
                    f"> 💡 **Insight**: {insight}",
                    "",
                ])
            
            lines.extend([
                "**Action Steps**:",
                "",
            ])
            for j, step in enumerate(rec.get('steps', []), 1):
                lines.append(f"{j}. {step}")
            lines.extend(["", "---", ""])
        
        lines.extend([
            "## Security & Compliance",
            "",
            "- ✅ This analysis uses **read-only** Databricks permissions only",
            "- ✅ No data is modified, exported, or stored",
            "- ✅ All analysis runs locally within your Docker container",
            "- ✅ Compliant with enterprise security policies",
            "",
            "## Implementation Support",
            "",
            "**Next Steps:**",
            "",
            "1. **Week 1-2**: Implement Phase 1 quick wins with your data engineering team",
            "2. **Week 2**: Monitor cost baseline to confirm savings materialization",
            "3. **Week 3-8**: Execute Phase 2 strategic changes",
            "4. **Ongoing**: Set up monthly cost reviews and re-run analysis quarterly",
            "",
            "---",
            "",
            "*This report was generated by Databricks Cost Optimizer.*",
            "",
        ])
        
        return "\n".join(lines)
