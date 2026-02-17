"""Analyzes Databricks cost structure and trends."""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class CostAnalyzer:
    """Analyzes usage data to calculate costs and identify optimization opportunities."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize cost analyzer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
    
    def analyze(
        self,
        usage_data: Dict[str, Any],
        clusters_data: Dict[str, Any],
        jobs_data: Dict[str, Any],
        warehouses_data: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Perform cost analysis on usage data.
        
        Args:
            usage_data: Data from usage collector (includes actual dollar costs)
            clusters_data: Data from cluster collector
            jobs_data: Data from job collector
            warehouses_data: Data from warehouse collector
        
        Returns:
            Cost analysis results
        """
        logger.info("Analyzing costs...")
        
        warehouses_data = warehouses_data or {}
        
        total_dbus = usage_data.get("total_dbus", 0)
        total_cost = usage_data.get("total_cost", 0)
        cost_by_product = usage_data.get("cost_by_product", {})
        cost_by_sku = usage_data.get("cost_by_sku", {})
        cost_by_cluster = usage_data.get("cost_by_cluster", {})
        cost_by_job = usage_data.get("cost_by_job", {})
        cost_by_warehouse = usage_data.get("cost_by_warehouse", {})
        cost_by_user = usage_data.get("cost_by_user", {})
        days = usage_data.get("period", {}).get("days", 30)
        
        # Serverless vs Classic breakdown
        serverless_cost = usage_data.get("serverless_cost", 0)
        classic_cost = usage_data.get("classic_cost", 0)
        serverless_dbus = usage_data.get("serverless_dbus", 0)
        classic_dbus = usage_data.get("classic_dbus", 0)
        
        # Calculate monthly projections
        days_factor = 30 / max(days, 1)
        estimated_monthly_dbus = total_dbus * days_factor
        estimated_monthly_cost = total_cost * days_factor
        
        # Build name lookups from collector data
        job_names = {}
        for job in jobs_data.get("jobs", []):
            job_id = str(job.get("job_id", ""))
            job_name = job.get("job_name")
            if job_id and job_name:
                job_names[job_id] = job_name
        
        cluster_names = {}
        for cc in clusters_data.get("cluster_costs", []):
            cluster_id = cc.get("cluster_id")
            cluster_name = cc.get("cluster_name")
            if cluster_id and cluster_name:
                cluster_names[cluster_id] = cluster_name
        for cl in clusters_data.get("clusters", []):
            cluster_id = cl.get("cluster_id")
            cluster_name = cl.get("cluster_name")
            if cluster_id and cluster_name:
                cluster_names[cluster_id] = cluster_name
        
        warehouse_names = {}
        for wh in warehouses_data.get("warehouses", []):
            wh_id = wh.get("warehouse_id")
            wh_name = wh.get("warehouse_name")
            if wh_id and wh_name:
                warehouse_names[wh_id] = wh_name
        
        # Find top cost drivers with names
        top_clusters = self._get_top_items_with_names(cost_by_cluster, cluster_names, limit=10)
        top_jobs = self._get_top_items_with_names(cost_by_job, job_names, limit=10)
        top_warehouses = self._get_top_items_with_names(cost_by_warehouse, warehouse_names, limit=10)
        top_products = self._get_top_items(cost_by_product, limit=10)
        top_skus = self._get_top_items(cost_by_sku, limit=10)
        top_users = self._get_top_items(cost_by_user, limit=10)
        
        # Warehouse issue costs from warehouse collector
        warehouse_issues = warehouses_data.get("issues", [])
        potential_savings = sum(
            issue.get("estimated_savings", 0)
            for issue in warehouse_issues
        )
        
        logger.info(f"Period cost: ${total_cost:.2f}, Monthly projection: ${estimated_monthly_cost:.2f}")
        logger.info(f"Serverless: ${serverless_cost:.2f}, Classic: ${classic_cost:.2f}")
        
        return {
            "total_dbus": total_dbus,
            "total_cost": total_cost,
            "period_days": days,
            "estimated_monthly_dbus": round(estimated_monthly_dbus, 2),
            "estimated_monthly_cost": round(estimated_monthly_cost, 2),
            # Serverless breakdown
            "serverless_cost": serverless_cost,
            "classic_cost": classic_cost,
            "serverless_dbus": serverless_dbus,
            "classic_dbus": classic_dbus,
            "serverless_percentage": round(serverless_cost / total_cost * 100, 1) if total_cost > 0 else 0,
            # Cost breakdowns
            "cost_by_product": cost_by_product,
            "cost_by_sku": cost_by_sku,
            "cost_by_cluster": cost_by_cluster,
            "cost_by_job": cost_by_job,
            "cost_by_warehouse": cost_by_warehouse,
            "cost_by_user": cost_by_user,
            # Top items
            "top_clusters": top_clusters,
            "top_jobs": top_jobs,
            "top_warehouses": top_warehouses,
            "top_products": top_products,
            "top_skus": top_skus,
            "top_users": top_users,
            # Potential savings
            "potential_savings": potential_savings,
            "warehouse_issues": warehouse_issues,
        }
    
    def _get_top_items(self, cost_dict: Dict[str, Dict], limit: int = 10) -> list:
        """Sort items by cost and return top N."""
        items = [
            {"id": k, "dbus": v.get("dbus", 0), "cost": v.get("cost", 0), "name": v.get("name")}
            for k, v in cost_dict.items()
        ]
        items.sort(key=lambda x: x["cost"], reverse=True)
        return items[:limit]
    
    def _get_top_items_with_names(self, cost_dict: Dict[str, Dict], name_lookup: Dict[str, str], limit: int = 10) -> list:
        """Sort items by cost and return top N, enriching with names from lookup."""
        items = []
        for k, v in cost_dict.items():
            name = name_lookup.get(str(k)) or v.get("name")
            items.append({
                "id": k,
                "name": name,
                "dbus": v.get("dbus", 0),
                "cost": v.get("cost", 0),
            })
        items.sort(key=lambda x: x["cost"], reverse=True)
        return items[:limit]
