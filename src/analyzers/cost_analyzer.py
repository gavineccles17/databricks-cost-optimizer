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
        self.dbu_pricing = self._load_pricing()
    
    def _load_pricing(self) -> Dict[str, float]:
        """Load DBU pricing from configuration."""
        # Default pricing - would load from config/pricing.yaml in production
        return {
            "compute": 0.40,
            "sql": 0.40,
            "jobs": 0.40,
        }
    
    def analyze(
        self,
        usage_data: Dict[str, Any],
        clusters_data: Dict[str, Any],
        jobs_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform cost analysis on usage data.
        
        Args:
            usage_data: Data from usage collector
            clusters_data: Data from cluster collector
            jobs_data: Data from job collector
        
        Returns:
            Cost analysis results
        """
        logger.info("Analyzing costs...")
        
        total_dbus = usage_data.get("total_dbus", 0)
        dbu_by_type = usage_data.get("by_type", {})
        days = usage_data.get("period", {}).get("days", 30)
        
        # Calculate monthly cost (assuming 30-day period)
        estimated_monthly_dbus = (total_dbus / max(days, 1)) * 30
        compute_dbu_price = self.dbu_pricing.get("compute", 0.40)
        estimated_monthly_cost = estimated_monthly_dbus * compute_dbu_price
        
        # Cost breakdown by type
        cost_by_type = {}
        for dbu_type, quantity in dbu_by_type.items():
            price = self.dbu_pricing.get(dbu_type.lower(), 0.40)
            monthly_quantity = (quantity / max(days, 1)) * 30
            cost_by_type[dbu_type] = {
                "dbus": monthly_quantity,
                "cost": monthly_quantity * price,
            }
        
        return {
            "period_dbus": total_dbus,
            "period_days": days,
            "estimated_monthly_dbus": round(estimated_monthly_dbus, 2),
            "estimated_monthly_cost": round(estimated_monthly_cost, 2),
            "cost_by_type": cost_by_type,
            "dbu_price": compute_dbu_price,
        }
