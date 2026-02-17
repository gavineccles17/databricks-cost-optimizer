"""Collects SQL Warehouse configuration and usage data."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class WarehouseCollector:
    """Collects warehouse metadata, configuration, and cost attribution."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize warehouse collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect warehouse data with cost attribution for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing warehouse data with costs
        """
        logger.info("Collecting warehouse data")
        
        # Get warehouse configurations from system.compute.warehouses
        warehouses = self._collect_warehouse_configs()
        
        # Get warehouse cost attribution from billing
        warehouse_costs = self._collect_warehouse_costs(start_date, end_date)
        
        # Get warehouse events for utilization analysis
        warehouse_events = self._collect_warehouse_events(start_date, end_date)
        
        # Merge costs with warehouse configs
        warehouses_with_costs = self._merge_warehouse_data(warehouses, warehouse_costs)
        
        # Analyze warehouse efficiency
        issues = self._analyze_warehouse_issues(warehouses_with_costs, warehouse_events)
        
        return {
            "warehouses": warehouses_with_costs,
            "warehouse_events": warehouse_events,
            "warehouse_count": len(warehouses),
            "issues": issues,
            "issue_count": len(issues),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
    
    def _collect_warehouse_configs(self) -> List[Dict]:
        """Get warehouse configurations."""
        try:
            query = """
            SELECT
                warehouse_id,
                warehouse_name,
                warehouse_type,
                warehouse_size,
                min_clusters,
                max_clusters,
                auto_stop_minutes,
                created_by,
                tags
            FROM system.compute.warehouses
            """
            warehouses = self.client.execute_query(query)
            logger.info(f"Warehouse config query returned {len(warehouses)} warehouses")
            return warehouses
        except Exception as e:
            logger.warning(f"Could not fetch warehouse configs: {str(e)}")
            return []
    
    def _collect_warehouse_costs(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get warehouse cost attribution from billing."""
        # Try account_prices first, fallback to list_prices
        costs = self._query_warehouse_costs_account_prices(start_date, end_date)
        if not costs:
            costs = self._query_warehouse_costs_list_prices(start_date, end_date)
        return costs
    
    def _query_warehouse_costs_account_prices(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Query warehouse costs using account_prices."""
        try:
            query = f"""
            SELECT
                usage.usage_metadata.warehouse_id as warehouse_id,
                usage.product_features.is_serverless as is_serverless,
                usage.sku_name,
                SUM(usage.usage_quantity) as total_dbus,
                SUM(usage.usage_quantity * prices.pricing.default) as total_cost
            FROM system.billing.usage usage
            JOIN system.billing.account_prices prices ON prices.sku_name = usage.sku_name
            WHERE usage.usage_metadata.warehouse_id IS NOT NULL
                AND usage.usage_end_time >= prices.price_start_time
                AND (prices.price_end_time IS NULL OR usage.usage_end_time < prices.price_end_time)
                AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY 1, 2, 3
            ORDER BY total_cost DESC
            """
            return self.client.execute_query(query)
        except Exception as e:
            logger.debug(f"account_prices warehouse query failed: {str(e)}")
            return []
    
    def _query_warehouse_costs_list_prices(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Query warehouse costs using list_prices."""
        try:
            query = f"""
            SELECT
                usage.usage_metadata.warehouse_id as warehouse_id,
                usage.product_features.is_serverless as is_serverless,
                usage.sku_name,
                SUM(usage.usage_quantity) as total_dbus,
                SUM(usage.usage_quantity * lp.pricing.effective_list.default) as total_cost
            FROM system.billing.usage usage
            JOIN system.billing.list_prices lp ON lp.sku_name = usage.sku_name
            WHERE usage.usage_metadata.warehouse_id IS NOT NULL
                AND usage.usage_end_time >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR usage.usage_end_time < lp.price_end_time)
                AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY 1, 2, 3
            ORDER BY total_cost DESC
            """
            costs = self.client.execute_query(query)
            logger.info(f"Warehouse costs query returned {len(costs)} records")
            return costs
        except Exception as e:
            logger.warning(f"Could not fetch warehouse costs: {str(e)}")
            return []
    
    def _collect_warehouse_events(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get warehouse events for utilization analysis."""
        try:
            query = f"""
            SELECT
                warehouse_id,
                event_type,
                cluster_count,
                event_time
            FROM system.compute.warehouse_events
            WHERE event_time BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
            ORDER BY event_time DESC
            LIMIT 10000
            """
            events = self.client.execute_query(query)
            logger.info(f"Warehouse events query returned {len(events)} events")
            return events
        except Exception as e:
            logger.warning(f"Could not fetch warehouse events: {str(e)}")
            return []
    
    def _merge_warehouse_data(self, warehouses: List[Dict], costs: List[Dict]) -> List[Dict]:
        """Merge warehouse configs with cost data."""
        # Build cost lookup by warehouse_id
        cost_by_warehouse = {}
        for cost in costs:
            wh_id = cost.get("warehouse_id")
            if wh_id not in cost_by_warehouse:
                cost_by_warehouse[wh_id] = {
                    "total_dbus": 0,
                    "total_cost": 0,
                    "serverless_cost": 0,
                    "classic_cost": 0,
                }
            cost_by_warehouse[wh_id]["total_dbus"] += float(cost.get("total_dbus") or 0)
            cost_by_warehouse[wh_id]["total_cost"] += float(cost.get("total_cost") or 0)
            if cost.get("is_serverless"):
                cost_by_warehouse[wh_id]["serverless_cost"] += float(cost.get("total_cost") or 0)
            else:
                cost_by_warehouse[wh_id]["classic_cost"] += float(cost.get("total_cost") or 0)
        
        # Merge with warehouse configs
        merged = []
        for wh in warehouses:
            wh_id = wh.get("warehouse_id")
            cost_data = cost_by_warehouse.get(wh_id, {})
            merged.append({
                **wh,
                "total_dbus": cost_data.get("total_dbus", 0),
                "total_cost": cost_data.get("total_cost", 0),
                "serverless_cost": cost_data.get("serverless_cost", 0),
                "classic_cost": cost_data.get("classic_cost", 0),
            })
        
        # Add warehouses that have costs but no config (might be deleted)
        known_ids = {wh.get("warehouse_id") for wh in warehouses}
        for wh_id, cost_data in cost_by_warehouse.items():
            if wh_id not in known_ids:
                merged.append({
                    "warehouse_id": wh_id,
                    "warehouse_name": f"Unknown ({wh_id})",
                    "total_dbus": cost_data.get("total_dbus", 0),
                    "total_cost": cost_data.get("total_cost", 0),
                    "serverless_cost": cost_data.get("serverless_cost", 0),
                    "classic_cost": cost_data.get("classic_cost", 0),
                })
        
        # Sort by cost descending
        merged.sort(key=lambda x: x.get("total_cost", 0), reverse=True)
        return merged
    
    def _analyze_warehouse_issues(self, warehouses: List[Dict], events: List[Dict]) -> List[Dict]:
        """Identify warehouse configuration issues."""
        issues = []
        
        for wh in warehouses:
            wh_id = wh.get("warehouse_id")
            wh_name = wh.get("warehouse_name") or wh_id
            auto_stop = wh.get("auto_stop_minutes")
            wh_type = str(wh.get("warehouse_type", "")).upper()
            wh_size = str(wh.get("warehouse_size", "")).upper()
            total_cost = wh.get("total_cost", 0)
            
            # Estimate monthly cost based on period cost
            monthly_multiplier = 1.0  # Default assumption
            
            # Check for no auto-stop or very long auto-stop
            if auto_stop is not None:
                if auto_stop == 0:
                    # Estimate savings: 20% of warehouse cost from idle time elimination
                    estimated_savings = total_cost * 0.2 * monthly_multiplier
                    issues.append({
                        "type": "no_auto_stop",
                        "warehouse_id": wh_id,
                        "warehouse_name": wh_name,
                        "severity": "high",
                        "description": "Warehouse has auto-stop disabled - will run indefinitely when started",
                        "cost": total_cost,
                        "estimated_savings": estimated_savings,
                        "recommendation": "Enable auto-stop (recommend 10-15 minutes)",
                    })
                elif auto_stop > 30:
                    # Estimate savings from reducing auto-stop time
                    estimated_savings = total_cost * 0.1 * monthly_multiplier
                    issues.append({
                        "type": "long_auto_stop",
                        "warehouse_id": wh_id,
                        "warehouse_name": wh_name,
                        "severity": "medium",
                        "auto_stop_delay": auto_stop * 60,  # Convert to seconds for compatibility
                        "description": f"Warehouse auto-stop set to {auto_stop} minutes (>30 min)",
                        "cost": total_cost,
                        "estimated_savings": estimated_savings,
                        "recommendation": "Consider reducing to 10-15 minutes",
                    })
            
            # Check for oversized warehouses with low usage
            if total_cost > 0 and wh_size in ["2X-LARGE", "3X-LARGE", "4X-LARGE"]:
                estimated_savings = total_cost * 0.25 * monthly_multiplier
                issues.append({
                    "type": "oversized",
                    "warehouse_id": wh_id,
                    "warehouse_name": wh_name,
                    "size": wh_size,
                    "severity": "medium",
                    "description": f"{wh_size} warehouse - verify if size is necessary",
                    "cost": total_cost,
                    "estimated_savings": estimated_savings,
                    "recommendation": "Review query complexity; downsize if queries don't require this capacity",
                })
            
            # Check for classic warehouses that could be serverless - lowered threshold
            if "CLASSIC" in wh_type and "SERVERLESS" not in wh_type and total_cost > 5:
                estimated_savings = total_cost * 0.15 * monthly_multiplier
                issues.append({
                    "type": "serverless_candidate",
                    "warehouse_id": wh_id,
                    "warehouse_name": wh_name,
                    "severity": "low",
                    "description": f"Classic warehouse with ${total_cost:.2f} spend",
                    "cost": total_cost,
                    "estimated_savings": estimated_savings,
                    "recommendation": "Consider serverless for variable workloads (instant start, per-second billing)",
                })
        
        return issues
