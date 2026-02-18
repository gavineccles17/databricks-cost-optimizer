"""Collects Databricks usage and billing data from system tables."""

import logging
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class UsageCollector:
    """Collects usage data from system.billing.usage joined with pricing tables."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """
        Initialize usage collector.
        
        Args:
            client: Databricks client instance
            config: Configuration dictionary
        """
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect usage data with dollar costs for the specified period.
        Uses account_prices (contracted rates) with fallback to list_prices.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing usage data with costs
        """
        logger.info(f"Collecting usage data from {start_date.date()} to {end_date.date()}")
        
        # Check if account_prices table exists before querying
        has_account_prices = self.client.table_exists("system.billing.account_prices")
        
        if has_account_prices:
            logger.info("Using account_prices for cost calculation (contracted rates)")
            results = self._query_with_account_prices(start_date, end_date)
        else:
            logger.info("account_prices table not available, using list_prices (standard rates)")
            results = []
        
        # Fallback to list_prices if account_prices query failed or not available
        if not results:
            if has_account_prices:
                logger.warning("account_prices query returned no results, falling back to list_prices")
            results = self._query_with_list_prices(start_date, end_date)
        
        logger.info(f"Usage query returned {len(results)} rows")
        
        if results and len(results) > 0:
            logger.info(f"Sample usage record: {results[0]}")
        
        return self._aggregate_results(results, start_date, end_date)
    
    def _query_with_account_prices(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Query using account_prices table (contracted rates)."""
        try:
            query = f"""
            SELECT
                usage.usage_date,
                usage.sku_name,
                usage.billing_origin_product,
                usage.workspace_id,
                usage.usage_quantity,
                usage.usage_unit,
                usage.usage_metadata.cluster_id as cluster_id,
                usage.usage_metadata.job_id as job_id,
                usage.usage_metadata.warehouse_id as warehouse_id,
                usage.usage_metadata.dlt_pipeline_id as pipeline_id,
                usage.usage_metadata.endpoint_name as endpoint_name,
                usage.identity_metadata.run_as as run_as_user,
                usage.product_features.is_serverless as is_serverless,
                usage.product_features.is_photon as is_photon,
                (usage.usage_quantity * prices.pricing.default) as dollar_cost
            FROM system.billing.usage usage
            JOIN system.billing.account_prices prices 
                ON prices.sku_name = usage.sku_name
            WHERE usage.usage_end_time >= prices.price_start_time
                AND (prices.price_end_time IS NULL OR usage.usage_end_time < prices.price_end_time)
                AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            ORDER BY usage.usage_date DESC, dollar_cost DESC
            """
            return self.client.execute_query(query)
        except Exception as e:
            logger.warning(f"account_prices query failed: {str(e)}")
            return []
    
    def _query_with_list_prices(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Query using list_prices table (standard published rates)."""
        try:
            query = f"""
            SELECT
                usage.usage_date,
                usage.sku_name,
                usage.billing_origin_product,
                usage.workspace_id,
                usage.usage_quantity,
                usage.usage_unit,
                usage.usage_metadata.cluster_id as cluster_id,
                usage.usage_metadata.job_id as job_id,
                usage.usage_metadata.warehouse_id as warehouse_id,
                usage.usage_metadata.dlt_pipeline_id as pipeline_id,
                usage.usage_metadata.endpoint_name as endpoint_name,
                usage.identity_metadata.run_as as run_as_user,
                usage.product_features.is_serverless as is_serverless,
                usage.product_features.is_photon as is_photon,
                (usage.usage_quantity * list_prices.pricing.effective_list.default) as dollar_cost
            FROM system.billing.usage usage
            JOIN system.billing.list_prices list_prices 
                ON list_prices.sku_name = usage.sku_name
            WHERE usage.usage_end_time >= list_prices.price_start_time
                AND (list_prices.price_end_time IS NULL OR usage.usage_end_time < list_prices.price_end_time)
                AND usage.usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            ORDER BY usage.usage_date DESC, dollar_cost DESC
            """
            return self.client.execute_query(query)
        except Exception as e:
            logger.warning(f"list_prices query failed: {str(e)}")
            return []
    
    def _aggregate_results(self, results: List[Dict], start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Aggregate raw results into summary structures."""
        # Aggregate costs by various dimensions
        cost_by_product = {}
        cost_by_sku = {}
        cost_by_workspace = {}
        cost_by_cluster = {}
        cost_by_job = {}
        cost_by_warehouse = {}
        cost_by_user = {}
        serverless_cost = 0.0
        classic_cost = 0.0
        serverless_dbus = 0.0
        classic_dbus = 0.0
        total_dbus = 0.0
        total_cost = 0.0
        
        for row in results:
            product = row.get("billing_origin_product") or "UNKNOWN"
            sku = row.get("sku_name") or "UNKNOWN"
            workspace = row.get("workspace_id") or "UNKNOWN"
            cluster_id = row.get("cluster_id")
            job_id = row.get("job_id")
            warehouse_id = row.get("warehouse_id")
            user = row.get("run_as_user") or "UNKNOWN"
            is_serverless = row.get("is_serverless")
            quantity = float(row.get("usage_quantity") or 0)
            cost = float(row.get("dollar_cost") or 0)
            
            # Track serverless vs classic
            if is_serverless:
                serverless_cost += cost
                serverless_dbus += quantity
            else:
                classic_cost += cost
                classic_dbus += quantity
            
            # Aggregate by product
            if product not in cost_by_product:
                cost_by_product[product] = {"dbus": 0.0, "cost": 0.0, "serverless": 0.0, "classic": 0.0}
            cost_by_product[product]["dbus"] += quantity
            cost_by_product[product]["cost"] += cost
            if is_serverless:
                cost_by_product[product]["serverless"] += cost
            else:
                cost_by_product[product]["classic"] += cost
            
            # Aggregate by SKU
            if sku not in cost_by_sku:
                cost_by_sku[sku] = {"dbus": 0.0, "cost": 0.0}
            cost_by_sku[sku]["dbus"] += quantity
            cost_by_sku[sku]["cost"] += cost
            
            # Aggregate by workspace
            if workspace not in cost_by_workspace:
                cost_by_workspace[workspace] = {"dbus": 0.0, "cost": 0.0}
            cost_by_workspace[workspace]["dbus"] += quantity
            cost_by_workspace[workspace]["cost"] += cost
            
            # Aggregate by cluster
            if cluster_id:
                if cluster_id not in cost_by_cluster:
                    cost_by_cluster[cluster_id] = {"dbus": 0.0, "cost": 0.0}
                cost_by_cluster[cluster_id]["dbus"] += quantity
                cost_by_cluster[cluster_id]["cost"] += cost
            
            # Aggregate by job
            if job_id:
                if job_id not in cost_by_job:
                    cost_by_job[job_id] = {"dbus": 0.0, "cost": 0.0}
                cost_by_job[job_id]["dbus"] += quantity
                cost_by_job[job_id]["cost"] += cost
            
            # Aggregate by warehouse
            if warehouse_id:
                if warehouse_id not in cost_by_warehouse:
                    cost_by_warehouse[warehouse_id] = {"dbus": 0.0, "cost": 0.0}
                cost_by_warehouse[warehouse_id]["dbus"] += quantity
                cost_by_warehouse[warehouse_id]["cost"] += cost
            
            # Aggregate by user
            if user and user != "UNKNOWN":
                if user not in cost_by_user:
                    cost_by_user[user] = {"dbus": 0.0, "cost": 0.0}
                cost_by_user[user]["dbus"] += quantity
                cost_by_user[user]["cost"] += cost
            
            total_dbus += quantity
            total_cost += cost
        
        logger.info(f"Total DBUs: {total_dbus:.2f}, Total Cost: ${total_cost:.2f}")
        logger.info(f"Serverless: ${serverless_cost:.2f} ({serverless_dbus:.2f} DBUs), Classic: ${classic_cost:.2f} ({classic_dbus:.2f} DBUs)")
        logger.info(f"Cost by product: {cost_by_product}")
        
        # Additional analysis: tagging and usage patterns
        tagging_analysis = self._analyze_tagging(start_date, end_date)
        usage_patterns = self._analyze_usage_patterns(start_date, end_date)
        
        return {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": (end_date - start_date).days,
            },
            "total_dbus": total_dbus,
            "total_cost": total_cost,
            "serverless_cost": serverless_cost,
            "serverless_dbus": serverless_dbus,
            "classic_cost": classic_cost,
            "classic_dbus": classic_dbus,
            "cost_by_product": cost_by_product,
            "cost_by_sku": cost_by_sku,
            "cost_by_workspace": cost_by_workspace,
            "cost_by_cluster": cost_by_cluster,
            "cost_by_job": cost_by_job,
            "cost_by_warehouse": cost_by_warehouse,
            "cost_by_user": cost_by_user,
            "tagging_analysis": tagging_analysis,
            "usage_patterns": usage_patterns,
            "raw_data": results,
        }
    
    def _analyze_tagging(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Analyze cost attribution by custom tags - identify untagged spend."""
        try:
            query = f"""
            SELECT
                CASE 
                    WHEN custom_tags IS NULL OR size(custom_tags) = 0 THEN 'untagged'
                    ELSE 'tagged'
                END as tag_status,
                SUM(usage_quantity) as total_dbus,
                COUNT(DISTINCT usage_metadata.cluster_id) as cluster_count,
                COUNT(DISTINCT usage_metadata.job_id) as job_count
            FROM system.billing.usage
            WHERE usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY 1
            """
            results = self.client.execute_query(query)
            
            tagged_dbus = 0
            untagged_dbus = 0
            for row in results:
                if row.get("tag_status") == "tagged":
                    tagged_dbus = float(row.get("total_dbus", 0) or 0)
                else:
                    untagged_dbus = float(row.get("total_dbus", 0) or 0)
            
            total = tagged_dbus + untagged_dbus
            untagged_pct = (untagged_dbus / total * 100) if total > 0 else 0
            
            logger.info(f"Tagging analysis: {untagged_pct:.1f}% untagged spend")
            
            return {
                "tagged_dbus": tagged_dbus,
                "untagged_dbus": untagged_dbus,
                "untagged_percentage": round(untagged_pct, 1),
                "has_tagging_gap": untagged_pct > 20,
            }
        except Exception as e:
            logger.warning(f"Could not analyze tagging: {str(e)}")
            return {"tagged_dbus": 0, "untagged_dbus": 0, "untagged_percentage": 0, "has_tagging_gap": False}
    
    def _analyze_usage_patterns(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Analyze usage by day-of-week and hour to identify off-hours/weekend waste."""
        try:
            # Cost by day of week
            dow_query = f"""
            SELECT
                DAYOFWEEK(usage_date) as day_of_week,
                SUM(usage_quantity) as total_dbus
            FROM system.billing.usage
            WHERE usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY 1
            ORDER BY 1
            """
            dow_results = self.client.execute_query(dow_query)
            
            # Day names (1=Sunday in Spark)
            day_names = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 
                        5: "Thursday", 6: "Friday", 7: "Saturday"}
            
            cost_by_day = {}
            weekend_dbus = 0
            weekday_dbus = 0
            
            for row in dow_results:
                dow = row.get("day_of_week")
                dbus = float(row.get("total_dbus", 0) or 0)
                if dow:
                    day_name = day_names.get(dow, f"Day {dow}")
                    cost_by_day[day_name] = dbus
                    if dow in (1, 7):  # Sunday or Saturday
                        weekend_dbus += dbus
                    else:
                        weekday_dbus += dbus
            
            total = weekend_dbus + weekday_dbus
            weekend_pct = (weekend_dbus / total * 100) if total > 0 else 0
            
            # Expected: 2/7 days = ~28.6% if uniform usage
            # If weekend is >15% of weekday avg, it's likely forgotten clusters
            weekday_avg = weekday_dbus / 5 if weekday_dbus > 0 else 0
            weekend_avg = weekend_dbus / 2 if weekend_dbus > 0 else 0
            weekend_ratio = weekend_avg / weekday_avg if weekday_avg > 0 else 0
            
            # "Forgotten" = weekend usage that's >15% of weekday average
            has_weekend_waste = weekend_ratio > 0.15 and weekend_dbus > 10
            
            logger.info(f"Usage patterns: Weekend {weekend_pct:.1f}% of total, ratio {weekend_ratio:.2f}")
            
            return {
                "cost_by_day": cost_by_day,
                "weekend_dbus": weekend_dbus,
                "weekday_dbus": weekday_dbus,
                "weekend_percentage": round(weekend_pct, 1),
                "weekend_to_weekday_ratio": round(weekend_ratio, 2),
                "has_weekend_waste": has_weekend_waste,
            }
        except Exception as e:
            logger.warning(f"Could not analyze usage patterns: {str(e)}")
            return {"cost_by_day": {}, "weekend_dbus": 0, "weekday_dbus": 0, 
                   "weekend_percentage": 0, "weekend_to_weekday_ratio": 0, "has_weekend_waste": False}
