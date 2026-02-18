"""
Cluster Utilization Collector - CPU/Memory pressure analysis using system.compute.node_timeline.

Based on KPI framework from: https://medium.com/@mzeoli.it/we-built-kpis-to-right-size-databricks-job-clusters-heres-how

Key data sources:
- system.compute.node_timeline: Per-node CPU and memory utilization over time
- system.billing.usage: DBU consumption (cost proxy)
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ClusterUtilizationCollector:
    """Collects CPU/memory utilization metrics for cluster rightsizing analysis."""
    
    # Configurable thresholds (can be overridden via config)
    DEFAULT_THRESHOLDS = {
        # Under-provisioned detection
        "cpu_p90_high": 0.85,        # If P90 CPU >= 85%, indicates CPU bottleneck
        "mem_p95_high": 0.95,        # If P95 memory >= 95%, risk of OOM
        "hot_time_frac_hi": 0.20,    # If >=20% time above hot thresholds
        
        # Over-provisioned detection
        "cpu_p50_low": 0.40,         # If median CPU <= 40% AND low hot-time
        "mem_p50_low": 0.70,         # If median memory <= 70% AND low hot-time
        "hot_time_frac_lo": 0.05,    # <5% time above hot = minimal stress
        
        # Raw percentage thresholds
        "cpu_hot_pct": 80.0,         # CPU "hot" threshold
        "cpu_very_hot_pct": 90.0,    # CPU "very hot" threshold
        "mem_hot_pct": 90.0,         # Memory "hot" threshold
        "mem_very_hot_pct": 95.0,    # Memory "very hot" threshold (OOM risk)
    }
    
    def __init__(self, client, config: Dict[str, Any]):
        """Initialize the collector."""
        self.client = client
        self.config = config
        self.thresholds = {**self.DEFAULT_THRESHOLDS, **config.get("utilization_thresholds", {})}
    
    def collect(self, days: int = 30, top_n: int = 20) -> Dict[str, Any]:
        """
        Collect cluster utilization metrics for rightsizing analysis.
        
        Args:
            days: Number of days to analyze
            top_n: Number of top-cost clusters to analyze
            
        Returns:
            Dict containing utilization analysis results
        """
        logger.info(f"Collecting cluster utilization metrics for last {days} days...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Check if node_timeline table is accessible
        if not self._check_table_access():
            logger.warning("system.compute.node_timeline not accessible - skipping utilization analysis")
            return {
                "available": False,
                "error": "system.compute.node_timeline table not accessible",
                "cluster_metrics": [],
                "summary": {},
            }
        
        # Get utilization metrics for top-cost clusters
        cluster_metrics = self._collect_cluster_metrics(start_date, end_date, top_n)
        
        # Generate summary statistics
        summary = self._generate_summary(cluster_metrics)
        
        # Additional analyses
        idle_clusters = self._detect_idle_clusters(start_date, end_date)
        driver_imbalance = self._analyze_driver_worker_imbalance(cluster_metrics)
        autoscale_analysis = self._analyze_autoscale_effectiveness(start_date, end_date)
        
        logger.info(f"Collected utilization metrics for {len(cluster_metrics)} clusters")
        
        return {
            "available": True,
            "period_days": days,
            "thresholds": self.thresholds,
            "cluster_metrics": cluster_metrics,
            "summary": summary,
            "idle_clusters": idle_clusters,
            "driver_imbalance": driver_imbalance,
            "autoscale_analysis": autoscale_analysis,
        }
    
    def _check_table_access(self) -> bool:
        """Check if system.compute.node_timeline is accessible."""
        try:
            query = "SELECT 1 FROM system.compute.node_timeline LIMIT 1"
            self.client.execute_query(query)
            return True
        except Exception as e:
            logger.debug(f"node_timeline access check failed: {str(e)}")
            return False
    
    def _collect_cluster_metrics(
        self, start_date: datetime, end_date: datetime, top_n: int
    ) -> List[Dict[str, Any]]:
        """Collect CPU/memory metrics for top-cost clusters."""
        
        # Get thresholds for SQL
        cpu_hot = self.thresholds["cpu_hot_pct"]
        cpu_very_hot = self.thresholds["cpu_very_hot_pct"]
        mem_hot = self.thresholds["mem_hot_pct"]
        mem_very_hot = self.thresholds["mem_very_hot_pct"]
        
        query = f"""
        WITH target_clusters AS (
            -- Identify top {top_n} highest-DBU clusters (excluding serverless)
            SELECT 
                usage_metadata.cluster_id as cluster_id,
                ROUND(SUM(usage_quantity), 3) AS total_dbus,
                ANY_VALUE(usage_metadata.job_id) as job_id
            FROM system.billing.usage
            WHERE usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
                AND usage_metadata.cluster_id IS NOT NULL
                AND (product_features.is_serverless = false OR product_features.is_serverless IS NULL)
            GROUP BY usage_metadata.cluster_id
            HAVING SUM(usage_quantity) > 10  -- Minimum threshold
            ORDER BY total_dbus DESC
            LIMIT {top_n}
        ),
        
        cluster_info AS (
            -- Get cluster metadata (most recent config only)
            SELECT 
                c.cluster_id,
                COALESCE(c.cluster_name, 'Unknown') as cluster_name,
                c.driver_node_type,
                c.worker_node_type,
                c.min_autoscale_workers,
                c.max_autoscale_workers
            FROM system.compute.clusters c
            JOIN target_clusters tc ON c.cluster_id = tc.cluster_id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY c.cluster_id ORDER BY c.change_time DESC) = 1
        ),
        
        timeline_data AS (
            -- Get raw per-node CPU and memory metrics
            SELECT
                nt.cluster_id,
                CASE WHEN nt.driver THEN 'driver' ELSE 'worker' END AS component,
                (COALESCE(nt.cpu_system_percent, 0) + COALESCE(nt.cpu_user_percent, 0) + COALESCE(nt.cpu_wait_percent, 0)) AS cpu_total_pct,
                COALESCE(nt.mem_used_percent, 0) AS mem_pct
            FROM system.compute.node_timeline nt
            JOIN target_clusters tc ON nt.cluster_id = tc.cluster_id
            WHERE nt.start_time >= '{start_date}'
        ),
        
        aggregated AS (
            -- Aggregate metrics per cluster/component
            SELECT
                t.cluster_id,
                t.component,
                
                -- CPU percentiles (0-1 scale)
                ROUND(PERCENTILE(t.cpu_total_pct, 0.25) / 100.0, 4) AS cpu_p25,
                ROUND(PERCENTILE(t.cpu_total_pct, 0.50) / 100.0, 4) AS cpu_p50,
                ROUND(PERCENTILE(t.cpu_total_pct, 0.75) / 100.0, 4) AS cpu_p75,
                ROUND(PERCENTILE(t.cpu_total_pct, 0.90) / 100.0, 4) AS cpu_p90,
                ROUND(PERCENTILE(t.cpu_total_pct, 0.99) / 100.0, 4) AS cpu_p99,
                
                -- Memory percentiles (0-1 scale)
                ROUND(PERCENTILE(t.mem_pct, 0.25) / 100.0, 4) AS mem_p25,
                ROUND(PERCENTILE(t.mem_pct, 0.50) / 100.0, 4) AS mem_p50,
                ROUND(PERCENTILE(t.mem_pct, 0.75) / 100.0, 4) AS mem_p75,
                ROUND(PERCENTILE(t.mem_pct, 0.90) / 100.0, 4) AS mem_p90,
                ROUND(PERCENTILE(t.mem_pct, 0.95) / 100.0, 4) AS mem_p95,
                ROUND(PERCENTILE(t.mem_pct, 0.99) / 100.0, 4) AS mem_p99,
                ROUND(MAX(t.mem_pct) / 100.0, 4) AS mem_max,
                
                -- Time above thresholds (fraction)
                ROUND(AVG(CASE WHEN t.cpu_total_pct >= {cpu_hot} THEN 1.0 ELSE 0.0 END), 4) AS cpu_time_above_80pct,
                ROUND(AVG(CASE WHEN t.cpu_total_pct >= {cpu_very_hot} THEN 1.0 ELSE 0.0 END), 4) AS cpu_time_above_90pct,
                ROUND(AVG(CASE WHEN t.mem_pct >= {mem_hot} THEN 1.0 ELSE 0.0 END), 4) AS mem_time_above_90pct,
                ROUND(AVG(CASE WHEN t.mem_pct >= {mem_very_hot} THEN 1.0 ELSE 0.0 END), 4) AS mem_time_above_95pct,
                
                COUNT(*) AS sample_count
            FROM timeline_data t
            GROUP BY t.cluster_id, t.component
            HAVING COUNT(*) >= 10  -- Minimum samples for reliability
        )
        
        SELECT
            a.cluster_id,
            ci.cluster_name,
            tc.total_dbus,
            tc.job_id,
            a.component,
            CASE WHEN a.component = 'driver' THEN ci.driver_node_type ELSE ci.worker_node_type END AS node_type,
            ci.min_autoscale_workers,
            ci.max_autoscale_workers,
            
            a.cpu_p25, a.cpu_p50, a.cpu_p75, a.cpu_p90, a.cpu_p99,
            a.mem_p25, a.mem_p50, a.mem_p75, a.mem_p90, a.mem_p95, a.mem_p99, a.mem_max,
            a.cpu_time_above_80pct, a.cpu_time_above_90pct,
            a.mem_time_above_90pct, a.mem_time_above_95pct,
            a.sample_count
        FROM aggregated a
        JOIN cluster_info ci ON a.cluster_id = ci.cluster_id
        JOIN target_clusters tc ON a.cluster_id = tc.cluster_id
        ORDER BY tc.total_dbus DESC, a.cluster_id, 
            CASE a.component WHEN 'driver' THEN 0 ELSE 1 END
        """
        
        try:
            results = self.client.execute_query(query)
            return self._process_metrics(results)
        except Exception as e:
            logger.error(f"Error collecting cluster metrics: {str(e)}")
            return []
    
    def _process_metrics(self, raw_results: List[Dict]) -> List[Dict[str, Any]]:
        """Process raw metrics and add status classifications."""
        
        processed = []
        for row in raw_results:
            # Extract values
            cpu_p50 = float(row.get("cpu_p50") or 0)
            cpu_p90 = float(row.get("cpu_p90") or 0)
            mem_p50 = float(row.get("mem_p50") or 0)
            mem_p95 = float(row.get("mem_p95") or 0)
            cpu_time_80 = float(row.get("cpu_time_above_80pct") or 0)
            mem_time_90 = float(row.get("mem_time_above_90pct") or 0)
            
            # Determine CPU status
            cpu_hot = (cpu_p90 >= self.thresholds["cpu_p90_high"] or 
                      cpu_time_80 >= self.thresholds["hot_time_frac_hi"])
            cpu_cold = (cpu_p50 <= self.thresholds["cpu_p50_low"] and 
                       cpu_time_80 < self.thresholds["hot_time_frac_lo"])
            
            if cpu_hot:
                cpu_status = "under-provisioned"
            elif cpu_cold:
                cpu_status = "over-provisioned"
            else:
                cpu_status = "right-sized"
            
            # Determine memory status
            mem_hot = (mem_p95 >= self.thresholds["mem_p95_high"] or 
                      mem_time_90 >= self.thresholds["hot_time_frac_hi"])
            mem_cold = (mem_p50 <= self.thresholds["mem_p50_low"] and 
                       mem_time_90 < self.thresholds["hot_time_frac_lo"])
            
            if mem_hot:
                mem_status = "under-provisioned"
            elif mem_cold:
                mem_status = "over-provisioned"
            else:
                mem_status = "right-sized"
            
            # Overall status
            if mem_hot or cpu_hot:
                overall_status = "under-provisioned"
            elif mem_cold or cpu_cold:
                overall_status = "over-provisioned"
            else:
                overall_status = "right-sized"
            
            # Suggested action
            if mem_hot and cpu_hot:
                suggested_action = "Increase memory first; reassess CPU"
            elif mem_hot:
                suggested_action = "Increase memory or reduce concurrency"
            elif cpu_hot:
                suggested_action = "Increase CPU (cores) or improve parallelism"
            elif mem_cold and cpu_cold:
                suggested_action = "Consider downsizing (both CPU & memory)"
            elif mem_cold:
                suggested_action = "Consider reducing memory (smaller node type)"
            elif cpu_cold:
                suggested_action = "Consider reducing CPU (fewer/smaller nodes)"
            else:
                suggested_action = "Keep current configuration"
            
            # Calculate headroom
            cpu_headroom = round(1.0 - cpu_p50, 3)
            mem_headroom = round(1.0 - mem_p95, 3)
            
            processed.append({
                "cluster_id": row.get("cluster_id"),
                "cluster_name": row.get("cluster_name"),
                "job_id": row.get("job_id"),
                "component": row.get("component"),
                "node_type": row.get("node_type"),
                "total_dbus": float(row.get("total_dbus") or 0),
                "autoscale_min": row.get("min_autoscale_workers"),
                "autoscale_max": row.get("max_autoscale_workers"),
                
                # CPU metrics
                "cpu_p25": float(row.get("cpu_p25") or 0),
                "cpu_p50": cpu_p50,
                "cpu_p75": float(row.get("cpu_p75") or 0),
                "cpu_p90": cpu_p90,
                "cpu_p99": float(row.get("cpu_p99") or 0),
                "cpu_time_above_80pct": cpu_time_80,
                "cpu_time_above_90pct": float(row.get("cpu_time_above_90pct") or 0),
                
                # Memory metrics
                "mem_p25": float(row.get("mem_p25") or 0),
                "mem_p50": mem_p50,
                "mem_p75": float(row.get("mem_p75") or 0),
                "mem_p90": float(row.get("mem_p90") or 0),
                "mem_p95": mem_p95,
                "mem_p99": float(row.get("mem_p99") or 0),
                "mem_max": float(row.get("mem_max") or 0),
                "mem_time_above_90pct": mem_time_90,
                "mem_time_above_95pct": float(row.get("mem_time_above_95pct") or 0),
                
                # Analysis
                "cpu_headroom_p50": cpu_headroom,
                "mem_headroom_p95": mem_headroom,
                "cpu_status": cpu_status,
                "mem_status": mem_status,
                "overall_status": overall_status,
                "suggested_action": suggested_action,
                "sample_count": int(row.get("sample_count") or 0),
            })
        
        return processed
    
    def _generate_summary(self, cluster_metrics: List[Dict]) -> Dict[str, Any]:
        """Generate summary statistics from cluster metrics."""
        
        if not cluster_metrics:
            return {
                "total_clusters_analyzed": 0,
                "over_provisioned_count": 0,
                "under_provisioned_count": 0,
                "right_sized_count": 0,
            }
        
        # Group by cluster (not component) for unique counts
        clusters_by_id = {}
        for m in cluster_metrics:
            cid = m["cluster_id"]
            if cid not in clusters_by_id:
                clusters_by_id[cid] = {
                    "cluster_id": cid,
                    "cluster_name": m["cluster_name"],
                    "total_dbus": m["total_dbus"],
                    "overall_status": m["overall_status"],
                    "suggested_action": m["suggested_action"],
                }
            # Use worker status as primary (more impactful than driver)
            if m["component"] == "worker":
                clusters_by_id[cid]["overall_status"] = m["overall_status"]
                clusters_by_id[cid]["suggested_action"] = m["suggested_action"]
        
        unique_clusters = list(clusters_by_id.values())
        
        over_provisioned = [c for c in unique_clusters if c["overall_status"] == "over-provisioned"]
        under_provisioned = [c for c in unique_clusters if c["overall_status"] == "under-provisioned"]
        right_sized = [c for c in unique_clusters if c["overall_status"] == "right-sized"]
        
        # Calculate potential savings from over-provisioned clusters
        # Conservative estimate: 20-30% cost reduction potential
        over_provisioned_dbus = sum(c["total_dbus"] for c in over_provisioned)
        estimated_savings_dbus = over_provisioned_dbus * 0.25  # 25% conservative estimate
        
        return {
            "total_clusters_analyzed": len(unique_clusters),
            "over_provisioned_count": len(over_provisioned),
            "under_provisioned_count": len(under_provisioned),
            "right_sized_count": len(right_sized),
            "over_provisioned_dbus": round(over_provisioned_dbus, 2),
            "under_provisioned_dbus": round(sum(c["total_dbus"] for c in under_provisioned), 2),
            "potential_savings_dbus": round(estimated_savings_dbus, 2),
            "over_provisioned_clusters": over_provisioned,
            "under_provisioned_clusters": under_provisioned,
        }
    
    def _detect_idle_clusters(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Detect clusters that are running but essentially idle (<5% CPU for >50% of time).
        These are prime candidates for termination or auto-stop policies.
        """
        try:
            query = f"""
            WITH cluster_usage AS (
                SELECT
                    nt.cluster_id,
                    AVG(COALESCE(nt.cpu_user_percent, 0) + COALESCE(nt.cpu_system_percent, 0)) as avg_cpu,
                    MAX(COALESCE(nt.cpu_user_percent, 0) + COALESCE(nt.cpu_system_percent, 0)) as max_cpu,
                    AVG(CASE WHEN (COALESCE(nt.cpu_user_percent, 0) + COALESCE(nt.cpu_system_percent, 0)) < 5 THEN 1.0 ELSE 0.0 END) as pct_time_idle,
                    COUNT(*) as sample_count
                FROM system.compute.node_timeline nt
                WHERE nt.start_time >= '{start_date}'
                    AND nt.driver = false  -- Focus on workers
                GROUP BY nt.cluster_id
                HAVING COUNT(*) >= 60  -- At least 1 hour of data
            ),
            cluster_costs AS (
                SELECT 
                    usage_metadata.cluster_id as cluster_id,
                    SUM(usage_quantity) as total_dbus
                FROM system.billing.usage
                WHERE usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
                    AND usage_metadata.cluster_id IS NOT NULL
                GROUP BY usage_metadata.cluster_id
            ),
            cluster_info AS (
                SELECT cluster_id, cluster_name FROM system.compute.clusters
            )
            SELECT
                cu.cluster_id,
                COALESCE(ci.cluster_name, cu.cluster_id) as cluster_name,
                ROUND(cu.avg_cpu, 2) as avg_cpu_percent,
                ROUND(cu.max_cpu, 2) as max_cpu_percent,
                ROUND(cu.pct_time_idle * 100, 1) as pct_time_idle,
                COALESCE(cc.total_dbus, 0) as total_dbus,
                cu.sample_count
            FROM cluster_usage cu
            LEFT JOIN cluster_costs cc ON cu.cluster_id = cc.cluster_id
            LEFT JOIN cluster_info ci ON cu.cluster_id = ci.cluster_id
            WHERE cu.pct_time_idle > 0.5  -- Idle >50% of the time
                AND cu.avg_cpu < 10  -- Average CPU <10%
            ORDER BY cc.total_dbus DESC NULLS LAST
            LIMIT 20
            """
            
            results = self.client.execute_query(query)
            
            idle_clusters = []
            for row in results:
                idle_clusters.append({
                    "cluster_id": row.get("cluster_id"),
                    "cluster_name": row.get("cluster_name"),
                    "avg_cpu_percent": float(row.get("avg_cpu_percent") or 0),
                    "max_cpu_percent": float(row.get("max_cpu_percent") or 0),
                    "pct_time_idle": float(row.get("pct_time_idle") or 0),
                    "total_dbus": float(row.get("total_dbus") or 0),
                    "wasted_dbus_estimate": float(row.get("total_dbus") or 0) * 0.8,  # 80% likely wasted
                })
            
            logger.info(f"Found {len(idle_clusters)} potentially idle clusters")
            return idle_clusters
            
        except Exception as e:
            logger.warning(f"Could not detect idle clusters: {str(e)}")
            return []
    
    def _analyze_driver_worker_imbalance(self, cluster_metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Identify clusters where driver is bottlenecked but workers are idle (or vice versa).
        This indicates architectural issues that waste resources.
        """
        imbalanced = []
        
        # Group metrics by cluster
        by_cluster = {}
        for m in cluster_metrics:
            cid = m["cluster_id"]
            if cid not in by_cluster:
                by_cluster[cid] = {"cluster_id": cid, "cluster_name": m["cluster_name"], "total_dbus": m["total_dbus"]}
            by_cluster[cid][m["component"]] = m
        
        for cid, data in by_cluster.items():
            driver = data.get("driver", {})
            worker = data.get("worker", {})
            
            if not driver or not worker:
                continue
            
            driver_cpu = driver.get("cpu_p90", 0)
            worker_cpu = worker.get("cpu_p90", 0)
            driver_mem = driver.get("mem_p95", 0)
            worker_mem = worker.get("mem_p95", 0)
            
            issue = None
            recommendation = None
            
            # Driver bottleneck: high driver utilization, low worker utilization
            if driver_cpu > 0.7 and worker_cpu < 0.3:
                issue = "driver_cpu_bottleneck"
                recommendation = "Driver is CPU-bound while workers are idle. Consider: (1) reducing collect() operations, (2) using coalesce() before actions, (3) driver-side processing optimization"
            elif driver_mem > 0.8 and worker_mem < 0.5:
                issue = "driver_memory_bottleneck"
                recommendation = "Driver memory pressure while workers have headroom. Avoid collecting large datasets to driver. Use distributed writes instead."
            # Worker bottleneck with idle driver (less common but indicates poor parallelization)
            elif worker_cpu > 0.8 and driver_cpu < 0.2:
                issue = "workers_saturated_driver_idle"
                recommendation = "Workers are saturated but driver is idle - this is expected for well-parallelized workloads. Consider adding more workers if jobs are slow."
            
            if issue:
                imbalanced.append({
                    "cluster_id": cid,
                    "cluster_name": data["cluster_name"],
                    "total_dbus": data["total_dbus"],
                    "issue": issue,
                    "driver_cpu_p90": round(driver_cpu, 2),
                    "worker_cpu_p90": round(worker_cpu, 2),
                    "driver_mem_p95": round(driver_mem, 2),
                    "worker_mem_p95": round(worker_mem, 2),
                    "recommendation": recommendation,
                })
        
        logger.info(f"Found {len(imbalanced)} clusters with driver/worker imbalance")
        return imbalanced
    
    def _analyze_autoscale_effectiveness(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Analyze how effectively autoscaling clusters are scaling.
        Identifies clusters that rarely scale down (wasting money) or scale up (hurting performance).
        """
        try:
            query = f"""
            WITH autoscale_clusters AS (
                SELECT 
                    cluster_id,
                    cluster_name,
                    min_autoscale_workers,
                    max_autoscale_workers
                FROM system.compute.clusters
                WHERE min_autoscale_workers IS NOT NULL
                    AND max_autoscale_workers IS NOT NULL
                    AND max_autoscale_workers > min_autoscale_workers
            ),
            worker_counts AS (
                SELECT
                    nt.cluster_id,
                    COUNT(DISTINCT nt.instance_id) as active_workers,
                    DATE(nt.start_time) as usage_date,
                    HOUR(nt.start_time) as usage_hour
                FROM system.compute.node_timeline nt
                JOIN autoscale_clusters ac ON nt.cluster_id = ac.cluster_id
                WHERE nt.start_time >= '{start_date}'
                    AND nt.driver = false
                GROUP BY nt.cluster_id, DATE(nt.start_time), HOUR(nt.start_time)
            ),
            cluster_scaling AS (
                SELECT
                    wc.cluster_id,
                    ac.cluster_name,
                    ac.min_autoscale_workers,
                    ac.max_autoscale_workers,
                    AVG(wc.active_workers) as avg_workers,
                    MIN(wc.active_workers) as min_observed_workers,
                    MAX(wc.active_workers) as max_observed_workers,
                    STDDEV(wc.active_workers) as worker_stddev,
                    COUNT(*) as sample_hours
                FROM worker_counts wc
                JOIN autoscale_clusters ac ON wc.cluster_id = ac.cluster_id
                GROUP BY wc.cluster_id, ac.cluster_name, ac.min_autoscale_workers, ac.max_autoscale_workers
                HAVING COUNT(*) >= 10
            ),
            cluster_costs AS (
                SELECT 
                    usage_metadata.cluster_id as cluster_id,
                    SUM(usage_quantity) as total_dbus
                FROM system.billing.usage
                WHERE usage_date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
                    AND usage_metadata.cluster_id IS NOT NULL
                GROUP BY usage_metadata.cluster_id
            )
            SELECT
                cs.*,
                COALESCE(cc.total_dbus, 0) as total_dbus,
                ROUND((cs.avg_workers - cs.min_autoscale_workers) / 
                      NULLIF(cs.max_autoscale_workers - cs.min_autoscale_workers, 0) * 100, 1) as scale_utilization_pct
            FROM cluster_scaling cs
            LEFT JOIN cluster_costs cc ON cs.cluster_id = cc.cluster_id
            ORDER BY cc.total_dbus DESC NULLS LAST
            LIMIT 20
            """
            
            results = self.client.execute_query(query)
            
            never_scales_down = []
            never_scales_up = []
            healthy_scaling = []
            
            for row in results:
                min_workers = int(row.get("min_autoscale_workers") or 0)
                max_workers = int(row.get("max_autoscale_workers") or 1)
                avg_workers = float(row.get("avg_workers") or 0)
                min_observed = int(row.get("min_observed_workers") or 0)
                max_observed = int(row.get("max_observed_workers") or 0)
                worker_stddev = float(row.get("worker_stddev") or 0)
                total_dbus = float(row.get("total_dbus") or 0)
                
                cluster_info = {
                    "cluster_id": row.get("cluster_id"),
                    "cluster_name": row.get("cluster_name"),
                    "autoscale_min": min_workers,
                    "autoscale_max": max_workers,
                    "avg_workers": round(avg_workers, 1),
                    "min_observed": min_observed,
                    "max_observed": max_observed,
                    "total_dbus": total_dbus,
                }
                
                # Never scales down: min observed == max configured or avg very close to max
                if min_observed >= max_workers * 0.9 or avg_workers >= max_workers * 0.95:
                    cluster_info["issue"] = "never_scales_down"
                    cluster_info["recommendation"] = f"Cluster always runs near max ({max_workers} workers). Consider: fixed-size cluster (save autoscale overhead) or increase max if jobs are slow."
                    cluster_info["wasted_dbus_estimate"] = total_dbus * 0.15  # ~15% wasted on unnecessary capacity
                    never_scales_down.append(cluster_info)
                # Never scales up: max observed == min configured
                elif max_observed <= min_workers * 1.1 and max_workers > min_workers * 1.5:
                    cluster_info["issue"] = "never_scales_up"
                    cluster_info["recommendation"] = f"Cluster never uses autoscaling (stays at {min_workers} workers). Reduce max_workers to save on idle capacity or investigate why scaling isn't triggering."
                    never_scales_up.append(cluster_info)
                # Low variance - might not need autoscaling
                elif worker_stddev < 0.5 and max_workers > min_workers + 2:
                    cluster_info["issue"] = "low_variance"
                    cluster_info["recommendation"] = f"Worker count is very stable (stddev={worker_stddev:.1f}). Consider fixed-size cluster at {round(avg_workers)} workers."
                    healthy_scaling.append(cluster_info)
                else:
                    healthy_scaling.append(cluster_info)
            
            return {
                "never_scales_down": never_scales_down,
                "never_scales_up": never_scales_up,
                "healthy_scaling": healthy_scaling,
                "total_analyzed": len(results),
            }
            
        except Exception as e:
            logger.warning(f"Could not analyze autoscale effectiveness: {str(e)}")
            return {"never_scales_down": [], "never_scales_up": [], "healthy_scaling": [], "total_analyzed": 0}
