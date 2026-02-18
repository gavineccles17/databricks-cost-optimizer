"""Generates actionable cost optimization recommendations."""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """Generates actionable infrastructure recommendations - focus on quick wins."""
    
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
        warehouses_data: Dict[str, Any] = None,
        usage_data: Dict[str, Any] = None,
        utilization_data: Dict[str, Any] = None,
        queries_data: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate actionable recommendations focused on quick infrastructure wins.
        
        Prioritizes:
        1. Config changes (auto-terminate, auto-stop, sizing)
        2. Compute type optimization (All-Purpose → Jobs, Photon evaluation)
        3. Cost-saving features (spot instances, pools)
        
        Deprioritizes:
        - Code changes (SQL rewrites)
        - Complex refactoring
        """
        logger.info("Generating recommendations...")
        
        recommendations = []
        warehouses_data = warehouses_data or {}
        usage_data = usage_data or {}
        utilization_data = utilization_data or {}
        queries_data = queries_data or {}
        
        total_dbus = cost_analysis.get("total_dbus", 0)
        total_cost = cost_analysis.get("total_cost", 0)
        
        if total_dbus == 0 or total_cost == 0:
            logger.info("No usage data found - skipping recommendations")
            return recommendations
        
        cost_by_product = cost_analysis.get("cost_by_product", {})
        monthly_cost = cost_analysis.get("estimated_monthly_cost", 0)
        
        # HIGH PRIORITY: Infrastructure quick wins
        recommendations.extend(self._cluster_auto_terminate(cluster_analysis, cost_analysis))
        recommendations.extend(self._warehouse_auto_stop(warehouses_data))
        recommendations.extend(self._warehouse_sizing(warehouses_data))
        recommendations.extend(self._warehouse_long_running(warehouses_data))
        recommendations.extend(self._all_purpose_to_jobs(cost_by_product, total_cost))
        recommendations.extend(self._photon_evaluation(cost_by_product, cost_analysis))
        recommendations.extend(self._serverless_opportunities(cost_by_product, job_analysis))
        recommendations.extend(self._cluster_sizing(cluster_analysis))
        recommendations.extend(self._cluster_rightsizing(utilization_data, cost_analysis))
        recommendations.extend(self._idle_clusters(utilization_data, cost_analysis))
        recommendations.extend(self._autoscale_issues(utilization_data, cost_analysis))
        recommendations.extend(self._driver_worker_imbalance(utilization_data))
        recommendations.extend(self._model_serving_scale_to_zero(cost_by_product))
        recommendations.extend(self._job_efficiency_issues(job_analysis))
        recommendations.extend(self._weekend_waste(usage_data, cost_analysis))
        
        # MEDIUM PRIORITY: Operational improvements
        recommendations.extend(self._spot_instances(cluster_analysis, job_analysis))
        recommendations.extend(self._cluster_pools(cluster_analysis, job_analysis))
        recommendations.extend(self._job_frequency(job_analysis))
        recommendations.extend(self._tagging_governance(usage_data))
        recommendations.extend(self._delta_optimization(sql_analysis, cost_analysis))
        recommendations.extend(self._disk_spill_upsize(queries_data, warehouses_data))
        recommendations.extend(self._shuffle_heavy_queries(queries_data))
        
        # LOW PRIORITY: Code-level optimizations (only if very significant)
        recommendations.extend(self._sql_quick_wins(sql_analysis, cost_by_product))
        
        # Always add governance
        recommendations.extend(self._cost_monitoring(cost_analysis))
        
        # Deduplicate
        seen_ids = set()
        unique_recs = []
        for rec in recommendations:
            if rec["id"] not in seen_ids:
                seen_ids.add(rec["id"])
                unique_recs.append(rec)
        
        # Filter out recommendations with zero or negligible savings (< $1/month)
        min_savings_threshold = 1.0
        filtered_recs = [
            rec for rec in unique_recs 
            if rec.get("estimated_savings", 0) >= min_savings_threshold
        ]
        
        # Sort: high severity first, then by savings
        severity_order = {"high": 0, "medium": 1, "low": 2}
        filtered_recs.sort(key=lambda x: (severity_order.get(x.get("severity", "low"), 3), -x.get("estimated_savings", 0)))
        
        logger.info(f"Generated {len(filtered_recs)} recommendations (filtered {len(unique_recs) - len(filtered_recs)} with <${min_savings_threshold} savings)")
        return filtered_recs

    # ================== HIGH PRIORITY: INFRASTRUCTURE QUICK WINS ==================
    
    def _cluster_auto_terminate(self, cluster_analysis: Dict, cost_analysis: Dict) -> List[Dict]:
        """Flag clusters without auto-termination - biggest easy win."""
        recs = []
        
        cluster_issues = cluster_analysis.get("issues", [])
        
        for issue in cluster_issues:
            if issue.get("type") == "no_autotermination":
                cluster_name = issue.get("cluster_name", "Unknown")
                cluster_id = issue.get("cluster_id", "unknown")
                cluster_cost = issue.get("cost", 0)
                
                # Estimate 30-50% of cluster cost is idle time without auto-term
                savings = cluster_cost * 0.4
                
                recs.append({
                    "id": f"auto_term_{cluster_id[:12]}",
                    "title": f"🚨 Enable auto-terminate on cluster '{cluster_name}'",
                    "severity": "high",
                    "category": "cluster",
                    "description": f"Cluster '{cluster_name}' has NO auto-termination. Once started, it runs forever until manually stopped. This is the #1 cause of unexpected Databricks bills.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        f"Go to Compute → Clusters → '{cluster_name}' → Edit",
                        "Under 'Autopilot Options', set 'Terminate after ___ minutes of inactivity'",
                        "Recommended: 30 minutes for dev, 60 minutes for shared clusters",
                        "Click 'Confirm' - takes effect immediately",
                    ],
                    "insight": f"Without auto-terminate, a cluster running overnight costs the same as a full workday. Setting 30-min timeout saves ~${savings:.2f}.",
                    "effort": "2 minutes",
                })
        
        return recs
    
    def _warehouse_auto_stop(self, warehouses_data: Dict) -> List[Dict]:
        """Flag warehouses without auto-stop."""
        recs = []
        
        warehouses = warehouses_data.get("warehouses", [])
        
        for wh in warehouses:
            wh_id = wh.get("warehouse_id", "unknown")
            wh_name = wh.get("warehouse_name") or wh_id
            auto_stop = wh.get("auto_stop_minutes")
            total_cost = wh.get("total_cost", 0)
            
            if auto_stop is not None and auto_stop == 0 and total_cost > 0:
                savings = total_cost * 0.3
                recs.append({
                    "id": f"wh_auto_stop_{wh_id[:12]}",
                    "title": f"🚨 Enable auto-stop on warehouse '{wh_name}'",
                    "severity": "high",
                    "category": "warehouse",
                    "description": f"SQL Warehouse '{wh_name}' has auto-stop DISABLED. It charges continuously once started, even with zero queries running.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        f"Go to SQL Warehouses → '{wh_name}' → Edit",
                        "Set 'Auto stop' to 10-15 minutes",
                        "Serverless warehouses restart in <5 seconds, so short timeout is fine",
                        "Save changes",
                    ],
                    "insight": f"A warehouse running 24/7 vs 8 hours/day = 3x higher cost. Auto-stop at 10 min saves ~${savings:.2f}.",
                    "effort": "1 minute",
                })
            elif auto_stop is not None and auto_stop > 60 and total_cost > 0:
                savings = total_cost * 0.1
                recs.append({
                    "id": f"wh_reduce_stop_{wh_id[:12]}",
                    "title": f"Reduce auto-stop on '{wh_name}' from {auto_stop} min to 10-15 min",
                    "severity": "medium",
                    "category": "warehouse",
                    "description": f"Warehouse '{wh_name}' waits {auto_stop} minutes before stopping. Most users don't need more than 10-15 minutes between queries.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        f"Edit warehouse '{wh_name}'",
                        "Reduce 'Auto stop' to 10-15 minutes",
                        "Serverless restarts are instant, so this won't impact users",
                    ],
                    "insight": f"Every extra minute of idle time costs money. Reducing from {auto_stop} to 15 min saves ~${savings:.2f}.",
                    "effort": "1 minute",
                })
        
        return recs
    
    def _warehouse_sizing(self, warehouses_data: Dict) -> List[Dict]:
        """Flag oversized warehouses."""
        recs = []
        
        warehouses = warehouses_data.get("warehouses", [])
        
        for wh in warehouses:
            wh_id = wh.get("warehouse_id", "unknown")
            wh_name = wh.get("warehouse_name") or wh_id
            wh_size = str(wh.get("warehouse_size", "")).upper()
            total_cost = wh.get("total_cost", 0)
            min_clusters = wh.get("min_clusters", 1)
            
            # Large warehouse sizes
            if wh_size in ["2X-LARGE", "3X-LARGE", "4X-LARGE"] and total_cost > 0:
                size_multiplier = {"2X-LARGE": 8, "3X-LARGE": 16, "4X-LARGE": 32}.get(wh_size, 8)
                savings = total_cost * 0.5  # Could potentially halve by downsizing
                
                recs.append({
                    "id": f"wh_size_{wh_id[:12]}",
                    "title": f"Consider downsizing warehouse '{wh_name}' from {wh_size}",
                    "severity": "medium",
                    "category": "warehouse",
                    "description": f"Warehouse '{wh_name}' is sized at {wh_size} ({size_multiplier}x base capacity). Unless you're running complex analytical queries, a smaller size may suffice.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        "Check Query History for this warehouse's queries",
                        "Look at 'Query Profile' → are queries using full capacity?",
                        "Try MEDIUM or LARGE first - you can always scale up",
                        f"Each size step = 2x cost difference",
                    ],
                    "insight": f"{wh_size} is {size_multiplier}x the cost of SMALL. If queries complete quickly, you're paying for unused capacity.",
                    "effort": "5 minutes to test",
                })
            
            # High min_clusters
            if min_clusters and min_clusters > 1 and total_cost > 0:
                savings = total_cost * 0.2
                recs.append({
                    "id": f"wh_min_clusters_{wh_id[:12]}",
                    "title": f"Reduce min_clusters on '{wh_name}' from {min_clusters} to 1",
                    "severity": "medium",
                    "category": "warehouse",
                    "description": f"Warehouse '{wh_name}' keeps {min_clusters} clusters running at all times. Unless you have constant concurrent users, min=1 saves money.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        f"Edit warehouse '{wh_name}'",
                        "Set 'Min clusters' to 1",
                        "Keep 'Max clusters' at current value for burst capacity",
                        "Auto-scaling adds clusters in seconds when needed",
                    ],
                    "insight": f"With min={min_clusters}, you pay for {min_clusters} clusters even at 2 AM. Set min=1 and let auto-scaling handle peaks.",
                    "effort": "1 minute",
                })
        
        return recs
    
    def _warehouse_long_running(self, warehouses_data: Dict) -> List[Dict]:
        """Flag warehouses that have been running or scaled up for too long."""
        recs = []
        
        # Long-running warehouses
        long_running = warehouses_data.get("long_running_warehouses", [])
        if long_running:
            # Estimate hourly cost based on warehouse size
            for wh in long_running:
                running_hours = wh.get("running_hours", 0)
                if running_hours > 4:  # Only flag if >4 hours continuous
                    wh_name = wh.get("warehouse_name", "Unknown")
                    cluster_count = wh.get("cluster_count", 1)
                    wh_size = wh.get("warehouse_size", "MEDIUM")
                    
                    # Rough hourly cost estimate by size
                    hourly_rates = {"SMALL": 2, "MEDIUM": 4, "LARGE": 8, "X-LARGE": 16, "2X-LARGE": 32}
                    base_rate = hourly_rates.get(str(wh_size).upper(), 4)
                    estimated_cost = running_hours * base_rate * cluster_count
                    
                    recs.append({
                        "id": f"wh_long_run_{wh.get('warehouse_id', 'unknown')[:8]}",
                        "title": f"🕐 Warehouse '{wh_name}' running for {running_hours:.1f} hours",
                        "severity": "high" if running_hours > 8 else "medium",
                        "category": "warehouse",
                        "description": f"Warehouse '{wh_name}' has been continuously running for {running_hours:.1f} hours with {cluster_count} cluster(s). Verify if this is intentional.",
                        "estimated_savings": round(estimated_cost * 0.5 * self.confidence_factor, 2),
                        "steps": [
                            "Check if the warehouse is actively being used",
                            "Review Query History for recent activity",
                            "If idle, stop the warehouse manually",
                            "Ensure auto-stop is configured (recommend 10-15 min)",
                        ],
                        "insight": f"A {wh_size} warehouse running {running_hours:.1f}h costs ~${estimated_cost:.0f}. If no queries are running, this is waste.",
                        "effort": "1 minute to stop",
                    })
        
        # Upscaled warehouses
        upscaled = warehouses_data.get("upscaled_warehouses", [])
        if upscaled:
            for wh in upscaled:
                upscaled_hours = wh.get("upscaled_hours", 0)
                if upscaled_hours > 1:  # Only flag if scaled up >1 hour
                    wh_name = wh.get("warehouse_name", "Unknown")
                    current_clusters = wh.get("current_clusters", 2)
                    max_clusters = wh.get("max_clusters", current_clusters)
                    
                    recs.append({
                        "id": f"wh_upscaled_{wh.get('warehouse_id', 'unknown')[:8]}",
                        "title": f"📈 Warehouse '{wh_name}' scaled up for {upscaled_hours:.1f} hours",
                        "severity": "medium",
                        "category": "warehouse",
                        "description": f"Warehouse '{wh_name}' has been running at {current_clusters} clusters (max: {max_clusters}) for {upscaled_hours:.1f} hours. This may indicate lack of scale-down activity.",
                        "estimated_savings": round(upscaled_hours * 4 * (current_clusters - 1) * self.confidence_factor, 2),
                        "steps": [
                            "Review query concurrency - do you need this many clusters?",
                            "Check if auto-scaling down is triggering",
                            "Consider reducing max_clusters if rarely needed",
                            "Review query patterns - batch queries if possible",
                        ],
                        "insight": f"Each additional cluster doubles cost. If queries aren't queuing, you may not need {current_clusters} clusters.",
                        "effort": "5 minutes to review settings",
                    })
        
        return recs
    
    def _disk_spill_upsize(self, queries_data: Dict, warehouses_data: Dict) -> List[Dict]:
        """Recommend upsizing warehouses that experience disk spill."""
        recs = []
        
        disk_spill = queries_data.get("disk_spill_analysis", {})
        warehouses_with_spill = disk_spill.get("warehouses_with_spill", [])
        
        # Build warehouse name lookup
        wh_names = {}
        for wh in warehouses_data.get("warehouses", []):
            wh_names[wh.get("warehouse_id")] = wh.get("warehouse_name", "Unknown")
        
        for wh_spill in warehouses_with_spill:
            if wh_spill.get("needs_upsize"):
                wh_id = wh_spill.get("warehouse_id")
                wh_name = wh_names.get(wh_id, wh_id or "Unknown")
                spill_freq = wh_spill.get("spill_frequency", 0)
                max_spill_gb = wh_spill.get("max_spilled_gb", 0)
                
                recs.append({
                    "id": f"disk_spill_{str(wh_id)[:8]}",
                    "title": f"💾 Upsize warehouse '{wh_name}' - experiencing disk spill",
                    "severity": "medium",
                    "category": "warehouse",
                    "description": f"Warehouse '{wh_name}' has {spill_freq} queries that spilled to disk (max: {max_spill_gb:.1f}GB). Disk spill indicates memory pressure and degrades performance.",
                    "estimated_savings": 0,  # Actually costs more but improves performance
                    "steps": [
                        "Disk spill = queries running out of memory and writing to slower disk",
                        f"This warehouse had {spill_freq} queries spill to disk",
                        "Upsize to the next t-shirt size (e.g., MEDIUM → LARGE)",
                        "OR optimize queries to use less memory (e.g., reduce shuffles)",
                    ],
                    "insight": f"While upsizing costs more per hour, queries complete faster with less spill. Often net-neutral or cheaper overall.",
                    "effort": "2 minutes to change size, test queries",
                })
        
        return recs
    
    def _shuffle_heavy_queries(self, queries_data: Dict) -> List[Dict]:
        """Flag queries with excessive shuffle as optimization candidates."""
        recs = []
        
        shuffle_data = queries_data.get("shuffle_analysis", {})
        shuffle_queries = shuffle_data.get("shuffle_heavy_queries", [])
        total_shuffle = shuffle_data.get("total_shuffle_queries", 0)
        
        if total_shuffle >= 5:  # Only if multiple shuffle-heavy queries
            # Get worst offenders
            worst = shuffle_queries[:3]
            query_examples = []
            for q in worst:
                preview = (q.get("statement_preview") or "")[:100]
                shuffle_gb = q.get("shuffle_gb", 0)
                query_examples.append(f"  - {shuffle_gb:.1f}GB shuffle: {preview}...")
            
            recs.append({
                "id": "shuffle_heavy_queries",
                "title": f"🔀 {total_shuffle} queries with excessive shuffle detected",
                "severity": "low",
                "category": "sql",
                "description": f"Found {total_shuffle} queries that shuffle large amounts of data between nodes. High shuffle indicates inefficient queries or poor table structure.",
                "estimated_savings": 0,  # Requires code changes
                "steps": [
                    "Shuffle = data movement between nodes (slow & expensive)",
                    "Common causes:",
                    "  - JOINs on non-partitioned columns",
                    "  - GROUP BY on high-cardinality columns",
                    "  - Uneven data distribution (skew)",
                    "Review these queries:",
                ] + query_examples + [
                    "Consider: partition tables by join keys, use broadcast joins for small tables",
                ],
                "insight": "Reducing shuffle improves query speed and reduces compute time. Focus on your most expensive queries first.",
                "effort": "1-4 hours per query to optimize",
            })
        
        return recs
    
    def _all_purpose_to_jobs(self, cost_by_product: Dict, total_cost: float) -> List[Dict]:
        """Recommend switching from All-Purpose/Interactive to Jobs compute."""
        recs = []
        
        interactive = cost_by_product.get("INTERACTIVE", {})
        all_purpose = cost_by_product.get("ALL_PURPOSE", {})
        
        interactive_cost = interactive.get("cost", 0) + all_purpose.get("cost", 0)
        
        if interactive_cost > 0:
            pct = (interactive_cost / total_cost * 100) if total_cost > 0 else 0
            # Jobs compute is roughly 2-3x cheaper
            savings = interactive_cost * 0.5  # 50% savings moving to jobs
            
            if pct > 10 or interactive_cost > 1:
                recs.append({
                    "id": "all_purpose_to_jobs",
                    "title": f"💰 Switch from All-Purpose to Jobs compute (${interactive_cost:.2f} on interactive)",
                    "severity": "high" if pct > 25 else "medium",
                    "category": "compute",
                    "description": f"You're spending ${interactive_cost:.2f} ({pct:.0f}%) on Interactive/All-Purpose compute. Jobs compute costs 2-3x LESS for the same work.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        "All-Purpose: ~$0.40/DBU | Jobs: ~$0.15/DBU (2.7x cheaper)",
                        "For scheduled/recurring workloads, create a Databricks Job instead",
                        "Jobs → Create Job → attach a notebook → set schedule",
                        "Use 'Job clusters' (auto-created, auto-terminated) not shared clusters",
                    ],
                    "insight": f"Same notebook on All-Purpose vs Jobs compute: Jobs is 60% cheaper. ${interactive_cost:.2f} could become ${interactive_cost * 0.4:.2f}.",
                    "effort": "15 minutes per workload",
                })
        
        return recs
    
    def _photon_evaluation(self, cost_by_product: Dict, cost_analysis: Dict) -> List[Dict]:
        """Evaluate if Photon is worth the 2x cost."""
        recs = []
        
        # Check raw data for Photon usage
        raw_data = cost_analysis.get("raw_data", []) if "raw_data" in cost_analysis else []
        
        photon_cost = 0
        non_photon_cost = 0
        photon_dbus = 0
        
        # This would require is_photon field from usage data
        for product, data in cost_by_product.items():
            # SQL warehouses and some clusters use Photon
            if product in ["SQL", "INTERACTIVE", "JOBS"]:
                # Rough estimate: assume some portion is Photon
                pass
        
        # For now, give general guidance on Photon
        sql_cost = cost_by_product.get("SQL", {}).get("cost", 0)
        if sql_cost > 5:
            recs.append({
                "id": "evaluate_photon",
                "title": "Evaluate Photon - is the 2x cost worth it for your queries?",
                "severity": "low",
                "category": "compute",
                "description": f"Photon accelerates SQL queries but costs ~2x more per DBU. It's worth it for CPU-bound queries, not for I/O-bound or simple queries.",
                "estimated_savings": round(sql_cost * 0.3 * self.confidence_factor, 2),
                "steps": [
                    "Check warehouse settings: is Photon enabled?",
                    "Photon helps: complex aggregations, joins, many transformations",
                    "Photon DOESN'T help: simple SELECT, I/O-bound, already fast queries",
                    "Test: Run same query with/without Photon, compare time × cost",
                ],
                "insight": "If Photon makes query 3x faster, it's worth 2x cost. If only 1.5x faster, you're losing money.",
                "effort": "30 minutes to benchmark",
            })
        
        return recs
    
    def _serverless_opportunities(self, cost_by_product: Dict, job_analysis: Dict) -> List[Dict]:
        """Identify opportunities for serverless compute."""
        recs = []
        
        jobs = job_analysis.get("jobs", [])
        
        # Find jobs running on classic compute with many runs
        for job in jobs[:5]:
            is_serverless = job.get("is_serverless", False)
            job_name = job.get("job_name") or str(job.get("job_id", ""))
            job_cost = float(job.get("total_cost", 0) or 0)
            run_count = job.get("run_count", 0)
            
            if not is_serverless and run_count >= 3 and job_cost > 0.1:
                savings = job_cost * 0.15
                recs.append({
                    "id": f"serverless_job_{str(job.get('job_id', ''))[:12]}",
                    "title": f"Try serverless for job '{job_name[:30]}' ({run_count} runs)",
                    "severity": "low",
                    "category": "jobs",
                    "description": f"Job '{job_name}' runs {run_count} times on classic compute. Serverless eliminates 5-10 min cluster startup time per run.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        "Edit job → Compute → select 'Serverless'",
                        "Serverless: instant start, per-second billing, no cluster management",
                        "Best for: short jobs, variable schedules, burst workloads",
                        "Try it: run the job once on serverless and compare cost",
                    ],
                    "insight": f"Cluster startup is ~5-10 min. For {run_count} runs, that's {run_count * 7} minutes of wasted startup time.",
                    "effort": "2 minutes to switch",
                })
                break  # Only one serverless recommendation
        
        return recs
    
    def _cluster_sizing(self, cluster_analysis: Dict) -> List[Dict]:
        """Flag oversized or fixed-size clusters."""
        recs = []
        
        cluster_issues = cluster_analysis.get("issues", [])
        
        for issue in cluster_issues:
            issue_type = issue.get("type")
            cluster_name = issue.get("cluster_name", "Unknown")
            cluster_id = issue.get("cluster_id", "unknown")
            cluster_cost = issue.get("cost", 0)
            
            if issue_type == "no_autoscaling":
                worker_count = issue.get("worker_count", 0)
                savings = cluster_cost * 0.25
                recs.append({
                    "id": f"autoscale_{cluster_id[:12]}",
                    "title": f"Enable autoscaling on cluster '{cluster_name}' (fixed at {worker_count} workers)",
                    "severity": "medium",
                    "category": "cluster",
                    "description": f"Cluster '{cluster_name}' has a fixed size of {worker_count} workers. Autoscaling scales down during low usage, saving money.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        f"Edit cluster '{cluster_name}'",
                        "Enable autoscaling: set min_workers=1, max_workers={worker_count}",
                        "Cluster scales down to 1 worker when idle, up to {worker_count} under load",
                        "Typical savings: 20-40% from avoiding over-provisioning",
                    ],
                    "insight": f"Fixed {worker_count} workers means paying for {worker_count} even when running a simple query. Autoscaling adjusts to actual demand.",
                    "effort": "2 minutes",
                })
            
            elif issue_type == "oversized":
                worker_count = issue.get("worker_count", 0)
                savings = cluster_cost * 0.4
                recs.append({
                    "id": f"rightsize_{cluster_id[:12]}",
                    "title": f"Review cluster '{cluster_name}' size ({worker_count} workers)",
                    "severity": "high" if worker_count >= 20 else "medium",
                    "category": "cluster",
                    "description": f"Cluster '{cluster_name}' is configured with {worker_count} workers. Large clusters should be reviewed for actual utilization.",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        "Check Spark UI → Executors tab: How many are active?",
                        "Check Ganglia metrics: CPU/memory utilization over time",
                        "If utilization <50%, reduce workers or enable autoscaling",
                        f"Consider: min=2, max={worker_count} instead of fixed {worker_count}",
                    ],
                    "insight": f"A {worker_count}-worker cluster costs {worker_count}x a single-worker cluster. If utilization is 30%, you're wasting 70%.",
                    "effort": "10 minutes to analyze",
                })
        
        return recs
    
    def _model_serving_scale_to_zero(self, cost_by_product: Dict) -> List[Dict]:
        """Recommend scale-to-zero for model serving endpoints.
        
        Note: MODEL_SERVING costs often include Databricks Foundation Model APIs
        (pay-per-token for built-in LLMs) which users cannot control via scale-to-zero.
        Only recommend if costs are significant and likely from user endpoints.
        """
        recs = []
        
        serving_data = cost_by_product.get("MODEL_SERVING", {})
        serving_cost = serving_data.get("cost", 0)
        
        # Skip if cost is low - likely just Foundation Model API usage
        # which is pay-per-use and not controllable via endpoint settings
        if serving_cost < 5:
            # Low MODEL_SERVING costs are typically Foundation Model APIs
            # (Databricks' built-in LLMs) or system inference, not user endpoints
            return recs
        
        # Only recommend if significant cost that's likely from user endpoints
        savings = serving_cost * 0.5
        recs.append({
            "id": "model_scale_to_zero",
            "title": f"Enable scale-to-zero for model serving endpoints (${serving_cost:.2f} spend)",
            "severity": "medium",
            "category": "ml",
            "description": f"Model serving costs ${serving_cost:.2f}. If you have custom model endpoints that aren't receiving constant traffic, enable scale-to-zero.",
            "estimated_savings": round(savings * self.confidence_factor, 2),
            "steps": [
                "Go to Serving → select endpoint → Edit",
                "Set 'Scale to zero' = enabled",
                "Set 'Min instances' = 0",
                "Endpoint spins down after idle period, restarts on first request",
                "Note: This doesn't apply to Foundation Model API usage (pay-per-token)",
            ],
            "insight": "Dev/test endpoints often sit idle 90% of the time. Scale-to-zero means paying only for actual inference requests.",
            "effort": "2 minutes per endpoint",
        })
        
        return recs

    # ================== MEDIUM PRIORITY: OPERATIONAL IMPROVEMENTS ==================
    
    def _spot_instances(self, cluster_analysis: Dict, job_analysis: Dict) -> List[Dict]:
        """Recommend spot instances for fault-tolerant workloads."""
        recs = []
        
        jobs = job_analysis.get("jobs", [])
        total_job_cost = sum(float(j.get("total_cost", 0) or 0) for j in jobs)
        
        if total_job_cost > 0.5:
            savings = total_job_cost * 0.6  # Spot is 60-90% cheaper
            recs.append({
                "id": "use_spot_instances",
                "title": f"Use spot instances for job clusters (up to 70% cheaper)",
                "severity": "medium",
                "category": "compute",
                "description": f"Job compute costs ${total_job_cost:.2f}. Spot instances cost 60-90% less than on-demand. Jobs can retry on spot preemption.",
                "estimated_savings": round(savings * self.confidence_factor, 2),
                "steps": [
                    "Edit job → Compute → Advanced → 'Use spot instances'",
                    "Or: in cluster config, set 'Spot/Preemptible' for worker nodes",
                    "Keep driver node on-demand for stability",
                    "Best for: batch jobs, ETL, jobs that can retry if interrupted",
                ],
                "insight": "Spot instances: same hardware, 60-90% cheaper. Databricks handles retries automatically if spot is reclaimed.",
                "effort": "2 minutes per job",
            })
        
        return recs
    
    def _job_efficiency_issues(self, job_analysis: Dict) -> List[Dict]:
        """Generate recommendations from job efficiency analysis."""
        recs = []
        
        efficiency_issues = job_analysis.get("efficiency_issues", [])
        
        for issue in efficiency_issues:
            issue_type = issue.get("type")
            job_name = issue.get("job_name", "Unknown")
            job_id = str(issue.get("job_id", "unknown"))
            
            if issue_type == "high_failure_rate":
                failure_rate = issue.get("failure_rate", 0)
                wasted_cost = issue.get("wasted_cost", 0)
                
                if wasted_cost >= 1:  # Only if significant waste
                    recs.append({
                        "id": f"job_failures_{job_id[:12]}",
                        "title": f"🔴 Fix failing job '{job_name[:25]}' ({failure_rate:.0f}% failure rate)",
                        "severity": "high",
                        "category": "jobs",
                        "description": f"Job '{job_name}' fails {failure_rate:.0f}% of the time, wasting ~${wasted_cost:.2f} in the analysis period on failed runs.",
                        "estimated_savings": round(wasted_cost * self.confidence_factor, 2),
                        "steps": [
                            "Check job run logs for error patterns",
                            "Common causes: data issues, resource exhaustion, timeout",
                            "Add error handling and retries for transient failures",
                            "Consider alerting on job failures",
                        ],
                        "insight": "Failed runs still consume compute resources and cost money.",
                        "effort": "Varies - depends on root cause",
                    })
            
            elif issue_type == "startup_overhead":
                avg_duration = issue.get("avg_duration", 0)
                cost_per_run = issue.get("cost_per_run", 0)
                
                if cost_per_run >= 0.10:  # $0.10+ per run is notable
                    recs.append({
                        "id": f"job_startup_{job_id[:12]}",
                        "title": f"Job '{job_name[:25]}' has high startup overhead ({avg_duration:.0f}s runtime, ${cost_per_run:.2f}/run)",
                        "severity": "medium",
                        "category": "jobs",
                        "description": f"Job only runs for {avg_duration:.0f} seconds but costs ${cost_per_run:.2f} per run. Cluster startup time likely exceeds actual work time.",
                        "estimated_savings": round(cost_per_run * 10 * 0.5 * self.confidence_factor, 2),
                        "steps": [
                            "Option 1: Use cluster pools - pre-warmed instances start in <1 min",
                            "Option 2: Batch multiple short jobs into one",
                            "Option 3: Use serverless jobs - no startup overhead",
                            "Option 4: If job runs frequently, keep cluster warm between runs",
                        ],
                        "insight": f"If startup takes 5 min but job runs {avg_duration:.0f}s, you're paying mostly for waiting.",
                        "effort": "15-30 minutes",
                    })
        
        return recs
    
    def _cluster_pools(self, cluster_analysis: Dict, job_analysis: Dict = None) -> List[Dict]:
        """Recommend cluster pools for faster startup based on cluster and job data."""
        recs = []
        job_analysis = job_analysis or {}
        
        cluster_costs = cluster_analysis.get("cluster_costs", [])
        short_run_jobs = job_analysis.get("short_run_overhead_jobs", [])
        
        # Strong signal: jobs with short runtimes but high cost per run
        if short_run_jobs and len(short_run_jobs) >= 2:
            total_affected_cost = sum(j.get("total_cost", 0) for j in short_run_jobs)
            job_names = [j.get("job_name", "")[:20] for j in short_run_jobs[:3]]
            
            recs.append({
                "id": "cluster_pools_startup",
                "title": f"Use cluster pools to reduce startup overhead ({len(short_run_jobs)} short-running jobs detected)",
                "severity": "medium",
                "category": "cluster",
                "description": f"Jobs like {', '.join(job_names)} have short runtimes but high cost per run - cluster startup dominates. Pools can reduce startup from 5-10 min to <1 min.",
                "estimated_savings": round(total_affected_cost * 0.3 * self.confidence_factor, 2),
                "steps": [
                    "Compute → Pools → Create Pool",
                    "Set min_idle_instances=1-2 based on job frequency",
                    "Edit job cluster configs to use the pool",
                    "Monitor: pools have a small cost for idle instances",
                ],
                "insight": "For short jobs, startup time can cost more than the actual job. Pools amortize this.",
                "effort": "15 minutes to set up",
            })
        elif len(cluster_costs) >= 2:
            total_cost = sum(float(c.get("total_cost", 0) or 0) for c in cluster_costs)
            if total_cost > 1:
                recs.append({
                    "id": "use_cluster_pools",
                    "title": "Consider cluster pools for faster startup times",
                    "severity": "low",
                    "category": "cluster",
                    "description": "Cluster pools pre-provision instances for faster cluster startup (seconds instead of minutes). Useful if you frequently start/stop clusters.",
                    "estimated_savings": round(total_cost * 0.05 * self.confidence_factor, 2),
                    "steps": [
                        "Compute → Pools → Create Pool",
                        "Set min_idle_instances=1-2 to have instances ready",
                        "Attach clusters to the pool in cluster settings",
                        "Tradeoff: small cost for idle instances vs faster startup",
                    ],
                    "insight": "Pools reduce startup from 5-10 min to <1 min. Worth it if developers complain about waiting for clusters.",
                    "effort": "15 minutes to set up",
                })
        
        return recs
    
    def _job_frequency(self, job_analysis: Dict) -> List[Dict]:
        """Identify jobs that run too frequently."""
        recs = []
        
        jobs = job_analysis.get("jobs", [])
        
        for job in jobs[:3]:
            run_count = job.get("run_count", 0)
            job_name = job.get("job_name") or str(job.get("job_id", ""))
            job_cost = float(job.get("total_cost", 0) or 0)
            
            # If job runs many times with notable cost
            if run_count >= 20 and job_cost > 0.5:
                cost_per_run = job_cost / run_count
                savings = job_cost * 0.3
                recs.append({
                    "id": f"job_freq_{str(job.get('job_id', ''))[:12]}",
                    "title": f"Review job frequency: '{job_name[:30]}' ({run_count} runs)",
                    "severity": "low",
                    "category": "jobs",
                    "description": f"Job '{job_name}' ran {run_count} times at ${cost_per_run:.3f}/run. Does it need to run this often?",
                    "estimated_savings": round(savings * self.confidence_factor, 2),
                    "steps": [
                        "Review: Does downstream need data this fresh?",
                        "Hourly job → every 4 hours = 75% cost reduction",
                        "Consider: trigger-based instead of scheduled (run when new data arrives)",
                        "Check: are multiple jobs doing overlapping work?",
                    ],
                    "insight": f"With {run_count} runs, reducing frequency by half saves ${job_cost * 0.5:.2f}.",
                    "effort": "5 minutes to evaluate",
                })
                break
        
        return recs

    # ================== LOW PRIORITY: CODE-LEVEL (only if very significant) ==================
    
    def _sql_quick_wins(self, sql_analysis: Dict, cost_by_product: Dict) -> List[Dict]:
        """Only mention SQL optimizations if they're very significant."""
        recs = []
        
        pattern_summary = sql_analysis.get("pattern_summary", {})
        query_count = sql_analysis.get("query_count", 0)
        sql_cost = cost_by_product.get("SQL", {}).get("cost", 0)
        
        if query_count == 0 or sql_cost < 1:
            return recs
        
        no_where_count = pattern_summary.get("no_where_clause", 0)
        no_where_pct = (no_where_count / query_count * 100) if query_count > 0 else 0
        
        # Only mention if a LOT of queries are affected
        if no_where_pct > 30 and no_where_count > 20:
            recs.append({
                "id": "sql_partitioning",
                "title": f"Consider table partitioning (optional: {no_where_count} queries without filters)",
                "severity": "low",
                "category": "sql",
                "description": f"{no_where_pct:.0f}% of queries don't have WHERE clauses. If tables are large, partitioning on a common filter column (like date) would help.",
                "estimated_savings": round(sql_cost * 0.1 * self.confidence_factor, 2),
                "steps": [
                    "Identify largest tables that are frequently queried",
                    "Find common filter patterns (usually date/timestamp)",
                    "ALTER TABLE ... PARTITION BY (date_column)",
                    "Queries with WHERE date = 'X' will only scan that partition",
                ],
                "insight": "Partitioning is a one-time change that benefits all future queries on that table. Best ROI for frequently-queried large tables.",
                "effort": "30-60 minutes per table",
            })
        
        return recs
    
    def _cluster_rightsizing(self, utilization_data: Dict, cost_analysis: Dict) -> List[Dict]:
        """Generate rightsizing recommendations based on actual CPU/memory utilization."""
        recs = []
        
        if not utilization_data.get("available"):
            return recs  # node_timeline not accessible
        
        summary = utilization_data.get("summary", {})
        cluster_metrics = utilization_data.get("cluster_metrics", [])
        
        over_provisioned_count = summary.get("over_provisioned_count", 0)
        under_provisioned_count = summary.get("under_provisioned_count", 0)
        over_provisioned_dbus = summary.get("over_provisioned_dbus", 0)
        potential_savings_dbus = summary.get("potential_savings_dbus", 0)
        
        # Get average DBU price from cost analysis
        total_cost = cost_analysis.get("total_cost", 0)
        total_dbus = cost_analysis.get("total_dbus", 0)
        avg_dbu_price = (total_cost / total_dbus) if total_dbus > 0 else 0.50
        
        # Over-provisioned clusters - downsize opportunity
        if over_provisioned_count > 0 and potential_savings_dbus > 10:
            estimated_savings = potential_savings_dbus * avg_dbu_price
            
            # Get details of top over-provisioned clusters
            over_clusters = summary.get("over_provisioned_clusters", [])[:5]
            top_clusters_info = []
            for c in over_clusters:
                name = c.get("cluster_name", c.get("cluster_id", "unknown"))
                dbus = c.get("total_dbus", 0)
                action = c.get("suggested_action", "")
                top_clusters_info.append(f"  - {name}: {dbus:.0f} DBUs - {action}")
            
            steps = [
                "Review CPU/memory utilization percentiles in the report",
                "For clusters with <40% median CPU AND <70% median memory:",
            ]
            if top_clusters_info:
                steps.append("Priority clusters to downsize:")
                steps.extend(top_clusters_info[:3])
            steps.extend([
                "Reduce worker count or switch to smaller instance types",
                "Consider autoscaling with lower min_workers",
                "Monitor for 1-2 weeks before finalizing changes",
            ])
            
            recs.append({
                "id": "rightsize_overprovisioned",
                "title": f"⬇️ Rightsize {over_provisioned_count} over-provisioned clusters (based on actual utilization)",
                "severity": "high",
                "category": "cluster",
                "description": f"{over_provisioned_count} clusters show consistently low CPU/memory utilization (P50 CPU <40%, P50 memory <70%), indicating over-provisioning. Combined DBU spend: {over_provisioned_dbus:,.0f} DBUs.",
                "estimated_savings": round(estimated_savings * self.confidence_factor, 2),
                "steps": steps,
                "insight": f"These clusters spend <5% of time above 80% CPU utilization, meaning they're rarely under load. ~25% cost reduction is achievable by downsizing.",
                "effort": "1-2 hours per cluster to analyze and adjust",
                "evidence": {
                    "clusters_analyzed": summary.get("total_clusters_analyzed", 0),
                    "over_provisioned": over_provisioned_count,
                    "dbus_affected": round(over_provisioned_dbus, 2),
                },
            })
        
        # Under-provisioned clusters - performance risk
        if under_provisioned_count > 0:
            under_clusters = summary.get("under_provisioned_clusters", [])[:5]
            under_clusters_info = []
            for c in under_clusters:
                name = c.get("cluster_name", c.get("cluster_id", "unknown"))
                action = c.get("suggested_action", "")
                under_clusters_info.append(f"  - {name}: {action}")
            
            steps = [
                "Review CPU/memory pressure metrics in the report",
                "For clusters with P90 CPU >85% OR P95 memory >95%:",
            ]
            if under_clusters_info:
                steps.append("Priority clusters to upsize:")
                steps.extend(under_clusters_info[:3])
            steps.extend([
                "Increase worker count or use larger instance types",
                "Consider horizontal scaling (more nodes) for CPU pressure",
                "Consider vertical scaling (more memory) for memory pressure",
            ])
            
            recs.append({
                "id": "rightsize_underprovisioned",
                "title": f"⬆️ Scale up {under_provisioned_count} under-provisioned clusters (performance risk)",
                "severity": "medium",
                "category": "cluster",
                "description": f"{under_provisioned_count} clusters show high CPU/memory pressure (P90 CPU >85% or P95 memory >95%), risking performance degradation or OOM failures.",
                "estimated_savings": 0,  # Costs more but improves reliability
                "steps": steps,
                "insight": "Under-provisioned clusters cause slower jobs and potential failures, impacting data freshness and SLAs. Scaling up improves reliability.",
                "effort": "1-2 hours per cluster to analyze and adjust",
            })
        
        # Individual cluster recommendations if there are many details
        for metric in cluster_metrics[:5]:  # Top 5 by DBU spend
            if metric.get("overall_status") == "over-provisioned" and metric.get("component") == "worker":
                cluster_name = metric.get("cluster_name", metric.get("cluster_id"))
                cpu_p50 = metric.get("cpu_p50", 0)
                mem_p50 = metric.get("mem_p50", 0)
                cpu_headroom = metric.get("cpu_headroom_p50", 0)
                mem_headroom = metric.get("mem_headroom_p95", 0)
                dbus = metric.get("total_dbus", 0)
                
                if dbus > 50 and cpu_headroom > 0.5:  # >50% CPU headroom
                    recs.append({
                        "id": f"rightsize_{metric.get('cluster_id', 'unknown')[:8]}",
                        "title": f"Downsize cluster '{cluster_name}' ({cpu_headroom:.0%} CPU headroom)",
                        "severity": "medium",
                        "category": "cluster",
                        "description": f"Cluster '{cluster_name}' uses only {cpu_p50:.0%} median CPU and {mem_p50:.0%} median memory. With {dbus:,.0f} DBUs consumed, downsizing could save ~{dbus * 0.25 * avg_dbu_price:.2f}.",
                        "estimated_savings": round(dbus * 0.25 * avg_dbu_price * self.confidence_factor, 2),
                        "steps": [
                            f"Current performance: P50 CPU {cpu_p50:.0%}, P50 memory {mem_p50:.0%}",
                            metric.get("suggested_action", "Consider downsizing"),
                            "Start by reducing worker count by 25-30%",
                            "Monitor P90 CPU to ensure it stays below 85%",
                        ],
                        "insight": f"This cluster has significant headroom - {cpu_headroom:.0%} CPU and {mem_headroom:.0%} memory unused at P50/P95 respectively.",
                        "effort": "30 minutes to implement, 1 week to validate",
                    })
        
        return recs
    
    def _idle_clusters(self, utilization_data: Dict, cost_analysis: Dict) -> List[Dict]:
        """Identify clusters that are running but essentially idle."""
        recs = []
        
        if not utilization_data.get("available"):
            return recs
        
        idle_clusters = utilization_data.get("idle_clusters", [])
        if not idle_clusters:
            return recs
        
        # Get average DBU price
        total_cost = cost_analysis.get("total_cost", 0)
        total_dbus = cost_analysis.get("total_dbus", 0)
        avg_dbu_price = (total_cost / total_dbus) if total_dbus > 0 else 0.50
        
        total_wasted_dbus = sum(c.get("wasted_dbus_estimate", 0) for c in idle_clusters)
        total_wasted_cost = total_wasted_dbus * avg_dbu_price
        
        if total_wasted_cost > 5:  # Only if meaningful savings
            top_idle = idle_clusters[:5]
            cluster_names = [f"  - {c.get('cluster_name', 'unknown')}: {c.get('avg_cpu_percent', 0):.1f}% avg CPU, {c.get('pct_time_idle', 0):.0f}% time idle" for c in top_idle]
            
            recs.append({
                "id": "idle_clusters",
                "title": f"🔴 Terminate {len(idle_clusters)} idle clusters (~${total_wasted_cost:.2f} wasted)",
                "severity": "high",
                "category": "cluster",
                "description": f"Found {len(idle_clusters)} clusters that spend >50% of their runtime essentially idle (<5% CPU). These are likely forgotten development clusters or misconfigured jobs.",
                "estimated_savings": round(total_wasted_cost * self.confidence_factor, 2),
                "steps": [
                    "Review these clusters immediately:",
                ] + cluster_names[:3] + [
                    "Terminate unused clusters",
                    "Set aggressive auto-terminate (15-30 min) on interactive clusters",
                    "Consider scheduled shutdown for development workspaces",
                ],
                "insight": "Idle clusters are the most obvious waste - you're paying for compute that's doing nothing. Even 'development' clusters should terminate when not in use.",
                "effort": "15 minutes to review and terminate",
            })
        
        return recs
    
    def _autoscale_issues(self, utilization_data: Dict, cost_analysis: Dict) -> List[Dict]:
        """Identify autoscaling clusters that aren't scaling effectively."""
        recs = []
        
        if not utilization_data.get("available"):
            return recs
        
        autoscale = utilization_data.get("autoscale_analysis", {})
        never_down = autoscale.get("never_scales_down", [])
        never_up = autoscale.get("never_scales_up", [])
        
        # Get average DBU price
        total_cost = cost_analysis.get("total_cost", 0)
        total_dbus = cost_analysis.get("total_dbus", 0)
        avg_dbu_price = (total_cost / total_dbus) if total_dbus > 0 else 0.50
        
        # Never scales down - wasting money
        if never_down:
            wasted_dbus = sum(c.get("wasted_dbus_estimate", 0) for c in never_down)
            wasted_cost = wasted_dbus * avg_dbu_price
            
            if wasted_cost > 5:
                cluster_examples = [f"  - {c.get('cluster_name')}: always at {c.get('avg_workers'):.0f}/{c.get('autoscale_max')} workers" for c in never_down[:3]]
                
                recs.append({
                    "id": "autoscale_never_down",
                    "title": f"⚙️ Fix {len(never_down)} autoscaling clusters that never scale down",
                    "severity": "medium",
                    "category": "cluster",
                    "description": f"These clusters have autoscaling configured but always run at or near max capacity. Either workload is constant (use fixed size) or autoscaling isn't triggering properly.",
                    "estimated_savings": round(wasted_cost * self.confidence_factor, 2),
                    "steps": [
                        "Clusters that never scale down:",
                    ] + cluster_examples + [
                        "Option 1: If workload is truly constant, switch to fixed-size cluster",
                        "Option 2: Review autoscaling triggers - may need tuning",
                        "Option 3: Check if cluster is oversized at min_workers",
                    ],
                    "insight": "Autoscaling that never scales down gives you the worst of both worlds: autoscaling overhead without the cost savings.",
                    "effort": "30 minutes per cluster to analyze and adjust",
                })
        
        # Never scales up - may indicate over-provisioning at min
        if len(never_up) >= 2:
            recs.append({
                "id": "autoscale_never_up",
                "title": f"⚙️ Review {len(never_up)} autoscaling clusters that never scale up",
                "severity": "low",
                "category": "cluster",
                "description": f"These clusters have autoscaling configured but workload never triggers scale-up. Either min_workers is already sufficient or autoscaling triggers are misconfigured.",
                "estimated_savings": 0,  # Not direct savings, but simplification
                "steps": [
                    "Consider these options:",
                    "If workload fits in min_workers: reduce max_workers (simpler config)",
                    "If jobs are slower than expected: check autoscaling triggers",
                    "If this is intentional buffer: document and ignore",
                ],
                "insight": "Unused autoscaling capacity isn't directly wasteful, but it can indicate over-provisioned min_workers or misconfigured triggers.",
                "effort": "15 minutes per cluster to review",
            })
        
        return recs
    
    def _driver_worker_imbalance(self, utilization_data: Dict) -> List[Dict]:
        """Identify clusters with driver/worker resource imbalance."""
        recs = []
        
        if not utilization_data.get("available"):
            return recs
        
        imbalanced = utilization_data.get("driver_imbalance", [])
        
        # Group by issue type
        driver_cpu_bottleneck = [c for c in imbalanced if c.get("issue") == "driver_cpu_bottleneck"]
        driver_mem_bottleneck = [c for c in imbalanced if c.get("issue") == "driver_memory_bottleneck"]
        
        if driver_cpu_bottleneck:
            total_dbus = sum(c.get("total_dbus", 0) for c in driver_cpu_bottleneck)
            examples = [f"  - {c.get('cluster_name')}: driver {c.get('driver_cpu_p90'):.0%} CPU vs workers {c.get('worker_cpu_p90'):.0%}" for c in driver_cpu_bottleneck[:3]]
            
            recs.append({
                "id": "driver_cpu_bottleneck",
                "title": f"🔧 Fix driver CPU bottleneck in {len(driver_cpu_bottleneck)} clusters",
                "severity": "medium",
                "category": "cluster",
                "description": f"These clusters have high driver CPU while workers are underutilized. This usually indicates code that's doing too much work on the driver (collect(), pandas operations, etc.).",
                "estimated_savings": round(total_dbus * 0.15 * 0.50, 2),  # 15% efficiency gain possible
                "steps": [
                    "Affected clusters:",
                ] + examples + [
                    "Common causes:",
                    "1. Large collect() operations pulling data to driver",
                    "2. Using toPandas() on large datasets",
                    "3. Complex UDFs that serialize through driver",
                    "Fix: Rewrite to keep processing distributed on workers",
                ],
                "insight": "When driver is maxed but workers are idle, you're paying for unused worker capacity. The bottleneck limits throughput regardless of worker count.",
                "effort": "2-4 hours to refactor code patterns",
            })
        
        if driver_mem_bottleneck:
            examples = [f"  - {c.get('cluster_name')}: driver {c.get('driver_mem_p95'):.0%} memory vs workers {c.get('worker_mem_p95'):.0%}" for c in driver_mem_bottleneck[:3]]
            
            recs.append({
                "id": "driver_memory_bottleneck",
                "title": f"🔧 Fix driver memory pressure in {len(driver_mem_bottleneck)} clusters",
                "severity": "high",  # Memory pressure can cause OOM
                "category": "cluster",
                "description": f"These clusters have high driver memory while workers have headroom. This risks OOM errors and indicates data is being collected to driver unnecessarily.",
                "estimated_savings": 0,  # Reliability improvement, not cost
                "steps": [
                    "Affected clusters:",
                ] + examples + [
                    "Common causes:",
                    "1. collect() on large datasets",
                    "2. Broadcast joins with large tables",
                    "3. Caching datasets on driver",
                    "Fix: Use distributed writes (saveAsTable, write.parquet) instead of collecting",
                ],
                "insight": "Driver OOM is a common cause of job failures. Fixing this improves reliability and may allow using a smaller (cheaper) driver node.",
                "effort": "1-3 hours to identify and fix collect patterns",
            })
        
        return recs
    
    def _tagging_governance(self, usage_data: Dict) -> List[Dict]:
        """Recommend proper tagging for cost attribution."""
        recs = []
        
        tagging = usage_data.get("tagging_analysis", {})
        untagged_pct = tagging.get("untagged_percentage", 0)
        untagged_dbus = tagging.get("untagged_dbus", 0)
        
        if tagging.get("has_tagging_gap") and untagged_pct > 20:
            # Estimate cost impact (rough: $0.50/DBU average)
            untagged_cost_estimate = untagged_dbus * 0.50
            
            recs.append({
                "id": "implement_tagging",
                "title": f"🏷️ Implement resource tagging ({untagged_pct:.0f}% of spend is unattributed)",
                "severity": "high" if untagged_pct > 50 else "medium",
                "category": "governance",
                "description": f"{untagged_pct:.0f}% of DBU usage has no custom tags, making it impossible to attribute costs to teams or projects. This hides waste.",
                "estimated_savings": round(untagged_cost_estimate * 0.1 * self.confidence_factor, 2),
                "steps": [
                    "Define mandatory tags: team, project, environment (dev/prod)",
                    "Update cluster policies to require tags on creation",
                    "Add tags to existing clusters and jobs",
                    "Set up cost allocation reports by tag",
                ],
                "insight": "Without tags, you can't answer 'which team is spending the most?' or 'what project is this cluster for?'. Tagging enables accountability.",
                "effort": "1-2 hours to implement policy, ongoing enforcement",
            })
        
        return recs
    
    def _weekend_waste(self, usage_data: Dict, cost_analysis: Dict) -> List[Dict]:
        """Identify forgotten resources running on weekends."""
        recs = []
        
        patterns = usage_data.get("usage_patterns", {})
        weekend_dbus = patterns.get("weekend_dbus", 0)
        weekend_ratio = patterns.get("weekend_to_weekday_ratio", 0)
        total_cost = cost_analysis.get("total_cost", 0)
        
        if patterns.get("has_weekend_waste") and weekend_dbus > 10:
            # Estimate weekend waste cost
            weekend_pct = patterns.get("weekend_percentage", 0)
            weekend_cost = total_cost * (weekend_pct / 100)
            
            # If weekend usage is >15% of weekday average, most is likely waste
            # Assume 70% of weekend usage is "forgotten" resources
            wasted_cost = weekend_cost * 0.7
            
            recs.append({
                "id": "weekend_waste",
                "title": f"📅 Reduce weekend/off-hours usage ({weekend_ratio:.0%} of weekday activity)",
                "severity": "high" if wasted_cost > 50 else "medium",
                "category": "governance",
                "description": f"Significant compute runs on weekends ({weekend_pct:.1f}% of total). Unless you have scheduled batch jobs, this is likely forgotten notebooks or dev clusters.",
                "estimated_savings": round(wasted_cost * self.confidence_factor, 2),
                "steps": [
                    "Audit: Check which clusters/notebooks ran last weekend",
                    "Set auto-termination to 30 min or less on all interactive clusters",
                    "For SQL warehouses: set auto-stop to 10 minutes",
                    "Consider scheduled shutdown policies for non-production workspaces",
                ],
                "insight": f"If your team doesn't work weekends, weekend Databricks spend is usually waste. At {weekend_ratio:.0%} of weekday levels, ~${wasted_cost:.2f} may be recoverable.",
                "effort": "30 minutes to audit, ongoing discipline",
            })
        
        return recs
    
    def _delta_optimization(self, sql_analysis: Dict, cost_analysis: Dict) -> List[Dict]:
        """Recommend Delta table optimizations based on query patterns."""
        recs = []
        
        query_count = sql_analysis.get("query_count", 0)
        pattern_summary = sql_analysis.get("pattern_summary", {})
        large_scans = pattern_summary.get("large_result_sets", 0)
        sql_cost = cost_analysis.get("cost_by_product", {}).get("SQL", {}).get("cost", 0)
        
        # If significant SQL spend and large data scans
        if sql_cost > 10 and (large_scans > 5 or query_count > 100):
            recs.append({
                "id": "delta_optimization",
                "title": "Consider Delta table optimization (Z-ordering, Liquid Clustering)",
                "severity": "low",
                "category": "data",
                "description": f"With ${sql_cost:.2f} SQL spend and {query_count} queries, table optimization can significantly reduce scan costs.",
                "estimated_savings": round(sql_cost * 0.15 * self.confidence_factor, 2),
                "steps": [
                    "Identify most-queried tables from Query History",
                    "For existing tables: OPTIMIZE table ZORDER BY (common_filter_column)",
                    "For new tables: Use Liquid Clustering (auto-manages data layout)",
                    "Enable Predictive Optimization in SQL Warehouses for auto-OPTIMIZE",
                ],
                "insight": "Z-ordering co-locates related data, reducing scan size. A well-optimized table can run 10x faster on the same queries.",
                "effort": "15 min per table to analyze and optimize",
            })
        
        return recs

    # ================== ALWAYS INCLUDE: GOVERNANCE ==================
    
    def _cost_monitoring(self, cost_analysis: Dict) -> List[Dict]:
        """Recommend basic cost monitoring."""
        recs = []
        
        monthly_cost = cost_analysis.get("estimated_monthly_cost", 0)
        
        recs.append({
            "id": "cost_monitoring",
            "title": "Set up cost alerts to catch runaway spend",
            "severity": "low",
            "category": "governance",
            "description": f"At ${monthly_cost:.2f}/month projected, set up alerts to catch unexpected spikes before they become big bills.",
            "estimated_savings": round(monthly_cost * 0.05 * self.confidence_factor, 2),
            "steps": [
                "Account Settings → Budgets → Create Budget",
                "Set monthly budget with alerts at 50%, 80%, 100%",
                "Share cost dashboards with team leads",
                "Review billing weekly until patterns are understood",
            ],
            "insight": "Most Databricks bill surprises come from: forgotten clusters, oversized dev resources, or runaway jobs. Alerts catch these early.",
            "effort": "10 minutes",
        })
        
        return recs
