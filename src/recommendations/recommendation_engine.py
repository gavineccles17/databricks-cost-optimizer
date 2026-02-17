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
        recommendations.extend(self._all_purpose_to_jobs(cost_by_product, total_cost))
        recommendations.extend(self._photon_evaluation(cost_by_product, cost_analysis))
        recommendations.extend(self._serverless_opportunities(cost_by_product, job_analysis))
        recommendations.extend(self._cluster_sizing(cluster_analysis))
        recommendations.extend(self._model_serving_scale_to_zero(cost_by_product))
        
        # MEDIUM PRIORITY: Operational improvements
        recommendations.extend(self._spot_instances(cluster_analysis, job_analysis))
        recommendations.extend(self._cluster_pools(cluster_analysis))
        recommendations.extend(self._job_frequency(job_analysis))
        
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
    
    def _cluster_pools(self, cluster_analysis: Dict) -> List[Dict]:
        """Recommend cluster pools for faster startup."""
        recs = []
        
        cluster_costs = cluster_analysis.get("cluster_costs", [])
        
        if len(cluster_costs) >= 2:
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
