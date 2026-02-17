"""Collects Databricks SQL query history and patterns."""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from src.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)


class QueryCollector:
    """Collects SQL query history, patterns, and user attribution."""
    
    def __init__(self, client: DatabricksClient, config: Dict[str, Any]):
        """Initialize query collector."""
        self.client = client
        self.config = config
    
    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Collect query history with user attribution for the specified period.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            Dictionary containing query data and analysis
        """
        logger.info("Collecting query history")
        
        # Get aggregated query stats by user
        user_stats = self._collect_user_query_stats(start_date, end_date)
        
        # Get expensive queries
        expensive_queries = self._collect_expensive_queries(start_date, end_date)
        
        # Get query patterns (for optimization recommendations)
        query_patterns = self._collect_query_patterns(start_date, end_date)
        
        return {
            "user_stats": user_stats,
            "expensive_queries": expensive_queries,
            "query_patterns": query_patterns,
            "query_count": sum(u.get("query_count", 0) for u in user_stats),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        }
    
    def _collect_user_query_stats(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get query statistics aggregated by user."""
        try:
            query = f"""
            SELECT
                executed_by as user,
                COUNT(*) as query_count,
                SUM(read_rows) as total_rows_read,
                SUM(read_files) as total_files_read,
                SUM(total_task_duration_ms) / 1000 as total_duration_seconds,
                AVG(total_task_duration_ms) / 1000 as avg_duration_seconds
            FROM system.query.history
            WHERE start_time >= '{start_date.isoformat()}'
                AND start_time <= '{end_date.isoformat()}'
                AND executed_by IS NOT NULL
            GROUP BY executed_by
            ORDER BY total_duration_seconds DESC
            LIMIT 50
            """
            stats = self.client.execute_query(query)
            logger.info(f"User query stats returned {len(stats)} users")
            return stats
        except Exception as e:
            logger.warning(f"Could not fetch user query stats: {str(e)}")
            return []
    
    def _collect_expensive_queries(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get the longest running queries."""
        try:
            query = f"""
            SELECT
                statement_id,
                executed_by as user,
                compute.warehouse_id as warehouse_id,
                statement_type,
                read_rows,
                read_files,
                total_task_duration_ms / 1000 as duration_seconds,
                start_time
            FROM system.query.history
            WHERE start_time >= '{start_date.isoformat()}'
                AND start_time <= '{end_date.isoformat()}'
                AND total_task_duration_ms > 5000
                AND statement_type IN ('SELECT', 'INSERT', 'MERGE', 'UPDATE', 'DELETE', 'CREATE', 'COPY')
            ORDER BY total_task_duration_ms DESC
            LIMIT 10
            """
            queries = self.client.execute_query(query)
            logger.info(f"Expensive queries returned {len(queries)} queries")
            return queries
        except Exception as e:
            logger.warning(f"Could not fetch expensive queries: {str(e)}")
            return []
    
    def _collect_query_patterns(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Identify problematic query patterns like SELECT *, full table scans.
        Returns aggregated pattern counts, not individual queries.
        """
        try:
            # Get a sample of queries to analyze patterns
            query = f"""
            SELECT
                statement_type,
                statement_text,
                read_rows,
                total_task_duration_ms
            FROM system.query.history
            WHERE start_time >= '{start_date.isoformat()}'
                AND start_time <= '{end_date.isoformat()}'
                AND statement_type = 'SELECT'
            ORDER BY total_task_duration_ms DESC
            LIMIT 500
            """
            queries = self.client.execute_query(query)
            
            patterns = {
                "select_star": 0,
                "no_where_clause": 0,
                "excessive_joins": 0,
                "large_result_sets": 0,
            }
            
            for q in queries:
                stmt = (q.get("statement_text") or "").upper()
                rows = q.get("read_rows") or 0
                
                # Detect SELECT *
                if re.search(r'SELECT\s+\*', stmt):
                    patterns["select_star"] += 1
                
                # Detect missing WHERE clause (simplified check)
                if "WHERE" not in stmt and "JOIN" not in stmt:
                    patterns["no_where_clause"] += 1
                
                # Detect many JOINs
                join_count = len(re.findall(r'\bJOIN\b', stmt))
                if join_count >= 5:
                    patterns["excessive_joins"] += 1
                
                # Detect large result sets
                if rows and rows > 10000000:  # 10M+ rows
                    patterns["large_result_sets"] += 1
            
            logger.info(f"Query patterns detected: {patterns}")
            return [{"pattern": k, "count": v} for k, v in patterns.items() if v > 0]
            
        except Exception as e:
            logger.warning(f"Could not analyze query patterns: {str(e)}")
            return []
