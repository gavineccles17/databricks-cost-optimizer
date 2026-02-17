"""Analyzes SQL query patterns and efficiency."""

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class SqlAnalyzer:
    """Identifies inefficient SQL patterns."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize SQL analyzer."""
        self.config = config
        self.excessive_joins_threshold = config.get("thresholds", {}).get("excessive_joins_threshold", 5)
    
    def analyze(self, queries_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze SQL query patterns for inefficiencies.
        
        Args:
            queries_data: Data from query collector
        
        Returns:
            SQL analysis results
        """
        logger.info("Analyzing SQL efficiency...")
        
        # Get query patterns detected by collector
        query_patterns = queries_data.get("query_patterns", [])
        expensive_queries = queries_data.get("expensive_queries", [])
        user_stats = queries_data.get("user_stats", [])
        
        # Build inefficient patterns list from collector's pattern detection
        inefficient_patterns = []
        pattern_counts = {}
        
        for pattern in query_patterns:
            pattern_type = pattern.get("pattern")
            count = pattern.get("count", 0)
            if count > 0:
                pattern_counts[pattern_type] = count
                # Add as an inefficient pattern finding
                if pattern_type == "select_star":
                    inefficient_patterns.append({
                        "type": "select_star",
                        "severity": "medium",
                        "count": count,
                        "description": f"{count} queries use SELECT * - specify columns to reduce data transfer",
                    })
                elif pattern_type == "no_where_clause":
                    inefficient_patterns.append({
                        "type": "no_where_clause",
                        "severity": "high",
                        "count": count,
                        "description": f"{count} queries lack WHERE clauses - add filters to reduce data scanned",
                    })
                elif pattern_type == "excessive_joins":
                    inefficient_patterns.append({
                        "type": "excessive_joins",
                        "severity": "medium",
                        "count": count,
                        "description": f"{count} queries have 5+ JOINs - consider denormalization",
                    })
                elif pattern_type == "large_result_sets":
                    inefficient_patterns.append({
                        "type": "large_result_sets",
                        "severity": "medium",
                        "count": count,
                        "description": f"{count} queries return 10M+ rows - add LIMIT or aggregate",
                    })
        
        # Calculate total queries analyzed from user stats
        total_queries = sum(u.get("query_count", 0) for u in user_stats)
        
        # Count total pattern instances
        total_pattern_count = sum(pattern_counts.values())
        
        logger.info(f"Analyzed {total_queries} queries, found {total_pattern_count} inefficient patterns")
        
        return {
            "query_count": total_queries,
            "inefficient_patterns": inefficient_patterns,
            "pattern_count": total_pattern_count,
            "pattern_summary": pattern_counts,
            "expensive_query_count": len(expensive_queries),
        }
