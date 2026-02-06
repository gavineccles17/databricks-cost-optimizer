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
        
        queries = queries_data.get("queries", [])
        inefficient_patterns = []
        
        for query in queries:
            query_text = query.get("query_text", "").upper()
            query_id = query.get("query_id")
            
            issues = self._detect_patterns(query_text, query_id)
            inefficient_patterns.extend(issues)
        
        return {
            "query_count": len(queries),
            "inefficient_patterns": inefficient_patterns,
            "pattern_count": len(inefficient_patterns),
        }
    
    def _detect_patterns(self, query: str, query_id: str) -> List[Dict[str, Any]]:
        """
        Detect inefficient SQL patterns.
        
        Args:
            query: SQL query text
            query_id: Query identifier
        
        Returns:
            List of detected issues
        """
        issues = []
        
        # Detect SELECT *
        if re.search(r'SELECT\s+\*', query):
            issues.append({
                "type": "select_star",
                "query_id": query_id,
                "severity": "medium",
                "description": "Query uses SELECT * - consider specifying columns",
            })
        
        # Detect missing WHERE clause
        if re.search(r'FROM\s+[\w.]+\s+(WHERE|GROUP BY|ORDER BY|LIMIT|$)', query) and "WHERE" not in query:
            issues.append({
                "type": "missing_where",
                "query_id": query_id,
                "severity": "high",
                "description": "Query may benefit from WHERE clause filtering",
            })
        
        # Count JOINs
        join_count = len(re.findall(r'\bJOIN\b', query))
        if join_count > self.excessive_joins_threshold:
            issues.append({
                "type": "excessive_joins",
                "query_id": query_id,
                "severity": "medium",
                "description": f"Query has {join_count} JOINs (consider optimizing)",
                "join_count": join_count,
            })
        
        return issues
