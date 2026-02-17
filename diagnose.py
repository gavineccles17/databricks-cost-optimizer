#!/usr/bin/env python3
"""Diagnostic script to check available data in system tables."""

import logging
import os
from dotenv import load_dotenv
from src.databricks_client import DatabricksClient

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def diagnose():
    """Run diagnostic queries to check table structure and data."""
    
    logger.info("=" * 80)
    logger.info("DATABRICKS SYSTEM TABLES DIAGNOSTIC")
    logger.info("=" * 80)
    
    try:
        # Initialize Databricks client
        client = DatabricksClient(mock_mode=False)
        client.verify_connection()
        logger.info("✓ Connected to Databricks SQL warehouse")
    except Exception as e:
        logger.error(f"✗ Failed to connect to Databricks: {str(e)}")
        return
    
    # List of queries to run
    diagnostics = [
        ("system.billing.usage row count", 
         "SELECT COUNT(*) as row_count FROM system.billing.usage"),
        
        ("system.billing.usage date range",
         "SELECT MIN(usage_date) as min_date, MAX(usage_date) as max_date, COUNT(*) as total_rows FROM system.billing.usage"),
        
        ("system.billing.usage sample (first 5)",
         "SELECT * FROM system.billing.usage LIMIT 5"),
        
        ("system.compute.clusters row count",
         "SELECT COUNT(*) as row_count FROM system.compute.clusters"),
        
        ("system.compute.clusters sample",
         "SELECT * FROM system.compute.clusters LIMIT 5"),
        
        ("system.compute.node_timeline row count",
         "SELECT COUNT(*) as row_count FROM system.compute.node_timeline"),
        
        ("system.compute.node_timeline sample",
         "SELECT * FROM system.compute.node_timeline LIMIT 5"),
        
        ("system.query.history row count",
         "SELECT COUNT(*) as row_count FROM system.query.history"),
        
        ("system.query.history sample",
         "SELECT * FROM system.query.history LIMIT 5"),
        
        ("system.billing.usage - sku_name values",
         "SELECT DISTINCT sku_name FROM system.billing.usage LIMIT 20"),
        
        ("system.billing.usage - usage_type values",
         "SELECT DISTINCT usage_type FROM system.billing.usage LIMIT 20"),
    ]
    
    for test_name, query in diagnostics:
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"Test: {test_name}")
            logger.info(f"{'='*80}")
            results = client.execute_query(query)
            
            if results:
                logger.info(f"✓ Query successful - {len(results)} rows returned")
                for i, row in enumerate(results[:3]):  # Show first 3 rows
                    logger.info(f"  Row {i+1}: {row}")
                if len(results) > 3:
                    logger.info(f"  ... {len(results) - 3} more rows")
            else:
                logger.info("✗ Query returned 0 rows")
        except Exception as e:
            logger.error(f"✗ Query failed: {str(e)}")
    
    logger.info(f"\n{'='*80}")
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info(f"{'='*80}")
    
    client.close()

if __name__ == "__main__":
    diagnose()
