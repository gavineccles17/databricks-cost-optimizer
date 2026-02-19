"""
Main entry point for Databricks Cost Optimizer.
Orchestrates data collection, analysis, and reporting.
"""

import logging
import os 
from datetime import datetime, timedelta
from pathlib import Path

import typer
from dotenv import load_dotenv

from src.databricks_client import DatabricksClient
from src.collectors import (
    UsageCollector,
    ClusterCollector,
    JobCollector,
    QueryCollector,
    WarehouseCollector,
    ClusterUtilizationCollector,
)
from src.analyzers import (
    CostAnalyzer,
    ClusterAnalyzer,
    JobAnalyzer,
    SqlAnalyzer,
)
from src.recommendations import RecommendationEngine
from src.reporting import MarkdownReport, JsonReport
from src.utils import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress noisy Databricks SQL connector logs
logging.getLogger("databricks.sql").setLevel(logging.WARNING)

app = typer.Typer()


@app.command()
def main(
    mock: bool = typer.Option(False, "--mock", help="Run in mock mode without Databricks"),
):
    """
    Generate a Databricks Cost & Performance Optimization Report.
    
    Connects to Databricks, extracts usage and cost data, performs analysis,
    generates recommendations, and produces client-ready reports.
    """
    
    # Load environment variables
    load_dotenv()
    
    # Check if MOCK_MODE is set in environment
    mock_env = os.getenv("MOCK_MODE", "").lower()
    if mock_env in ("true", "1", "yes"):
        mock = True
    elif mock_env in ("false", "0", "no"):
        mock = False
    # Otherwise use the command-line flag value
    
    # Load configuration
    config = load_config()
    logger.info("Configuration loaded successfully")
    
    try:
        # Initialize Databricks client
        if mock:
            logger.info("Running in MOCK MODE - using synthetic data")
            db_client = DatabricksClient(mock_mode=True)
        else:
            db_client = DatabricksClient(mock_mode=False)
            db_client.verify_connection()
            logger.info("Connected to Databricks workspace")
        
        # Determine date range
        start_date = datetime.fromisoformat(config["date_range"]["start_date"])
        end_date = datetime.fromisoformat(config["date_range"]["end_date"])
        
        logger.info(f"Analyzing period: {start_date.date()} to {end_date.date()}")
        
        # ============ COLLECTORS ============
        logger.info("Collecting usage data...")
        usage_collector = UsageCollector(db_client, config)
        usage_data = usage_collector.collect(start_date, end_date)
        
        logger.info("Collecting cluster data...")
        cluster_collector = ClusterCollector(db_client, config)
        clusters_data = cluster_collector.collect(start_date, end_date)
        
        logger.info("Collecting warehouse data...")
        warehouse_collector = WarehouseCollector(db_client, config)
        warehouses_data = warehouse_collector.collect(start_date, end_date)
        
        logger.info("Collecting job data...")
        job_collector = JobCollector(db_client, config)
        jobs_data = job_collector.collect(start_date, end_date)
        
        logger.info("Collecting query data...")
        query_collector = QueryCollector(db_client, config)
        queries_data = query_collector.collect(start_date, end_date)
        
        logger.info("Collecting cluster utilization metrics...")
        utilization_collector = ClusterUtilizationCollector(db_client, config)
        utilization_data = utilization_collector.collect(days=(end_date - start_date).days)
        
        # ============ ANALYZERS ============
        logger.info("Performing cost analysis...")
        cost_analyzer = CostAnalyzer(config)
        cost_analysis = cost_analyzer.analyze(usage_data, clusters_data, jobs_data, warehouses_data)
        
        logger.info("Analyzing cluster efficiency...")
        cluster_analyzer = ClusterAnalyzer(config)
        cluster_analysis = cluster_analyzer.analyze(clusters_data, usage_data)
        
        logger.info("Analyzing job patterns...")
        job_analyzer = JobAnalyzer(config)
        job_analysis = job_analyzer.analyze(jobs_data, usage_data)
        
        logger.info("Analyzing SQL efficiency...")
        sql_analyzer = SqlAnalyzer(config)
        sql_analysis = sql_analyzer.analyze(queries_data)
        
        # ============ RECOMMENDATION ENGINE ============
        logger.info("Generating recommendations...")
        workspace_url = db_client.get_workspace_url()
        rec_engine = RecommendationEngine(config, workspace_url=workspace_url)
        recommendations = rec_engine.generate(
            cost_analysis, cluster_analysis, job_analysis, sql_analysis, 
            warehouses_data, usage_data, utilization_data, queries_data
        )
        
        # ============ REPORTING ============
        output_dir = Path(config["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Generating Markdown report...")
        md_report = MarkdownReport(config, workspace_url=workspace_url)
        md_path = md_report.generate(
            output_dir,
            cost_analysis,
            cluster_analysis,
            job_analysis,
            sql_analysis,
            recommendations,
            warehouses_data,
            queries_data,
            usage_data,
            utilization_data,
        )
        logger.info(f"Markdown report generated: {md_path}")
        
        logger.info("Generating JSON report...")
        json_report = JsonReport(config)
        json_path = json_report.generate(
            output_dir,
            cost_analysis,
            cluster_analysis,
            job_analysis,
            sql_analysis,
            recommendations,
        )
        logger.info(f"JSON report generated: {json_path}")
        
        logger.info("✓ Analysis complete. Reports generated successfully.")
        
    except Exception as e:
        logger.error(f"✗ Error during analysis: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    app()
