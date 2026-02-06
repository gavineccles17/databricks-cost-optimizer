"""Tests for cost calculation logic."""

import pytest
from src.analyzers.cost_analyzer import CostAnalyzer


def test_cost_analyzer_initialization():
    """Test CostAnalyzer initialization."""
    config = {"thresholds": {}}
    analyzer = CostAnalyzer(config)
    assert analyzer is not None


def test_cost_analysis():
    """Test basic cost analysis."""
    config = {"thresholds": {}, "confidence_factor": 0.75}
    analyzer = CostAnalyzer(config)
    
    usage_data = {
        "total_dbus": 1000,
        "by_type": {"COMPUTE": 600, "SQL": 400},
        "period": {"days": 10},
    }
    clusters_data = {"clusters": []}
    jobs_data = {"jobs": []}
    
    result = analyzer.analyze(usage_data, clusters_data, jobs_data)
    
    assert result["period_dbus"] == 1000
    assert result["period_days"] == 10
    assert result["estimated_monthly_dbus"] > 0
    assert result["estimated_monthly_cost"] > 0
