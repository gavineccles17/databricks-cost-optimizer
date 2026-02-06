"""Configuration management utilities."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml

def load_config() -> Dict[str, Any]:
    """
    Load configuration from YAML files and environment variables.
    
    Returns:
        Configuration dictionary
    """
    config_dir = Path(__file__).parent.parent.parent / "config"
    
    # Load default configuration
    default_config_path = config_dir / "default.yaml"
    if not default_config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {default_config_path}")
    
    with open(default_config_path) as f:
        config = yaml.safe_load(f) or {}
    
    # Override with environment variables
    if os.getenv("START_DATE"):
        config["date_range"]["start_date"] = os.getenv("START_DATE")
    if os.getenv("END_DATE"):
        config["date_range"]["end_date"] = os.getenv("END_DATE")
    
    # Set output directory
    config["output_dir"] = os.getenv("OUTPUT_DIR", "/output")
    
    # Override mock mode
    if os.getenv("MOCK_MODE"):
        config["mock"]["enabled"] = os.getenv("MOCK_MODE").lower() == "true"
    
    return config
