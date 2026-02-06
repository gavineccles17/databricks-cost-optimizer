# Dev container - reuses the main image
FROM databricks-cost-optimizer:latest

# Add dev-specific tools
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install dev Python packages
RUN pip install --no-cache-dir \
    black==23.12.0 \
    ruff==0.1.8 \
    mypy==1.7.0 \
    ipython==8.18.1

USER optimizer
