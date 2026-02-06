FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r optimizer && useradd -r -g optimizer optimizer

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ src/
COPY config/ config/

# Create output directory with proper permissions
RUN mkdir -p /output && chown -R optimizer:optimizer /output /app

# Switch to non-root user
USER optimizer

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "-m", "src.main"]
