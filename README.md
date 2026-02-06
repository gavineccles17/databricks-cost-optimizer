# Databricks Cost & Performance Optimization Analyzer

A **production-grade, Dockerized Python tool** that connects to your Databricks workspace and generates a comprehensive cost optimization report.

## What This Tool Does

✓ Analyzes your Databricks workspace usage and cost patterns  
✓ Identifies waste, inefficiencies, and optimization opportunities  
✓ Estimates potential monthly savings ($)  
✓ Generates client-ready Markdown + machine-readable JSON reports  
✓ Requires **read-only permissions only** (no data mutation)  
✓ Runs fully inside Docker (no Python installation needed locally)  

## Why Docker?

- **Portability**: Same reproducible execution everywhere (laptop, CI/CD, cloud)
- **Security**: Secrets never baked into image, credentials via `.env`
- **Isolation**: No dependency conflicts with host system
- **Client-Friendly**: Customers run a single command without installing Python

## Why Dev-Containers?

- **Zero Setup**: Open in VS Code, code immediately
- **Same Environment**: Dev container reuses the production Dockerfile
- **Debugging**: Full IDE support for development and testing
- **No Dependency Duplication**: Single source of truth for Dockerfile

## Quick Start

### Prerequisites

- Docker (19.03+)
- Databricks workspace with SQL warehouse
- Databricks personal access token (PAT)

### Production Usage

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-org/databricks-cost-optimizer.git
   cd databricks-cost-optimizer
   ```

2. **Configure credentials**:
   ```bash
   cp .env.example .env
   # Edit .env with your Databricks details
   ```

3. **Build the image**:
   ```bash
   docker build -t databricks-cost-optimizer .
   ```

4. **Run the analysis**:
   ```bash
   docker run --env-file .env -v $(pwd)/output:/output databricks-cost-optimizer
   ```

5. **Check results**:
   ```bash
   ls -la output/
   cat output/optimization_report.md
   ```

### Local Development (VS Code)

1. **Open in Dev Container**:
   - Open the project in VS Code
   - Press `F1` and select "Dev Containers: Reopen in Container"
   - Wait for container to start

2. **Run locally with mock data**:
   ```bash
   docker-compose run --env MOCK_MODE=true databricks-cost-optimizer
   ```

3. **Edit code** and test interactively:
   ```bash
   python -m pytest tests/
   python -m src.main --mock
   ```

## Databricks Permissions Required

This tool requires **read-only access only**. Create a PAT token with these minimal permissions:

```
system.billing.usage → read
system.compute.clusters → read
system.compute.cluster_events → read
system.compute.cluster_snapshots → read
system.jobs.jobs → read
system.jobs.job_runs → read
system.query.history → read
```

No cluster creation, modification, or data access required.

## Configuration

### Environment Variables (`.env`)

```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
START_DATE=2024-01-01
END_DATE=2024-01-31
OUTPUT_DIR=/output
MOCK_MODE=false
```

### Config Files

- `config/default.yaml` - Analysis thresholds, date ranges, reporting options
- `config/pricing.yaml` - DBU pricing by region (defaults: AWS 2024 pricing)

## Output

The tool generates two reports in `/output`:

1. **`optimization_report.md`** - Client-ready Markdown report
   - Executive summary
   - Cost breakdown
   - Top issues and waste areas
   - Actionable recommendations with savings estimates
   - Professional tone, non-blaming

2. **`optimization_report.json`** - Machine-readable JSON
   - All analysis data in structured format
   - For programmatic consumption, dashboards, etc.

## Report Sections

### Executive Summary
- Estimated monthly spend
- Potential monthly savings
- Number of optimization opportunities

### Cost Breakdown
- Total DBUs consumed
- Cost by DBU type (Compute, SQL, Jobs)
- Breakdown by cluster, job, user

### Cluster Analysis
- Over-provisioned clusters (high worker count)
- Always-on clusters (no auto-termination)
- High idle time patterns
- Missing autoscaling configuration

### Job Analysis
- Long-running jobs
- High retry rates
- Interactive cluster usage (inefficient)
- Overlapping schedules

### SQL Query Efficiency
- SELECT * usage (specifies columns instead)
- Missing WHERE clauses (full table scans)
- Excessive JOINs
- Missing partition filters
- Repeated table scans

### Recommendations
Each with:
- Severity (Low / Medium / High)
- Estimated monthly savings ($)
- Technical explanation
- Step-by-step remediation

## Security & Compliance

✓ **Read-Only**: Tool never modifies clusters, jobs, or data  
✓ **Credentials**: Passed via environment variables, never logged  
✓ **No Secrets in Image**: Dockerfile doesn't embed any credentials  
✓ **Local Execution**: Analysis runs inside Docker, no data sent externally  
✓ **Network**: Communicates only with customer's Databricks workspace  

## Architecture

```
┌─────────────────────────────────────────┐
│  Docker Container / Dev Container       │
├─────────────────────────────────────────┤
│                                         │
│  src/main.py (orchestrator)             │
│    ├─ collectors/ (extract metadata)    │
│    ├─ analyzers/ (identify patterns)    │
│    ├─ recommendations/ (generate ideas) │
│    └─ reporting/ (format output)        │
│                                         │
│  ↓ (system.billing.usage,             │
│     system.compute.*, ...)             │
│                                         │
└─────────────────────────────────────────┘
           ↓
    Databricks Workspace
    (read-only SQL queries)
```

## Example Analysis Output

```
# Databricks Cost & Performance Optimization Report

*Generated: 2024-01-13 14:23:45*

## Executive Summary

- **Estimated Monthly Spend**: $12,450.00
- **Potential Monthly Savings**: $2,890.00
- **Optimization Opportunities**: 8

## Recommendations

### 1. Enable auto-termination on prod-cluster-1
**Severity**: HIGH
**Estimated Monthly Savings**: $850.00

This cluster has no auto-termination configured and runs 24/7 even during idle periods.

**Action Steps**:
1. Open cluster configuration in Databricks UI
2. Enable auto-termination (recommend 60 minutes)
3. Verify setting is applied

### 2. Optimize inefficient SQL queries
**Severity**: MEDIUM
**Estimated Monthly Savings**: $650.00

Found 23 queries with inefficient patterns (SELECT *, missing WHERE, etc).

**Action Steps**:
1. Review detected query patterns in detailed report
2. Add WHERE clauses to full table scans
3. Replace SELECT * with specific columns
...
```

## Troubleshooting

### "Failed to connect to Databricks"
- Check `DATABRICKS_HOST` format (should include `https://`)
- Verify token is valid (hasn't expired)
- Ensure SQL warehouse is running

### "Permission denied" on system tables
- Verify PAT token has read access to system schema
- Contact Databricks account team for workspace permissions

### Docker build fails
- Ensure Python 3.11+ is available
- Check internet connection for dependency download
- Run `docker build --no-cache` to skip cached layers

### Mock mode not working
- Set `MOCK_MODE=true` in `.env`
- Tool will generate synthetic data instead of querying Databricks

## Development

### Run Tests
```bash
docker-compose run databricks-cost-optimizer pytest tests/
```

### Format Code
```bash
docker-compose run databricks-cost-optimizer black src/
docker-compose run databricks-cost-optimizer ruff check src/
```

### Type Checking
```bash
docker-compose run databricks-cost-optimizer mypy src/
```

## Contributing

1. Clone repository and open in Dev Container
2. Install dev dependencies: `pip install -e ".[dev]"`
3. Create feature branch
4. Add tests for new functionality
5. Run `black`, `ruff`, and `mypy` before committing
6. Submit PR with clear description

## Support

- Databricks Docs: https://docs.databricks.com
- Issues: [GitHub Issues](https://github.com/your-org/databricks-cost-optimizer/issues)

## License

MIT License - See LICENSE file for details

## Disclaimer

This tool is provided as-is for cost analysis purposes. Savings estimates are based on identified patterns and may vary depending on your specific configuration and usage. Always test recommendations in a development environment before applying to production.
