# Databricks Cost & Performance Optimization Analyzer

A **production-grade, Dockerized Python tool** that connects to your Databricks workspace and generates a comprehensive cost optimization report.

## 📚 Documentation

- **[Quick Start Guide](QUICK_START.md)** - Get running in 5 minutes
- **[System Tables Reference](SYSTEM_TABLES.md)** - Detailed info on data sources and queries
- **[Go-To-Market Guide](GO_TO_MARKET.md)** - Sales and positioning guide

---

## What This Tool Does

✓ Analyzes your Databricks workspace usage and cost patterns  
✓ Identifies waste, inefficiencies, and optimization opportunities  
✓ Estimates potential monthly savings ($)  
✓ Generates client-ready Markdown + machine-readable JSON reports  
✓ Requires **read-only permissions only** (no data mutation)  
✓ Runs fully inside Docker (no Python installation needed locally)  

### Key Analysis Features

**Cost Analysis**
- Complete billing breakdown by product, user, cluster, job, warehouse
- Serverless vs Classic compute comparison
- DBU consumption patterns and trends

**Cluster Rightsizing (KPI-Based)**
- CPU/memory utilization analysis (P50, P90, P95 percentiles)
- Identifies over-provisioned clusters (wasting money)
- Identifies under-provisioned clusters (performance issues)
- Detects idle clusters (<5% CPU usage)
- Analyzes autoscale effectiveness
- Finds driver/worker resource imbalances

**Governance & Attribution**
- Tag compliance analysis (untagged resources)
- Weekend/off-hours waste detection
- Cost attribution gaps

**SQL Warehouse Monitoring**
- Currently running warehouses (long-running detection)
- Upscaled warehouses (high cluster count)
- Auto-stop configuration issues

**Query Performance Analysis**
- Disk spill detection (memory pressure → upsize recommendations)
- Shuffle-heavy queries (inefficient data movement)
- Longest-running queries with user attribution
- Query patterns by user

**Job Efficiency**
- High-failure jobs (wasted spend on retries)
- Short-running jobs (startup overhead)
- Job duration and cost patterns  

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

### System Tables Used

| System Table | Purpose | Analysis Feature |
|--------------|---------|------------------|
| `system.billing.usage` | Cost data, DBU consumption | Cost analysis, billing breakdown |
| `system.compute.clusters` | Cluster metadata | Configuration analysis |
| `system.compute.cluster_events` | Cluster lifecycle events | Uptime patterns, waste detection |
| `system.compute.node_timeline` | CPU/memory utilization metrics | Rightsizing, over/under-provisioning |
| `system.compute.warehouse_events` | SQL warehouse runtime/scaling | Long-running/upscaled warehouse detection |
| `system.jobs.jobs` | Job metadata | Job efficiency analysis |
| `system.jobs.job_runs` | Job execution history | Failure rates, duration patterns |
| `system.query.history` | Query execution details | Disk spill, shuffle analysis, slow queries |

### Required Permissions

```
system.billing.usage → READ
system.compute.clusters → READ
system.compute.cluster_events → READ
system.compute.node_timeline → READ
system.compute.warehouse_events → READ
system.jobs.jobs → READ
system.jobs.job_runs → READ
system.query.history → READ
```

**No cluster creation, modification, or data access required.**

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
- Business impact assessment

### Governance & Cost Attribution
- Tag compliance (untagged resources)
- Weekend/off-hours usage patterns
- Cost attribution by team/project

### Cluster Rightsizing Analysis (KPI-Based)
- CPU utilization (P50, P90 percentiles)
- Memory utilization (P50, P95 percentiles)
- Over-provisioned clusters (low utilization)
- Under-provisioned clusters (resource pressure)
- Idle cluster detection (<5% CPU usage)
- Autoscale effectiveness analysis
- Driver/worker resource imbalance

### Cost Breakdown
- Total DBUs consumed
- Cost by product (All-Purpose Compute, Jobs Compute, SQL Compute, DLT, etc.)
- Serverless vs Classic compute split
- Breakdown by cluster, job, user, warehouse

### SQL Warehouse Analysis
- Currently running warehouses (4+ hours)
- Scaled-up warehouses (high cluster count)
- Auto-stop configuration issues
- Warehouse sizing recommendations

### Job Analysis
- Top jobs by cost
- High failure rates (wasted spend)
- Short-running jobs (startup overhead)
- Long-running inefficient jobs
- Job efficiency patterns

### SQL Query Efficiency
- Top longest-running queries
- Disk spill detection (memory pressure)
- Shuffle-heavy queries (data movement)
- Query patterns by user
- Inefficient query patterns

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
┌─────────────────────────────────────────────────────────┐
│  Docker Container / Dev Container                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  src/main.py (orchestrator)                             │
│    ├─ collectors/                                       │
│    │  ├─ usage_collector.py (billing, tagging)         │
│    │  ├─ cluster_collector.py (config, events)         │
│    │  ├─ cluster_utilization_collector.py (CPU/mem)    │
│    │  ├─ job_collector.py (runs, failures)             │
│    │  ├─ warehouse_collector.py (runtime, scaling)     │
│    │  └─ query_collector.py (spill, shuffle, perf)     │
│    │                                                    │
│    ├─ analyzers/ (identify patterns)                    │
│    ├─ recommendations/ (generate ideas)                 │
│    └─ reporting/ (format output)                        │
│                                                         │
│  ↓ READ-ONLY SQL QUERIES ↓                            │
│                                                         │
└─────────────────────────────────────────────────────────┘
           ↓
    ╔════════════════════════════════════════╗
    ║   Databricks System Tables             ║
    ╟────────────────────────────────────────╢
    ║  • system.billing.usage                ║
    ║  • system.compute.clusters             ║
    ║  • system.compute.cluster_events       ║
    ║  • system.compute.node_timeline        ║
    ║  • system.compute.warehouse_events     ║
    ║  • system.jobs.jobs                    ║
    ║  • system.jobs.job_runs                ║
    ║  • system.query.history                ║
    ╚════════════════════════════════════════╝
           ↑
    SQL Warehouse (isolated compute)
```

**Data Flow**:
1. Tool connects via SQL Warehouse (no cluster cost overhead)
2. Queries system tables (read-only, no customer data access)
3. Collectors extract metadata (configs, metrics, events)
4. Analyzers identify patterns (inefficiencies, waste)
5. Recommendation engine generates actionable advice
6. Reports written to `/output` directory

See [SYSTEM_TABLES.md](SYSTEM_TABLES.md) for detailed documentation on each system table.

## Example Analysis Output

```markdown
# Databricks Cost & Performance Optimization Report

*Generated: 2024-01-13 14:23:45*

## Executive Summary

- **Analysis Period**: 30 days
- **Period Spend**: $10,234.50
- **Estimated Monthly Spend**: $12,450.00
- **Potential Monthly Savings**: $2,890.00
- **Optimization Opportunities**: 12
- **Savings Potential**: 23.2% of current spend

## Business Impact

🎯 **Recommended Actions**: 5 immediate, 4 medium-term
💰 **Implementation Payoff**: Reduce costs from $12,450/mo to $9,560/mo
⏱️ **Implementation Timeline**: 2-3 weeks
📊 **Annual Impact**: $34,680/year (~3-4 months runway extension)

## Governance & Cost Attribution

### Tag Compliance
🟡 **75.3%** of spend is properly tagged
- **Unattributable Spend**: ~$3,112/month (24.7%)
- **Untagged DBUs**: 12,450 of 50,380

### Weekend/Off-Hours Usage
⚠️ **Weekend-to-Weekday Ratio**: 35%
- **Estimated Weekend Spend**: ~$2,180/month

## Cluster Rightsizing Analysis

Analysis of CPU and memory utilization across **15** highest-cost clusters:

| Status | Clusters | % | DBUs Affected |
|--------|----------|---|---------------|
| ⬇️ Over-provisioned | 8 | 53% | 15,230 |
| ⬆️ Under-provisioned | 2 | 13% | 3,450 |
| ✅ Right-sized | 5 | 33% | - |

> 💰 **Potential Savings**: ~25% reduction on over-provisioned clusters could save ~$1,425/month

### 🔴 Idle Clusters Detected
Found **3** clusters running but essentially idle (<5% CPU for >50% of runtime)

### SQL Warehouse Analysis

#### 🕐 Currently Running Warehouses
| Name | Size | Running Hours | Clusters |
|------|------|---------------|----------|
| analytics-wh | Large | 🔴 12.3h | 1 |
| reporting-wh | Medium | 🟡 6.8h | 1 |

#### 💾 Disk Spill Detected
Some warehouses are running out of memory:
| Warehouse ID | Spill Frequency | Max Spill | Needs Upsize? |
|--------------|-----------------|-----------|---------------|
| 5f3a2b1c | 47 queries | 3.2GB | ⚠️ Yes |

#### 🔀 Shuffle-Heavy Queries
5 queries moving large amounts of data between nodes (optimization candidates)

## Recommendations

### 1. Terminate idle clusters immediately
**Severity**: HIGH | **Est. Savings**: $850/month

Found 3 clusters with <5% average CPU usage consuming 3,450 DBUs/month.

**Action Steps**:
1. Review cluster IDs: cluster-abc123, cluster-def456, cluster-ghi789
2. Verify no active workloads
3. Terminate or reduce size by 50%

### 2. Downsize over-provisioned prod-analytics cluster
**Severity**: HIGH | **Est. Savings**: $640/month

Cluster shows P50 CPU=18%, P50 Memory=35% - significantly over-provisioned.

**Action Steps**:
1. Current: 8 workers × m5.4xlarge
2. Recommended: 4 workers × m5.2xlarge
3. Monitor for 1 week, adjust if needed

### 3. Upsize data-warehouse-prod (disk spill detected)
**Severity**: MEDIUM | **Est. Savings**: -$200/month (cost increase, but 3x faster)

Warehouse spilling 3.2GB to disk in 47 queries - memory pressure detected.

**Action Steps**:
1. Current: Medium (32GB RAM per cluster)
2. Recommended: Large (64GB RAM per cluster)
3. Reduces query time by ~65%, prevents OOM errors

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
