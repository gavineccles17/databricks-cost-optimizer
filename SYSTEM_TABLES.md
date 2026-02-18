# System Tables Reference

This document describes all Databricks system tables accessed by the cost optimizer tool, what data is collected, and why it's needed.

## Overview

All system tables accessed are in **READ-ONLY** mode. No data is modified, created, or deleted. The tool only queries metadata about your Databricks workspace usage.

## System Tables Used

### 1. `system.billing.usage`

**Purpose**: Primary source for cost and DBU consumption data

**Columns Accessed**:
- `usage_date` - Date of usage
- `usage_metadata` - JSON with cluster/job/warehouse IDs
- `usage_unit` - DBU type (e.g., "DBU")
- `usage_quantity` - Number of DBUs consumed
- `identity_metadata` - User information
- `sku_name` - Product type (JOBS_COMPUTE, JOBS_LIGHT_COMPUTE, SQL_COMPUTE, etc.)
- `workspace_id` - Workspace identifier
- `record_id` - Unique record identifier
- `account_id` - Databricks account ID
- `cloud` - Cloud provider (AWS, Azure, GCP)
- `custom_tags` - User-defined tags for cost attribution

**Queries Performed**:
- Cost aggregation by date, product, user, cluster, job, warehouse
- Tag compliance analysis (tagged vs untagged resources)
- Weekend vs weekday usage patterns
- Serverless vs classic compute breakdown
- Daily/weekly spending trends

**Why Needed**: Core billing data for all cost analysis and recommendations

---

### 2. `system.compute.clusters`

**Purpose**: Cluster metadata and configuration

**Columns Accessed**:
- `cluster_id` - Unique cluster identifier
- `cluster_name` - Human-readable cluster name
- `cluster_source` - Source type (JOB, INTERACTIVE, etc.)
- `owner_user_name` - Cluster owner
- `autoscale` - Autoscaling configuration (min/max workers)
- `auto_termination_minutes` - When cluster auto-terminates
- `enable_elastic_disk` - Disk autoscaling setting
- `workspace_id` - Workspace identifier
- `state` - Current cluster state

**Queries Performed**:
- Cluster configuration review
- Auto-termination analysis (identify always-on clusters)
- Autoscale configuration validation
- Cluster ownership attribution

**Why Needed**: Identify configuration issues (missing auto-termination, inappropriate cluster types)

---

### 3. `system.compute.cluster_events`

**Purpose**: Cluster lifecycle events (start, stop, resize, etc.)

**Columns Accessed**:
- `cluster_id` - Cluster identifier
- `event_type` - Event type (CREATING, RUNNING, TERMINATING, etc.)
- `timestamp` - Event timestamp
- `details` - Event details (JSON)
- `workspace_id` - Workspace identifier

**Queries Performed**:
- Cluster uptime calculation
- Start/stop patterns
- Weekend usage detection
- Always-on cluster identification

**Why Needed**: Detect clusters running 24/7 unnecessarily, identify weekend waste

---

### 4. `system.compute.node_timeline`

**Purpose**: Real-time CPU and memory utilization metrics for rightsizing

**Columns Accessed**:
- `cluster_id` - Cluster identifier
- `timestamp` - Measurement timestamp
- `component` - Component type (driver or worker)
- `cpu_percent` - CPU utilization (0-100)
- `memory_used_bytes` - Memory in use
- `memory_total_bytes` - Total memory available
- `workspace_id` - Workspace identifier

**Queries Performed**:
- CPU utilization percentiles (P50, P90)
- Memory utilization percentiles (P50, P95)
- Over-provisioned detection (P50 CPU <40% AND P50 memory <70%)
- Under-provisioned detection (P90 CPU >85% OR P95 memory >95%)
- Idle cluster detection (avg CPU <5% for >50% of runtime)
- Driver vs worker resource balance
- Time-above-threshold analysis (how often resources are hot)

**Why Needed**: KPI-based rightsizing recommendations using actual utilization data (not guessing)

**Source**: Inspired by [Databricks blog on KPI-based rightsizing](https://community.databricks.com)

---

### 5. `system.compute.warehouse_events`

**Purpose**: SQL warehouse runtime and scaling events

**Columns Accessed**:
- `warehouse_id` - Warehouse identifier
- `event_type` - Event type (RUNNING, STOPPED, STARTING, SCALED_UP, SCALED_DOWN)
- `timestamp` - Event timestamp
- `cluster_count` - Number of clusters running
- `warehouse_name` - Warehouse name
- `warehouse_size` - Warehouse size (X-Small, Small, Medium, etc.)
- `workspace_id` - Workspace identifier

**Queries Performed**:
- **Long-running warehouses**: Find warehouses currently in RUNNING state for >4 hours
- **Upscaled warehouses**: Detect warehouses scaled up (>2 clusters) for >1 hour
- Warehouse usage patterns

**Why Needed**: Identify forgotten/idle warehouses consuming DBUs, detect over-scaling

**Source**: Inspired by [Databricks system tables queries](https://community.databricks.com)

---

### 6. `system.jobs.jobs`

**Purpose**: Job metadata and configuration

**Columns Accessed**:
- `job_id` - Unique job identifier
- `job_name` - Job name
- `creator_user_name` - Job creator
- `settings` - Job configuration (JSON)
- `workspace_id` - Workspace identifier
- `created_time` - When job was created

**Queries Performed**:
- Job configuration review
- Job ownership attribution
- Job metadata lookup

**Why Needed**: Link job runs to job names, analyze job configurations

---

### 7. `system.jobs.job_runs`

**Purpose**: Job execution history and performance

**Columns Accessed**:
- `job_id` - Job identifier
- `run_id` - Unique run identifier
- `start_time` - Run start time
- `end_time` - Run end time
- `state` - Final state (SUCCESS, FAILED, etc.)
- `run_duration` - Duration in milliseconds
- `workspace_id` - Workspace identifier

**Queries Performed**:
- Job failure rate calculation
- Average job duration analysis
- Cost per run calculation
- Failed run waste quantification
- Short-running job detection (startup overhead)

**Why Needed**: Identify unreliable jobs (high failure = wasted money), optimize job scheduling

---

### 8. `system.query.history`

**Purpose**: SQL query execution details and performance metrics

**Columns Accessed**:
- `statement_id` - Unique query identifier
- `warehouse_id` - Warehouse that ran the query
- `executed_by` - User who ran query
- `start_time` - Query start time
- `end_time` - Query end time
- `execution_status` - FINISHED, FAILED, etc.
- `duration` - Query duration (milliseconds)
- `read_rows` - Rows read
- `read_bytes` - Bytes read
- `statement_type` - SELECT, INSERT, etc.
- `statement_text` - SQL query text (preview only)
- `spilled_local_bytes` - Bytes spilled to disk (memory overflow)
- `shuffle_read_bytes` - Data shuffled between nodes
- `workspace_id` - Workspace identifier

**Queries Performed**:
- **Longest-running queries**: Top queries by duration
- **Disk spill analysis**: Find warehouses spilling >1GB to disk (memory pressure)
- **Shuffle-heavy queries**: Detect queries moving >10GB between nodes
- Query patterns by user
- Inefficient query detection

**Why Needed**: 
- **Disk spill** → Warehouse needs upsizing (running out of memory)
- **Heavy shuffle** → Query needs optimization (inefficient data movement)
- Long queries → Performance issues, cost impact

**Source**: Inspired by [Databricks community queries on warehouse sizing](https://community.databricks.com)

---

## Query Frequency

All queries are executed **once per analysis run**. No continuous monitoring or polling occurs.

Typical analysis queries:
- Date range: 7-90 days (configurable via `START_DATE` and `END_DATE`)
- No real-time or streaming queries
- All queries use filters (date, workspace_id) to minimize compute

---

## Data Privacy & Security

✅ **No PII Collected**: User emails are aggregated, no personal data stored  
✅ **No Query Content**: Only query metadata, not actual data returned by queries  
✅ **No Table Access**: Tool never queries customer tables, only system metadata  
✅ **No Data Export**: All analysis stays within Docker container  
✅ **Read-Only**: System tables are read-only by design, tool cannot modify them  

---

## Performance Impact

**Minimal to Zero Impact on Workspace**:
- Queries run against system tables (pre-aggregated, optimized by Databricks)
- Small data volumes (metadata only, not raw data)
- Uses SQL Warehouse (isolated compute, doesn't affect production clusters)
- Queries typically complete in seconds

**Estimated DBU Cost per Analysis**: <0.1 DBUs (~$0.04 on AWS Standard pricing)

---

## Troubleshooting

### "Table or view not found: system.billing.usage"

**Cause**: System tables not available in your Databricks workspace  
**Solution**: System tables require Databricks Runtime 11.3+ and workspace admin enablement. Contact your Databricks account team.

### "Permission denied on system.billing.usage"

**Cause**: PAT token doesn't have read access to system tables  
**Solution**: 
1. Ask workspace admin to grant system table access
2. Create new PAT with updated permissions
3. Update `.env` with new token

### "No data returned from system tables"

**Cause**: Date range outside workspace history OR no usage in period  
**Solution**:
1. Check `START_DATE` and `END_DATE` in `.env`
2. Verify workspace had usage during that period
3. Try shorter/more recent date range

---

## References

- [Databricks System Tables Documentation](https://docs.databricks.com/en/administration-guide/system-tables/index.html)
- [System Tables Billing Usage Schema](https://docs.databricks.com/en/administration-guide/system-tables/billing.html)
- [System Tables Compute Schema](https://docs.databricks.com/en/administration-guide/system-tables/compute.html)
- [Community Blog: KPI-Based Cluster Rightsizing](https://community.databricks.com)
- [Community Blog: 7 Queries for Cost Optimization](https://community.databricks.com)

---

## Version History

- **v1.0**: Initial system tables implementation (billing, clusters, jobs, queries)
- **v1.1**: Added `node_timeline` for CPU/memory utilization analysis
- **v1.2**: Added `warehouse_events` for SQL warehouse monitoring
- **v1.3**: Added disk spill and shuffle analysis from `query.history`
