## Role & Goal

You are a **senior data platform engineer and SaaS founder**.

Your task is to generate a **production-ready, Dockerized Python repository** that connects to a customer’s **Databricks workspace** and produces a **Databricks Cost & Performance Optimization Report**.

The project must:

* Run in **Docker** for client execution
* Include a **VS Code dev-container** for easy local development
* Use **one Dockerfile as the single source of truth**
* Never require code to run directly on the host

---

## Core Business Objective

The repository must:

1. Run fully inside Docker
2. Connect securely to a customer’s Databricks workspace
3. Extract usage, cost, cluster, job, and query metadata
4. Estimate Databricks spend (DBUs + cloud cost)
5. Identify waste and inefficiencies
6. Quantify potential monthly savings ($)
7. Generate a client-ready report
8. Require **read-only Databricks permissions only**

---

## Operating Model

### Client / Production

```bash
docker build -t databricks-cost-optimizer .
docker run --env-file .env -v $(pwd)/output:/output databricks-cost-optimizer
```

### Local Development

```bash
Reopen in Container (VS Code Dev Container)
```

Both must run the **same code and image**.

---

## REQUIRED REPO STRUCTURE

```
databricks-cost-optimizer/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── .env.example
├── config/
│   ├── default.yaml
│   └── pricing.yaml
├── src/
│   ├── main.py
│   ├── databricks_client.py
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── usage_collector.py
│   │   ├── cluster_collector.py
│   │   ├── job_collector.py
│   │   └── query_collector.py
│   ├── analyzers/
│   │   ├── __init__.py
│   │   ├── cost_analyzer.py
│   │   ├── cluster_analyzer.py
│   │   ├── job_analyzer.py
│   │   └── sql_analyzer.py
│   ├── recommendations/
│   │   ├── __init__.py
│   │   └── recommendation_engine.py
│   ├── reporting/
│   │   ├── __init__.py
│   │   ├── markdown_report.py
│   │   └── json_report.py
│   └── utils/
│       ├── __init__.py
│       ├── sql_runner.py
│       └── money.py
├── examples/
│   └── sample_report.md
├── tests/
│   └── test_cost_calculation.py
└── .devcontainer/
    ├── devcontainer.json
    └── Dockerfile
```

---

## DOCKER REQUIREMENTS (MANDATORY)

### Main Dockerfile

* Base image: `python:3.11-slim`
* Create non-root user
* Install dependencies
* Copy source code
* Entrypoint runs `src/main.py`
* Output written to `/output`

No secrets baked into image.

---

### docker-compose.yml

* Single service
* Loads `.env`
* Mounts `./output:/output`
* Used only for convenience

---

## DEV-CONTAINER REQUIREMENTS (IMPORTANT)

### Dev-container MUST:

* Reuse the **main Docker image**
* Not duplicate dependencies
* Mount source code for live editing
* Support debugging & tests

### `.devcontainer/Dockerfile`

* `FROM databricks-cost-optimizer:latest`
* Add only dev tooling (optional)

### `devcontainer.json`

* Uses the above Dockerfile
* Mounts project workspace
* Installs VS Code extensions:

  * Python
  * Pylance
  * YAML
* Sets default shell
* Runs as non-root user

Dev-container is **for development only**, not production.

---

## ENVIRONMENT VARIABLES

Loaded via `.env`:

```
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_HTTP_PATH=
START_DATE=
END_DATE=
OUTPUT_DIR=/output
```

---

## CONNECTIVITY REQUIREMENTS

* Prefer Databricks SQL Warehouse
* REST API fallback
* PAT token authentication
* Read-only permissions only

---

## DATA SOURCES (MANDATORY)

Use **Databricks system tables only**:

### Billing & Usage

* `system.billing.usage`
* `system.compute.node_usage`
* `system.compute.cluster_events`

### Clusters

* `system.compute.clusters`
* `system.compute.cluster_snapshots`

### Jobs

* `system.jobs.jobs`
* `system.jobs.job_runs`

### SQL Queries

* `system.query.history`

---

## ANALYSIS REQUIREMENTS

### Cost Analysis

* DBUs by cluster, job, user
* Estimated monthly cost
* Idle vs active DBUs
* % spend on terminated clusters
* Cost per job run

### Cluster Analysis

Detect:

* Always-on clusters
* Over-provisioned clusters
* High idle time
* Missing auto-termination
* No autoscaling

### Job Analysis

Detect:

* Long runtimes
* High retries
* Interactive cluster usage
* Overlapping schedules

### SQL / Query Analysis (CRITICAL)

Detect:

* `SELECT *`
* Missing WHERE clauses
* Excessive JOINs
* Missing partition filters
* Repeated table scans
* Cross-schema scans

For each inefficient query:

* Explanation
* Cost impact estimate
* Example improved SQL pattern

---

## RECOMMENDATION ENGINE

Each recommendation must include:

* Title
* Severity (Low / Medium / High)
* Estimated monthly savings
* Technical explanation
* Concrete remediation steps

---

## REPORTING REQUIREMENTS

Generate:

1. Markdown report (client-ready)
2. JSON report (machine-readable)

Markdown sections:

* Executive Summary
* Spend Breakdown
* Top Cost Drivers
* Waste Identified
* Optimization Opportunities
* Estimated Savings
* Next Steps

Tone:

* Professional
* Consultant-grade
* Non-blaming

---

## CONFIGURATION

Support:

* Date ranges
* DBU pricing overrides
* AWS region pricing
* Savings confidence factor

---

## README REQUIREMENTS

README must explain:

* What the tool does
* Why Docker is used
* Why dev-containers are included
* How to run (Docker & Dev Container)
* Required Databricks permissions
* Security & compliance guarantees
* Disclaimer (read-only, no mutation)

---

## FINAL QUALITY BAR

* Clean Python
* Type hints
* Docstrings
* Modular design
* `docker build` succeeds
* `docker run` produces report
* Dev-container opens cleanly
* Mock mode if no Databricks access

---

**END OF INSTRUCTIONS**

---