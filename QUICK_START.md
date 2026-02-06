# Quick Start Guide

Get the Databricks Cost Optimizer running in under 5 minutes.

## 1️⃣ Prerequisites

- **Docker** installed ([download here](https://www.docker.com/products/docker-desktop))
- **Databricks workspace** with SQL warehouse
- **Personal Access Token (PAT)** from Databricks

## 2️⃣ Get Your Databricks Credentials

### Step A: Create a PAT Token
1. Log into your Databricks workspace
2. Go to **Settings** → **User Settings** → **Access Tokens**
3. Click **Generate new token**
4. Copy the token (you'll only see it once!)

### Step B: Find Your Workspace Details
1. In Databricks, go to **SQL** → **SQL Warehouses**
2. Click your warehouse
3. Copy the **Server hostname** (e.g., `adb-123456.cloud.databricks.com`)
4. Copy the **HTTP path** (e.g., `/sql/1.0/warehouses/abc123def`)

## 3️⃣ Clone & Configure

```bash
# Clone the repository
git clone https://github.com/your-org/databricks-cost-optimizer.git
cd databricks-cost-optimizer

# Copy the example config
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use your favorite editor
```

**Your `.env` should look like:**
```
DATABRICKS_HOST=https://adb-123456.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123def
START_DATE=2024-01-01
END_DATE=2024-01-31
OUTPUT_DIR=/output
MOCK_MODE=false
```

## 4️⃣ Build & Run

```bash
# Build the Docker image
docker build -t databricks-cost-optimizer .

# Run the analysis
docker run --env-file .env -v $(pwd)/output:/output databricks-cost-optimizer

# View your reports
cat output/optimization_report.md
```

## 5️⃣ Check Your Results

```bash
ls -la output/

# Open the Markdown report
open output/optimization_report.md  # macOS
xdg-open output/optimization_report.md  # Linux
start output/optimization_report.md  # Windows

# Or view the JSON for programmatic access
cat output/optimization_report.json | python -m json.tool
```

## 🧪 Test Mode (No Databricks Needed)

Want to test the tool without connecting to Databricks?

```bash
# Run in mock mode
docker run --env MOCK_MODE=true -v $(pwd)/output:/output databricks-cost-optimizer
```

This generates sample data so you can see how reports look.

## 🐳 Using docker-compose (Convenience)

If you prefer a simpler command:

```bash
# Edit .env first with your credentials
docker-compose run --rm databricks-cost-optimizer

# Or with mock data
docker-compose run --env MOCK_MODE=true --rm databricks-cost-optimizer
```

## ⚠️ Permissions Required

The tool only needs **read-only** access. Your PAT token requires permissions to:

- `system.billing.usage` → read
- `system.compute.clusters` → read  
- `system.compute.cluster_events` → read
- `system.jobs.jobs` → read
- `system.jobs.job_runs` → read
- `system.query.history` → read

**No create, modify, or delete permissions needed.**

## 🆘 Troubleshooting

### "Connection refused"
- Verify `DATABRICKS_HOST` includes `https://`
- Check your SQL warehouse is running
- Confirm your PAT token is valid (hasn't expired)

### "Permission denied" errors
- Ensure your PAT token has read access to system tables
- Contact your Databricks admin for workspace permissions

### Docker not found
- Install Docker Desktop from https://www.docker.com

### On Windows with WSL?
- Use WSL2 backend for Docker
- Path should be `/home/username/...` not `C:\Users\...`

## 📊 Output Files

After running, check `/output/`:

| File | Purpose | Format |
|------|---------|--------|
| `optimization_report.md` | Client-ready summary | Markdown (human-readable) |
| `optimization_report.json` | Full analysis data | JSON (machine-readable) |

## 🚀 Next Steps

1. **Share the Markdown report** with stakeholders
2. **Review recommendations** by severity (High → Low)
3. **Prioritize** by estimated monthly savings
4. **Test changes** in dev environment first
5. **Schedule monthly re-runs** to track progress

## 💡 Pro Tips

- **Adjust date range** in `.env` to analyze different periods
- **Override DBU pricing** if your rates differ from defaults
- **Run monthly** to track cost reduction progress
- **Share JSON** with dashboarding tools (Tableau, Looker, etc.)

## 🔐 Security Notes

✓ No data is exported from your workspace  
✓ No data is sent to external services  
✓ All analysis runs locally in the Docker container  
✓ Your PAT token stays in the `.env` file (never logged)  
✓ `.env` is in `.gitignore` - don't commit it to git!

---

**Questions?** Check the [full README](README.md) or open an issue.
