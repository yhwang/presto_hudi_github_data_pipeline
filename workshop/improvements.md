# Workshop Improvements — Pre-Workshop Checklist

Findings from a thorough code and configuration review of the repo, organized by priority.

---

## 🔴 Critical — Fix Before Every Workshop

### 1. Ingestion runtime is incompatible with a live workshop
**File:** [`airflow/dags/github_ingestion_dag.py`](../airflow/dags/github_ingestion_dag.py:94)  
**Problem:** The DAG iterates over **24 hourly files** (~1.7M events). Downloading from `data.gharchive.org` takes **20–40 minutes** due to network latency and file size (~50–200 MB each compressed). A live workshop cannot wait that long.  
**Fix (recommended):** Add a `WORKSHOP_MODE` environment variable that reduces the loop from 24 to 3 or 4 hours:
```python
# In download_and_upload_github_data()
hours_to_fetch = int(os.environ.get('WORKSHOP_HOURS', '24'))
for i in range(hours_to_fetch):
```
Set `WORKSHOP_HOURS=4` in `.env` for the workshop. 4 hours ≈ 250k–300k events, still analytically rich, ingestion completes in ~5 minutes.

**Alternative:** Run the full 24-hour DAG in the background *before* the session starts and use `docker compose down` (not `-v`) to preserve the data. Start the session with the data already there.

---

### 2. The DAG `start_date` is hardcoded and will expire
**File:** [`airflow/dags/github_ingestion_dag.py`](../airflow/dags/github_ingestion_dag.py:53)  
**Problem:** `'start_date': datetime.datetime(2026, 7, 3)` is a hardcoded past date. As months pass, Airflow's backfill logic (even with `catchup=False`) can cause confusion on first unpause. The date will need manual updates before each workshop season.  
**Fix:**
```python
'start_date': datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0),
```
Or simply document in presenter notes that this must be set to a recent date before the workshop.

---

### 3. No pre-flight check for Docker memory allocation
**Problem:** The #1 attendee failure is Docker running with < 16 GB RAM, causing Spark OOM. There is no guard rail.  
**Fix:** Add a `check_environment` pre-task to the DAG (or a standalone shell script) that reads Docker system info and warns early:
```python
# In a new 'preflight_check' task (runs before download)
import subprocess, json
info = json.loads(subprocess.check_output(['docker', 'info', '--format', '{{json .}}']).decode())
mem_gb = info.get('MemTotal', 0) / (1024**3)
if mem_gb < 14:
    raise RuntimeError(f"Insufficient Docker memory: {mem_gb:.1f} GB. Need 16 GB.")
```
Or add a `workshop/preflight.sh` script attendees run before `docker compose up`:
```bash
#!/bin/bash
docker info --format '{{.MemTotal}}' | awk '{if ($1 < 15000000000) print "WARNING: Docker has less than 16GB RAM allocated. Increase in Docker Desktop → Resources."}'
echo "Docker memory: $(docker info --format '{{.MemTotal}}') bytes"
```

---

## 🟡 Important — Improve Workshop Experience

### 4. Superset database connection is manual and error-prone
**Problem:** Every attendee must manually create the Presto connection in Superset (Settings → Database Connections → + Database). The most common mistake is using `localhost` instead of `presto-server` in the SQLAlchemy URI.  
**Fix:** Add an auto-init script to the Superset container entrypoint that creates the connection programmatically using Superset's CLI:
```bash
superset set_database_uri -d "Presto Hudi" -u "presto://superset@presto-server:8080/hudi/default"
```
This runs after `superset init` and before `run-server.sh`. The connection then persists in the `superset-data` volume.

---

### 5. No workshop-mode documentation for quick ingestion verification
**Problem:** After the DAG completes, there is no single "did it work?" command that an attendee can run to verify end-to-end in < 5 seconds.  
**Fix:** Add a `workshop/verify.sh` script:
```bash
#!/bin/bash
echo "=== Checking containers ===" && docker compose ps
echo ""
echo "=== Checking Hudi table (requires ingestion to have run) ===" 
docker exec presto-server presto-cli --catalog hudi --schema default --execute \
  "SELECT type, count(*) AS events FROM github_events GROUP BY type ORDER BY events DESC LIMIT 5;"
```

---

### 6. `hudi_acid_superpowers_demo.py` exits silently if table is empty
**File:** [`spark/hudi_acid_superpowers_demo.py`](../spark/hudi_acid_superpowers_demo.py:49)  
**Problem:** If `df.filter(...)` returns no rows (e.g., no PullRequestEvent in the data), `sample_row` is an empty list and the upsert demo is silently skipped with no error message. Attendees see blank output and are confused.  
**Fix:**
```python
if not sample_row:
    print("⚠️  No PullRequestEvent or IssuesEvent found. Try IssueCommentEvent:")
    sample_row = df.filter(col("type") == "IssueCommentEvent").limit(1).collect()
if not sample_row:
    print("⚠️  Table appears empty or these event types are missing. Run the ingestion DAG first.")
    sys.exit(1)
```

---

### 7. Airflow uses SequentialExecutor — runs one task at a time
**File:** [`docker-compose.yml`](../docker-compose.yml:243)  
**Current:** `AIRFLOW__CORE__EXECUTOR=SequentialExecutor`  
**Impact:** Only one DAG can run at a time. This is fine for a workshop where attendees run sequentially, but if multiple people share one instance or a task hangs, nothing else can run.  
**Note:** This is a deliberate simplification (SQLite backend). Document this in presenter notes as a known limitation. For larger groups or shared instances, upgrade to `LocalExecutor` with a PostgreSQL backend.

---

### 8. `source-limit` of 1 GB may be too high for machines with 16 GB RAM
**File:** [`airflow/dags/github_ingestion_dag.py`](../airflow/dags/github_ingestion_dag.py:177)  
**Current:** `--source-limit 1073741824` (1 GB)  
**Problem:** On machines with exactly 16 GB Docker allocation, processing up to 1 GB of compressed JSON (decompresses to ~10 GB) alongside the 6 GB driver heap may hit memory pressure.  
**Fix for workshop:** Reduce to `268435456` (256 MB) when using `WORKSHOP_HOURS=4`. Full 24-hour runs can keep 1 GB.

---

## 🟢 Nice-to-Have — Polish

### 9. No `.env.workshop` template for workshop-specific settings
**Fix:** Add a `.env.workshop` file to the repo (tracked):
```dotenv
# Workshop-optimized settings (cp .env.workshop .env)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET_NAME=github-raw-data
S3_ENDPOINT=http://minio:9000
S3_PATH_STYLE_ACCESS=true
S3_SSL_ENABLED=false
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_PASSWORD=hivepassword
SUPERSET_SECRET_KEY=superset_workshop_key_123
SLACK_WEBHOOK_URL=
WORKSHOP_HOURS=4
```
This makes setup a one-liner: `cp .env.workshop .env && docker compose up -d`.

---

### 10. No schema evolution exercise built into the workshop
**Opportunity:** Schema evolution is one of Hudi's most powerful features but has no guided hands-on exercise.  
**Add to repo:** `workshop/exercises/schema_evolution.md` with step-by-step instructions:
1. Add `reactions_count` (long, nullable) to `github-schema.avsc`
2. Re-trigger the DAG
3. Query to verify old records show NULL for the new field
4. Add a breaking change (rename a field) and follow the 3-step table recreation workflow

---

### 11. Presto worker healthcheck is missing
**File:** [`docker-compose.yml`](../docker-compose.yml:151)  
**Problem:** `presto-worker-1` and `presto-worker-2` have no `healthcheck` defined. This means `docker compose ps` shows them as `running` even if the Presto launcher has not fully registered with the coordinator.  
**Fix:** Add the same healthcheck as the coordinator but checking the worker endpoint:
```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -sf http://localhost:8080/v1/info | grep -q '\"starting\":false' || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 12
  start_period: 30s
```

---

### 12. No `.gitignore` entry for the `workshop/` artifacts directory
**File:** [`.gitignore`](../.gitignore)  
**Problem:** Running `docker compose up` and the workshop may generate logs or temp files in `workshop/`. Consider adding:
```gitignore
workshop/*.log
workshop/*.tmp
```
The PPTX, improvements.md, and presenter_notes.md should be tracked.

---

## Summary Table

| # | Severity | Area | Effort |
|---|----------|------|--------|
| 1 | 🔴 Critical | Add `WORKSHOP_HOURS` env var to reduce DAG runtime | 10 min |
| 2 | 🔴 Critical | Fix hardcoded `start_date` in DAG | 2 min |
| 3 | 🔴 Critical | Add pre-flight Docker RAM check script | 15 min |
| 4 | 🟡 Important | Auto-create Superset Presto connection | 20 min |
| 5 | 🟡 Important | Add `workshop/verify.sh` end-to-end check script | 10 min |
| 6 | 🟡 Important | Fix silent skip in ACID demo when event type missing | 5 min |
| 7 | 🟡 Important | Document SequentialExecutor limitation | 2 min |
| 8 | 🟡 Important | Reduce `source-limit` for workshop mode | 5 min |
| 9 | 🟢 Nice-to-have | Add `.env.workshop` template | 5 min |
| 10 | 🟢 Nice-to-have | Add schema evolution exercise | 30 min |
| 11 | 🟢 Nice-to-have | Add healthcheck for Presto workers | 5 min |
| 12 | 🟢 Nice-to-have | Update `.gitignore` for workshop dir | 2 min |
