# Presenter Notes — GitHub Event Lakehouse Workshop

Your personal preparation guide. Keep this open during the session.

---

## Before the Workshop Day

### 1–2 days before
- [ ] Run `docker compose pull` on your own machine to pre-download all images (~6 GB). Confirm all 10 containers start cleanly.
- [ ] Run the full 24-hour DAG end-to-end: `docker compose up -d` → unpause DAG → wait for all 3 tasks to go green. This validates the pipeline works and pre-warms the Ivy JAR cache.
- [ ] Verify the Presto connection works in Superset SQL Lab: `SELECT count(*) FROM github_events;`
- [ ] Run the ACID superpowers demo to confirm it works with your locally ingested data.
- [ ] Share the repo URL and prerequisites with attendees **at least 3 days ahead**. Ask them to run `docker compose pull` before arriving — image downloads at conference WiFi speeds are painful.
- [ ] Test your screen sharing at the same resolution you'll use. Superset dashboards and Airflow's DAG graph view need screen width.

### Morning of the workshop
- [ ] Start `docker compose up -d` **early** (30–60 min before attendees join) so all health checks pass before the session starts.
- [ ] Optionally: trigger the DAG immediately so data is already ingested when you reach Scenario A — you can use the pre-populated data for the demo while attendees' pipelines run in the background.
- [ ] Have a terminal window, browser with 5 tabs open (Airflow, MinIO, Superset, Spark History, Presto Coordinator), and the slides ready.

---

## Audience Check-In (First 5 Minutes)

Ask attendees to confirm:
1. **Docker is running** and allocated ≥ 16 GB RAM (Docker Desktop → Settings → Resources)
2. **Repo is cloned** (`git clone <url> && cd presto_hudi_github_data_pipeline`)
3. **`.env` is configured** (`cp .env.example .env`)
4. **`docker compose pull` is running in the background** — start this NOW so ~6 GB of images download during the lecture

If anyone has < 16 GB Docker RAM → suggest they pair with a neighbor or reduce `--master local[2]` in the DAG.

---

## Slide-by-Slide Talking Points

### Slide 1 — Title
> "Today we're building a data lakehouse from scratch — entirely in Docker, on your laptop. By the end you'll have ingested 1.7 million real GitHub events, queried them with a distributed SQL cluster, run GDPR-compliant record deletions, and built a live BI dashboard. No cloud account needed."

### Slide 2 — Agenda
Briefly walk through the two parts. Emphasize that all hands-on exercises in Part 2 run locally without any cloud account needed.

### Slide 3 — Fast-Track Setup (Pre-Pull Images)
> "Before we dive into the lakehouse concepts, let's get our machines ready. Open your terminal, clone the repository, copy the `.env` file, and run `docker compose pull`. Keep that terminal running in the background. While we walk through the theory, ~6 GB of Docker images will download so we can start Part 2 instantly without waiting."

### Slide 4 — The Dataset: GitHub Archive
Walk through the scale: ~1.7M events/day, 24 hourly files, 16 distinct event types. Point out that PushEvents make up ~65% of the data.

### Slide 5 & 6 — What is a Data Lakehouse? & The Lakehouse Pattern
Ask: "Who's worked with a data warehouse before? A data lake? Great — a lakehouse gives you the best of both: the scalable cheap storage of a lake plus the ACID transactions, upserts, and fast queries of a warehouse."
- **GDPR deletes**: Before tools like Hudi, engineers had to write batch Spark jobs to rewrite entire dataset partitions to remove a single user's data.
- **Small files**: Frequent micro-batches create millions of tiny files. Hudi solves this with inline clustering.

### Slide 7 — Hudi Key Concepts
Take time here — these 6 boxes map directly to lines in `spark/github-ingest.properties`. Open the file in a terminal and point to each property as you explain it:
- Record Key = `hoodie.datasource.write.recordkey.field=id`
- Precombine Key = `hoodie.datasource.write.precombine.field=created_at`
- Partition = `hoodie.datasource.write.partitionpath.field=type`
- Commit Timeline = `.hoodie/` transaction log
- Inline Clustering & Cleaning = automatic small-file merge + obsolete version pruning

### Slide 8 & 9 — Technology Stack Overview & Deep Dive
Draw the analogy: "Think of this like a mini version of what Uber, Netflix, or Walmart runs in their own data platforms. The only difference is we're running it all inside Docker on your laptop instead of hundreds of AWS instances."

### Slide 10 & 11 — Architecture Data Flow & HoodieStreamer
Walk left to right slowly. Key points:
1. GitHub Archive publishes one `.json.gz` file per hour. We fetch the last 24.
2. Airflow orchestrates but doesn't do the heavy lifting — it delegates to Docker exec.
3. Spark's `HoodieStreamer` is the workhorse: it deduplicates, applies schema, writes Parquet, updates Hive Metastore.
4. The data quality gate at the end is a real CI/CD pattern — we're treating the lakehouse like production infrastructure.

---

## Scenario A — Ingest Data (Estimated time: 20–40 min running in background)

**Key actions to watch with attendees:**
1. Show the DAG unpause toggle — point out that the toggle both unpauses AND triggers the first run
2. Show the Grid View — three colored boxes will turn green sequentially
3. Click into Task 2 logs to show the HoodieStreamer output — `"Commit ..."` line is the ACID commit completing
4. While Task 2 runs → switch to http://localhost:4040 (Spark UI). Show the Jobs, Stages, and Storage tabs
5. After all tasks green → open MinIO Console http://localhost:9001, navigate to `github-raw-data` bucket → show `hudi-tables/github_events/` directory with `type=PushEvent/`, `type=PullRequestEvent/`, etc.

**If the DAG fails:**
- Task 1 fails: usually a network issue downloading from gharchive.org. Re-trigger. GitHub Archive sometimes has delays.
- Task 2 fails with OOM: reduce Docker RAM or change `local[4]` → `local[2]` in the DAG
- Task 2 fails with "Nothing to commit": raw files already exist from a previous run but Hudi has already processed them. Delete `hudi-tables/` prefix in MinIO and re-trigger.
- Task 3 fails: check that Presto is healthy first (`http://localhost:8080` should show the cluster info page)

---

## Scenario B — Query with Presto

**One-time Superset setup (do this for all attendees):**
- The SQLAlchemy URI is: `presto://superset@presto-server:8080/hudi/default`
- ⚠️ Common mistake: using `localhost` instead of `presto-server`. Superset runs inside Docker; it calls Presto container-to-container, not via the host's localhost.
- After "Test Connection" shows green → click Connect → then go to SQL Lab

**Live query demo script:**
```sql
-- Start simple: show all tables
SHOW TABLES;

-- Partition pruning: run without WHERE first, then with
SELECT type, count(*) AS events
FROM github_events
GROUP BY type ORDER BY events DESC;

-- Explain what happened: Presto read ALL partitions above
-- Now add a WHERE and watch Presto only scan one folder:
SELECT count(*) FROM github_events WHERE type = 'PushEvent';

-- Show nested struct access (this surprises people)
SELECT payload.push_id, payload.ref, payload.head
FROM github_events WHERE type = 'PushEvent' LIMIT 5;

-- Show the internal Hudi metadata column
SELECT _hoodie_commit_time, count(*) AS rows
FROM github_events
GROUP BY _hoodie_commit_time;
```

**Talking point on nested structs:** "In a traditional data warehouse you'd have to flatten this into separate columns. Because our schema is Avro and Hudi preserves the struct hierarchy into Parquet, we query nested objects with simple dot notation — no JSON parsing, no casting."

---

## Scenario C — ACID Superpowers Demo

**Setup:** Run in a second terminal before presenting so you're not waiting:
```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master "local[4]" --driver-memory 2G \
  /opt/spark/hudi_acid_superpowers_demo.py
```

**What to point out in the output:**
- Demo 1 (Upsert): Note the "Writing ACID UPSERT batch..." line, then the "Upsert completed!" line. Ask: "What would this have required in a plain data lake? You'd need to read the entire `type=PullRequestEvent` partition (~hundreds of MB), modify one row in memory, and write the whole partition back." With Hudi: only the Parquet file containing that record is rewritten.
- Demo 2 (GDPR Delete): This is the crowd-pleaser. A named user's records are surgically removed. After the demo runs, go back to Presto and run `SELECT count(*) FROM github_events WHERE actor.login = '<deleted_user>';` — should return 0.
- Demo 3 (Timeline): Show that the commit timestamps are now 3 (original ingest + upsert + delete). Each is a sealed, auditable transaction.

---

## Scenario D — Superset Dashboard

**Navigation:** Dashboards → "GitHub Archive Analytics (Last 24 Hours)"

**Charts to call out:**
1. **Activity Heatmap** — "This reveals the global developer workday rhythm. Peak hours are 14:00–18:00 UTC, which aligns with US East Coast morning + EU afternoon overlap."
2. **Humans vs Bots donut** — "19.6% of all GitHub events are automated. Tools like Dependabot, GitHub Actions, and now CoPilot are becoming significant actors in the developer ecosystem."
3. **PR Lifecycle** — "92.4% merge efficiency means that for every PR that isn't abandoned, it gets merged. This is a real signal about the health of the open-source ecosystem."
4. **Top AI Agents** — "coderabbitai[bot] is an AI code reviewer. This is a real signal of the industry shift toward automated code review."

**Virtual Dataset explanation:** "Notice there's zero data transformation pipeline running for Superset. The `github_events_flat` dataset is just a SQL query saved in Superset that runs against Presto which reads Parquet from MinIO. The entire BI layer is a chain of live queries — there are no data copies."

---

## Timing Guide

| Segment | Estimated Time | Notes |
|---------|---------------|-------|
| Welcome + fast-track setup | 10 min | Slides 1–3: attendees kick off `docker compose pull` |
| Slides Part 1 (Foundation) | 40 min | Slides 4–11: lecture while image download completes |
| Break | 10 min | |
| Env setup & boot (Slide 13) | 10 min | `docker compose up -d` starts instantly from cached images |
| Scenario A kick-off (Slide 15) | 10 min | Trigger DAG, explain what's happening, let it run in background |
| Scenario B — Presto queries (Slide 16–17) | 20 min | Connect Superset, run queries while DAG runs |
| Scenario C — ACID demo (Slide 18) | 20 min | Requires Scenario A complete first |
| Scenario D — Dashboard (Slide 19) | 15 min | |
| Design decisions (Slide 20) | 10 min | |
| Extensions + Q&A (Slide 21–23) | 20 min | |
| **Total** | **~3 hours** | |

---

## Frequently Asked Questions

**Q: Can I run this on Windows?**  
A: Yes, with Docker Desktop for Windows (WSL2 backend). The bind mounts in `docker-compose.yml` use forward slashes which Docker Desktop translates automatically. The Airflow container mounts `/var/run/docker.sock` — on Windows with Docker Desktop this maps to the named pipe automatically.

**Q: Why can't I UPDATE rows directly in Presto?**  
A: The Presto Hudi connector is read-only by design. Hudi writes go through Spark (either HoodieStreamer for bulk ingestion or PySpark for targeted upserts/deletes). Presto is the analytics engine; Spark is the mutation engine.

**Q: Why isn't partitioning by date better than by type?**  
A: For production, absolutely — date partitioning guarantees even file sizes. We partition by `type` in this workshop because it makes partition pruning visually obvious in SQL (`WHERE type = 'PushEvent'` maps directly to a folder name). See the Design Decisions slide.

**Q: What happens if the DAG runs twice on the same data?**  
A: Hudi's `id` record key deduplication means re-ingesting the same raw files writes zero new records. The "Nothing to commit" circuit breaker catches this and raises an error so Airflow marks the task failed (preventing false-positive green runs on stale data).

**Q: Can I connect this to real AWS S3 instead of MinIO?**  
A: Yes — update `.env` with `S3_ENDPOINT=https://s3.amazonaws.com`, `S3_PATH_STYLE_ACCESS=false`, `S3_SSL_ENABLED=true`, and real AWS credentials. Stop the `minio` and `minio-init` containers (they're not needed). Create the target S3 bucket manually first.

**Q: How many records does a full 24-hour run ingest?**  
A: Approximately 1.3–1.8 million GitHub events depending on the day. GitHub activity peaks on weekdays during US/EU business hours.

**Q: What does `_hoodie_commit_time` look like?**  
A: A 14-digit timestamp like `20260709143022`. This is the Hudi commit instant in `yyyyMMddHHmmss` format, stored as a string. It's automatically added to every record by Hudi.

---

## Emergency Commands Reference

```bash
# Check all container statuses
docker compose ps

# Restart just Presto (if S3 auth error)
docker compose restart presto-server presto-worker-1 presto-worker-2

# Re-initialize Hive Metastore schema (if MySQL was interrupted)
docker exec hive-metastore /opt/hive/bin/schematool -dbType mysql -initSchema

# Drop the Hudi table catalog entry (then re-run DAG to recreate)
docker exec spark-client /opt/spark/bin/spark-sql \
  --master "local[2]" --driver-memory 1G \
  -e "DROP TABLE IF EXISTS default.github_events;"

# Trigger DAG via CLI (instead of UI)
docker exec airflow airflow dags trigger github_events_ingestion

# View Task 2 logs in CLI
docker exec airflow airflow tasks logs github_events_ingestion trigger_hudi_ingestion <execution_date>

# Soft reset (preserves DAG history and images, deletes data only)
# 1. In MinIO console: delete hudi-tables/ prefix
# 2. Run the DROP TABLE command above
# 3. Re-trigger the DAG

# Hard reset (nuclear option — deletes EVERYTHING)
docker compose down -v && docker compose up -d
```

---

## Key Files Quick Reference

| File | What It Does |
|------|-------------|
| `docker-compose.yml` | All 10 services, 6 volumes, startup order |
| `.env.example` | 11 env vars — copy to `.env` |
| `airflow/dags/github_ingestion_dag.py` | The 3-task pipeline (download → ingest → quality gate) |
| `spark/github-ingest.properties` | 18 HoodieStreamer config properties |
| `spark/github-schema.avsc` | Avro schema contract (8 top-level fields) |
| `spark/hudi_acid_superpowers_demo.py` | ACID upsert, GDPR delete, timeline inspection |
| `presto/coordinator/catalog/hudi.properties` | Registers the `hudi` Presto catalog |
| `metastore/metastore-site.xml` | Hive Metastore config (MySQL + S3A) |
