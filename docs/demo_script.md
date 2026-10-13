# Live Demonstration Script

**Course**: 22MDCEL10 Data Engineering Laboratory  
**Project**: Phase II Real-Time Lakehouse Live Demo

---

## Pre-requisites & Verification
1. Ensure Docker Desktop is running.
2. Verify all tests pass:
   ```bash
   python -m pytest -v
   ```

---

## Step-by-Step Live Demo

### 1. Launch the Lakehouse Stack (1 command)
```bash
docker compose -f docker-compose.yml -f docker-compose.lite.yml up -d
```
Verify container health:
```bash
docker compose ps
```

### 2. Verify Lakehouse Schema & Trino Catalogs
```bash
docker exec lakehouse-trino trino --execute "SHOW CATALOGS"
docker exec lakehouse-trino trino --execute "SHOW TABLES FROM lake.lakehouse"
```

### 3. Start Streaming Ingestion & Replay Backfill
Open a terminal and start the PySpark streaming job:
```bash
docker exec -d lakehouse-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/streaming/stream_job.py
```

Trigger backfill of the Phase I CSV dataset into Kafka:
```bash
docker exec lakehouse-producer python -m producer.producer --mode backfill --csv /opt/spark/work-dir/phase1/master_endangered_species.csv
```

### 4. Demonstrate Near-Real-Time Data Ingestion (<30s Latency)
Query Trino row counts:
```bash
docker exec lakehouse-trino trino --execute "SELECT COUNT(*) FROM lake.lakehouse.fact_observations"
```
Wait 15 seconds (micro-batch trigger interval) and re-query. Watch the row count increment continuously.

### 5. Open Real-Time Dashboard
1. Open your browser to `http://localhost:8050`.
2. Notice the live KPIs, category distribution, spatial map, and OLAP cube queries updating directly from Trino.
3. Post a simulated live observation from the UI or via live producer mode:
   ```bash
   docker exec -d lakehouse-producer python -m producer.producer --mode live --interval 2
   ```
4. Refresh `http://localhost:8050` after ~20 seconds to see new observations reflected on the map.

### 6. Demonstrate Dead-Letter Queue (DLQ)
Publish a malformed record (e.g. coordinates outside India):
```bash
docker exec lakehouse-trino trino --execute "SELECT failure_reason, COUNT(*) FROM lake.lakehouse.dead_letter_queue GROUP BY failure_reason"
```

### 7. Run Airflow Maintenance & Health Checks
1. Access Airflow UI at `http://localhost:8080` (credentials: `admin`/`admin`).
2. Trigger the `table_maintenance` DAG to see Iceberg file compaction (`OPTIMIZE`) and `ANALYZE` run.
3. Access MailHog at `http://localhost:8025` to inspect alerting emails.

### 8. Run Performance Benchmarks
```bash
python benchmarks/run_benchmark.py --host localhost --port 8080 --runs 3
```
Inspect the generated `docs/benchmark_report.md`.
