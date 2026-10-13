# Viva Voce Cheat-Sheet & Defense Notes

**Course**: 22MDCEL10 Data Engineering Laboratory  
**Project**: Real-Time Lakehouse for Endangered Species Analytics

---

### Q1: Why use an Iceberg Lakehouse instead of an OLTP database (like SQLite/PostgreSQL) or a plain Data Lake (S3 Parquet)?
**Answer**:
- Plain S3 Parquet lacks ACID transactions, atomic commits, and snapshot isolation. Appending data while querying leads to reading partial or inconsistent states.
- OLTP databases like SQLite do not scale for heavy OLAP aggregations across millions of rows, cannot partition terabytes of data cleanly, and become storage bottlenecks.
- Apache Iceberg brings ACID transactions, Merge-on-Read, hidden partitioning, schema evolution, and time-travel querying to cheap object storage (MinIO/S3), queryable via Trino.

### Q2: How is deduplication handled in the streaming pipeline?
**Answer**:
- We generate a deterministic SHA-256 fingerprint hash for each record: `id|observed_on|latitude|longitude|common_name`.
- When writing micro-batches in PySpark, we execute an Iceberg `MERGE INTO lake.lakehouse.fact_observations t USING batch_data s ON t.record_hash = s.record_hash WHEN NOT MATCHED THEN INSERT *`.
- This ensures that if the producer replays Kafka messages, duplicates are discarded idempotently without duplicates in the gold/fact table.

### Q3: What is the Dead-Letter Queue (DLQ) pattern, and how is it implemented?
**Answer**:
- A DLQ quarantines malformed or out-of-boundary records without halting the streaming job.
- In PySpark, we split incoming records using a boolean filter checking null IDs, dates, coordinates outside India bounding boxes (`[6.0, 38.0]` lat, `[68.0, 98.0]` lon), and media score ranges (`[0, 10]`).
- Rejected records are enriched with an explicit `failure_reason` and written directly to `lake.lakehouse.dead_letter_queue`.

### Q4: Why did we choose `month(observed_on)` and `iucn_status` for partitioning?
**Answer**:
- Over-partitioning (e.g. by day or exact timestamp) causes the "small file problem", degrading metadata and read performance.
- Month-based partitioning aligns with time-series analytical queries (e.g. seasonal trends, annual counts).
- `iucn_status` is a low-cardinality categorical column frequently filtered in conservation queries. Iceberg prunes entire partition manifests at planning time.

### Q5: What is the purpose of compaction and the Airflow maintenance DAG?
**Answer**:
- Streaming micro-batches write small Parquet files every 15 seconds. Over time, query performance degrades due to high I/O overhead.
- Airflow runs `ALTER TABLE fact_observations EXECUTE optimize(file_size_threshold => '128MB')` hourly, rewriting small files into consolidated 128 MB Parquet files.
- It also runs `expire_snapshots` to clean up unreferenced historical data files and `ANALYZE` to refresh Trino cost-based optimizer table statistics.

### Q6: How does Trino achieve low-latency query performance on Iceberg?
**Answer**:
- Trino reads Iceberg metadata files (manifest lists and manifests) to perform partition pruning and min/max column index pruning before touching storage.
- Vectorized Parquet readers and dictionary filtering minimize CPU overhead and disk I/O.
- Compacting small files into larger ones allows Trino worker threads to read sequential chunks efficiently.

### Q7: How does Phase II maintain parity with Phase I?
**Answer**:
- All feature engineering logic is codified in `common/rules.py` and imported by Phase II.
- Parity is verified via `tests/test_parity.py` checking identical hashes, coordinate normalizations, IUCN mappings, state extractions, and validation outcomes.
- Trino views (`v_kpis`, `v_cube`, `v_dim_*`) reproduce the exact column structures and aggregate shapes required by the Phase I frontend dashboard.

### Q8: What happens during a pipeline failure?
**Answer**:
- If Spark streaming stops, Airflow's 5-minute `pipeline_health` DAG fails the Spark UI or data freshness checks.
- Airflow triggers failure notifications via SMTP (MailHog).
- Spark Structured Streaming resumes from S3 checkpoint offsets (`s3a://lakehouse/checkpoints/`) without data loss upon restart.

