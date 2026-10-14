# Trino Lakehouse Query Optimization & Benchmark Report

**Generated:** 2026-10-14T06:23:19Z  
**Target Host:** localhost (Trino 470 on Iceberg REST/JDBC catalog via MinIO)  
**Table Analyzed:** `iceberg.lakehouse.fact_observations` (7,314 records)  
**Benchmark Iterations:** 3 runs per query  
**Compaction & Stats Runtime:** 1747.2 ms  

> [!NOTE]
> Single-node Trino runtime (1.5 GB JVM heap) on a developer workstation. Results illustrate relative speedup and scan pruning dynamics, not raw cloud cluster throughput.

## Performance Comparison (Before vs After Compaction & ANALYZE)

| Query | Description | Before Mean (ms) | After Mean (ms) | Latency Delta | Change / Speedup | Rows |
|---|---|---:|---:|---:|---:|---:|
| Q1 | Species count (full scan) | 1363.9 ms | 521.2 ms | -842.7 ms | **+61.8% faster** | 10 |
| Q2 | Year + IUCN status (partition prune) | 598.3 ms | 557.6 ms | -40.6 ms | **+6.8% faster** | 21 |
| Q3 | Top 10 states | 618.5 ms | 619.4 ms | +0.9 ms | **-0.1% slower** | 10 |
| Q4 | Avg media score by quality grade | 682.9 ms | 437.4 ms | -245.5 ms | **+36.0% faster** | 2 |
| Q5 | Monthly trend Asian Elephant | 554.8 ms | 340.2 ms | -214.6 ms | **+38.7% faster** | 234 |
| Q6 | Weekend vs weekday | 668.9 ms | 439.9 ms | -229.0 ms | **+34.2% faster** | 2 |
| Q7 | Distinct species per state | 593.4 ms | 614.7 ms | +21.3 ms | **-3.6% slower** | 17 |

## Technical Observations & Optimization Impact

1. **Partition Pruning (Q2 vs Q1):** Filtering on partitioned dimensions (`obs_year >= 2020` and `iucn_status`) avoids scanning the entire dataset by leveraging Iceberg metadata manifest files.
2. **Small File Compaction:** PySpark Structured Streaming ingests data in micro-batches, generating multiple smaller Parquet files. Running `EXECUTE optimize(file_size_threshold => '128MB')` merges fragmented files into consolidated Parquet blocks, reducing split generation overhead in the Trino query planner.
3. **Cost-Based Optimizer (ANALYZE):** Running `ANALYZE` populates Trino with NDV (number of distinct values), null counts, and column size metrics, enabling the planner to choose optimal hash join strategies and pushdown filters.
4. **JVM Warmup & Buffer Caching:** Subsequent iterations benefit from Trino worker JVM compilation (JIT C2 compiler) and S3 client block cache hits.

## Reproducibility

```bash
python benchmarks/run_benchmark.py --host localhost --port 8080 --runs 3
```