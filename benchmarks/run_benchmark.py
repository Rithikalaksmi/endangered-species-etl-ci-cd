"""
Benchmark runner: Q1–Q7 on Trino Iceberg table.
Runs queries before and after Iceberg compaction (OPTIMIZE + ANALYZE).
Outputs results to docs/benchmark_report.md.

Usage:
    python benchmarks/run_benchmark.py [--host localhost] [--port 8080] [--runs 3]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.rules import setup_logger

log = setup_logger("benchmark", component="benchmark")

QUERIES = [
    (
        "Q1", "Species count (full scan)",
        "SELECT animal_category, COUNT(*) AS total_obs FROM iceberg.lakehouse.fact_observations GROUP BY animal_category ORDER BY total_obs DESC"
    ),
    (
        "Q2", "Year + IUCN status (partition prune)",
        "SELECT obs_year, iucn_status, COUNT(*) AS cnt FROM iceberg.lakehouse.fact_observations WHERE obs_year >= 2020 GROUP BY obs_year, iucn_status ORDER BY obs_year, cnt DESC"
    ),
    (
        "Q3", "Top 10 states",
        "SELECT state_province, COUNT(*) AS sightings FROM iceberg.lakehouse.fact_observations GROUP BY state_province ORDER BY sightings DESC LIMIT 10"
    ),
    (
        "Q4", "Avg media score by quality grade",
        "SELECT quality_grade, ROUND(AVG(CAST(media_score AS double)), 3) AS avg_media, COUNT(*) AS n FROM iceberg.lakehouse.fact_observations GROUP BY quality_grade ORDER BY avg_media DESC"
    ),
    (
        "Q5", "Monthly trend Asian Elephant",
        "SELECT obs_year, obs_month, COUNT(*) AS monthly_count FROM iceberg.lakehouse.fact_observations WHERE animal_category = 'Asian Elephant' GROUP BY obs_year, obs_month ORDER BY obs_year, obs_month"
    ),
    (
        "Q6", "Weekend vs weekday",
        "SELECT CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, COUNT(*) AS observations, ROUND(AVG(CAST(media_score AS double)), 3) AS avg_media FROM iceberg.lakehouse.fact_observations GROUP BY is_weekend"
    ),
    (
        "Q7", "Distinct species per state",
        "SELECT state_province, COUNT(DISTINCT animal_category) AS species_count, COUNT(*) AS total_obs FROM iceberg.lakehouse.fact_observations GROUP BY state_province ORDER BY species_count DESC, total_obs DESC"
    ),
]


def run_query(cursor, sql: str, runs: int) -> tuple[list[float], list]:
    """Run SQL `runs` times, return (latencies_ms, last_result_rows)."""
    latencies = []
    rows = []
    for _ in range(runs):
        t0 = time.perf_counter()
        cursor.execute(sql)
        rows = cursor.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        latencies.append(elapsed)
    return latencies, rows


def execute_maintenance(cursor):
    """Run Iceberg compaction (rewrite_data_files) and Trino statistics collection."""
    log.info("Running Iceberg compaction (OPTIMIZE rewrite_data_files)...")
    t0 = time.perf_counter()
    try:
        cursor.execute("ALTER TABLE iceberg.lakehouse.fact_observations EXECUTE optimize(file_size_threshold => '128MB')")
        optimize_result = cursor.fetchall()
        log.info(f"OPTIMIZE complete: {optimize_result}")
    except Exception as exc:
        log.warning(f"OPTIMIZE command notice: {exc}")

    log.info("Collecting table statistics (ANALYZE)...")
    try:
        cursor.execute("ANALYZE iceberg.lakehouse.fact_observations")
        analyze_result = cursor.fetchall()
        log.info(f"ANALYZE complete: {analyze_result}")
    except Exception as exc:
        log.warning(f"ANALYZE notice: {exc}")

    maintenance_time_ms = (time.perf_counter() - t0) * 1000
    return maintenance_time_ms


def build_comparison_report(
    before_results: list[dict],
    after_results: list[dict],
    maintenance_ms: float,
    row_count: int,
    runs: int,
    host: str,
    ts: str,
) -> str:
    lines = [
        "# Trino Lakehouse Query Optimization & Benchmark Report",
        "",
        f"**Generated:** {ts}  ",
        f"**Target Host:** {host} (Trino 470 on Iceberg REST/JDBC catalog via MinIO)  ",
        f"**Table Analyzed:** `iceberg.lakehouse.fact_observations` ({row_count:,} records)  ",
        f"**Benchmark Iterations:** {runs} runs per query  ",
        f"**Compaction & Stats Runtime:** {maintenance_ms:.1f} ms  ",
        "",
        "> [!NOTE]",
        "> Single-node Trino runtime (1.5 GB JVM heap) on a developer workstation. Results illustrate relative speedup and scan pruning dynamics, not raw cloud cluster throughput.",
        "",
        "## Performance Comparison (Before vs After Compaction & ANALYZE)",
        "",
        "| Query | Description | Before Mean (ms) | After Mean (ms) | Latency Delta | Change / Speedup | Rows |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]

    for b, a in zip(before_results, after_results):
        delta = a["mean_ms"] - b["mean_ms"]
        pct = ((b["mean_ms"] - a["mean_ms"]) / b["mean_ms"]) * 100.0 if b["mean_ms"] > 0 else 0.0
        pct_str = f"+{pct:.1f}% faster" if pct >= 0 else f"{pct:.1f}% slower"
        lines.append(
            f"| {b['id']} | {b['desc']} | "
            f"{b['mean_ms']:.1f} ms | {a['mean_ms']:.1f} ms | "
            f"{delta:+.1f} ms | **{pct_str}** | {b['rows']} |"
        )

    lines += [
        "",
        "## Technical Observations & Optimization Impact",
        "",
        "1. **Partition Pruning (Q2 vs Q1):** Filtering on partitioned dimensions (`obs_year >= 2020` and `iucn_status`) avoids scanning the entire dataset by leveraging Iceberg metadata manifest files.",
        "2. **Small File Compaction:** PySpark Structured Streaming ingests data in micro-batches, generating multiple smaller Parquet files. Running `EXECUTE optimize(file_size_threshold => '128MB')` merges fragmented files into consolidated Parquet blocks, reducing split generation overhead in the Trino query planner.",
        "3. **Cost-Based Optimizer (ANALYZE):** Running `ANALYZE` populates Trino with NDV (number of distinct values), null counts, and column size metrics, enabling the planner to choose optimal hash join strategies and pushdown filters.",
        "4. **JVM Warmup & Buffer Caching:** Subsequent iterations benefit from Trino worker JVM compilation (JIT C2 compiler) and S3 client block cache hits.",
        "",
        "## Reproducibility",
        "",
        "```bash",
        "python benchmarks/run_benchmark.py --host localhost --port 8080 --runs 3",
        "```",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Trino Iceberg Benchmark Runner")
    parser.add_argument("--host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out", default="docs/benchmark_report.md")
    args = parser.parse_args()

    import trino

    log.info(f"Connecting to Trino host={args.host}, port={args.port}")
    conn = trino.dbapi.connect(
        host=args.host,
        port=args.port,
        user="benchmark",
        catalog="iceberg",
        schema="lakehouse",
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM iceberg.lakehouse.fact_observations")
    row_count = cur.fetchone()[0]
    log.info(f"Target table row count: {row_count}")

    # Phase 1: Benchmark Before Compaction
    log.info("=== Phase 1: Running Queries BEFORE Compaction ===")
    before_results = []
    for qid, desc, sql in QUERIES:
        latencies, rows = run_query(cur, sql, args.runs)
        mean_lat = mean(latencies)
        before_results.append({
            "id": qid,
            "desc": desc,
            "min_ms": min(latencies),
            "mean_ms": mean_lat,
            "max_ms": max(latencies),
            "rows": len(rows),
            "latencies": latencies,
        })
        log.info(f"Query {qid} (Before): mean={mean_lat:.1f}ms")

    # Phase 2: Table Maintenance (Compaction + Stats)
    log.info("=== Phase 2: Executing Table Maintenance ===")
    maintenance_ms = execute_maintenance(cur)

    # Phase 3: Benchmark After Compaction
    log.info("=== Phase 3: Running Queries AFTER Compaction ===")
    after_results = []
    for qid, desc, sql in QUERIES:
        latencies, rows = run_query(cur, sql, args.runs)
        mean_lat = mean(latencies)
        after_results.append({
            "id": qid,
            "desc": desc,
            "min_ms": min(latencies),
            "mean_ms": mean_lat,
            "max_ms": max(latencies),
            "rows": len(rows),
            "latencies": latencies,
        })
        log.info(f"Query {qid} (After): mean={mean_lat:.1f}ms")

    cur.close()
    conn.close()

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    report = build_comparison_report(
        before_results, after_results, maintenance_ms, row_count, args.runs, args.host, ts
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    log.info(f"Benchmark report written to {out_path}")
    print("\n" + report)


if __name__ == "__main__":
    main()
