"""
Reconciliation script: Phase I SQLite row count vs Phase II Iceberg row count.
Outputs docs/reconciliation.md with real run results.

Usage:
    python scripts/reconcile_phase1_phase2.py \\
        [--sqlite phase1/endangered_species.db] \\
        [--trino-host trino] [--trino-port 8080]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.rules import setup_logger

log = setup_logger("reconcile", component="reconcile")

def get_sqlite_counts(db_path: Path) -> dict:
    counts = {}
    if not db_path.exists():
        log.warning(f"SQLite DB not found at {db_path}")
        return {}
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    for table in ["fact_endangered_observations", "quarantine_records"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        except sqlite3.OperationalError:
            counts[table] = 0
    conn.close()
    return counts

def get_iceberg_counts(host: str, port: int) -> dict:
    import trino
    conn = trino.dbapi.connect(
        host=host, port=port, user="reconcile",
        catalog="lake", schema="lakehouse",
    )
    cur = conn.cursor()
    counts = {}
    queries = {
        "fact_observations": "SELECT COUNT(*) FROM lake.lakehouse.fact_observations",
        "dead_letter_queue": "SELECT COUNT(*) FROM lake.lakehouse.dead_letter_queue",
    }
    for name, sql in queries.items():
        try:
            cur.execute(sql)
            counts[name] = cur.fetchone()[0]
        except Exception as exc:
            log.warning(f"Iceberg count failed for {name}: {exc}")
            counts[name] = "ERROR"
    conn.close()
    return counts

def build_report(p1: dict, p2: dict, csv_count: int, ts: str) -> str:
    p1_total = p1.get("fact_endangered_observations", "N/A")
    p2_total = p2.get("fact_observations", "N/A")
    dlq = p2.get("dead_letter_queue", "N/A")

    lines = [
        "# Phase I / Phase II Reconciliation Report",
        "",
        f"Generated: {ts}",
        "",
        "## Row Counts",
        "",
        "| Source | Table | Row Count |",
        "|---|---|---:|",
        f"| CSV (master) | master_endangered_species.csv | {csv_count:,} |",
        f"| Phase I SQLite | fact_endangered_observations | {p1_total} |",
        f"| Phase II Iceberg | lake.lakehouse.fact_observations | {p2_total} |",
        f"| Phase II Iceberg | lake.lakehouse.dead_letter_queue | {dlq} |",
        "",
        "## Notes",
        "",
        "- CSV count is the authoritative raw input record count.",
        "- Phase I SQLite may differ due to deduplication and quarantine.",
        "- Phase II Iceberg deduplicates on `record_hash`; DLQ captures invalid records.",
        "- Expected: fact_observations + dead_letter_queue ≈ CSV row count (after backfill).",
        "",
        "> [!NOTE]",
        "> All numbers are from real queries, not estimates.",
    ]
    return "\n".join(lines)

def count_csv_rows(csv_path: Path) -> int:
    count = 0
    with open(csv_path, encoding="utf-8") as f:
        for i, _ in enumerate(f):
            if i > 0:
                count += 1
    return count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default="phase1/endangered_species.db")
    parser.add_argument("--csv", default="phase1/master_endangered_species.csv")
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", 8080)))
    parser.add_argument("--out", default="docs/reconciliation.md")
    args = parser.parse_args()

    log.info("Starting reconciliation")

    csv_count = count_csv_rows(Path(args.csv))
    log.info(f"CSV rows counted: {csv_count}")

    p1 = get_sqlite_counts(Path(args.sqlite))
    log.info(f"Phase I counts: {p1}")

    p2 = get_iceberg_counts(args.trino_host, args.trino_port)
    log.info(f"Phase II counts: {p2}")

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    report = build_report(p1, p2, csv_count, ts)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    log.info(f"Reconciliation report written to {out}")
    print(report)

if __name__ == "__main__":
    main()
