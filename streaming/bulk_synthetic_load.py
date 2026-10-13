"""
Bulk synthetic data loader for benchmarks.
Generates N synthetic records and publishes them to Kafka.
Used to populate Iceberg with sufficient data for meaningful benchmarks.

Usage:
    python streaming/bulk_synthetic_load.py --count 50000 --tps 500
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.producer import _build_producer, run_synthetic
from common.rules import setup_logger

log = setup_logger("bulk_loader", component="bulk_loader")

def main():
    parser = argparse.ArgumentParser(description="Bulk Synthetic Lakehouse Data Loader")
    parser.add_argument("--count", type=int, default=50_000,
                        help="Number of synthetic records to publish")
    parser.add_argument("--tps", type=float, default=500.0,
                        help="Target records per second")
    parser.add_argument("--bootstrap",
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic",
                        default=os.getenv("KAFKA_TOPIC", "species.observations"))
    args = parser.parse_args()

    log.info(f"Bulk load starting: count={args.count}, tps={args.tps}, topic={args.topic}")
    producer = _build_producer(args.bootstrap)
    run_synthetic(producer, args.topic, args.count, args.tps)
    producer.close()
    log.info(f"Bulk load complete: {args.count} total records sent")

if __name__ == "__main__":
    main()
