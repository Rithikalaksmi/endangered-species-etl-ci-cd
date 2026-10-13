"""
Kafka Producer for Endangered Species Lakehouse.
Modes:
  backfill  - publish all rows from master_endangered_species.csv
  live      - call api_extractor.fetch_api_observations() in a loop
  synthetic - generate synthetic records at a target rate
Usage:
  python -m producer.producer --mode backfill --batch-size 100
  python -m producer.producer --mode live --interval 5
  python -m producer.producer --mode synthetic --count 1000 --tps 50
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.rules import (
    IUCN_MAP,
    INDIAN_STATES,
    setup_logger,
    generate_record_hash,
)

log = setup_logger("producer", component="producer")

SPECIES_LIST = list(IUCN_MAP.keys())

def _build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

def _publish(producer: KafkaProducer, topic: str, record: dict) -> None:
    key = str(record.get("id", uuid.uuid4()))
    future = producer.send(topic, key=key, value=record)
    try:
        future.get(timeout=10)
    except KafkaError as exc:
        log.error(f"Kafka send error: {exc}")

def _make_synthetic_record() -> dict:
    """Generate one plausible-looking observation record."""
    common_name = random.choice(SPECIES_LIST)
    start = date(2020, 1, 1)
    obs_date = start + timedelta(days=random.randint(0, (date.today() - start).days))
    lat = round(random.uniform(8.0, 35.0), 6)
    lon = round(random.uniform(68.0, 97.0), 6)
    state = random.choice(INDIAN_STATES)
    media_score = random.randint(0, 10)
    rec = {
        "id": random.randint(10_000_000, 99_999_999),
        "observed_on": str(obs_date),
        "season": random.choice(["Winter (Dec-Feb)", "Spring (Mar-May)", "Summer (Mar-May)", "Monsoon (Jun-Sep)"]),
        "quality_grade": random.choice(["research", "needs_id", "casual"]),
        "media_score": media_score,
        "place_guess": f"{state}, India",
        "latitude": lat,
        "longitude": lon,
        "common_name": common_name,
        "scientific_name": f"Synthetic {common_name}",
        "animal_category": common_name,
        "data_quality_flag": "Clean (Research Grade)" if media_score >= 5 else "Needs Review",
        "gps_precision": "High (<10m)",
        "image_url": "",
        "sound_url": "",
        "has_audio": 0,
        "has_image": 1 if media_score > 0 else 0,
        "description": "Synthetic record for load testing",
        "country": "India",
    }
    return rec

def run_backfill(producer: KafkaProducer, topic: str, csv_path: Path, batch_size: int) -> None:
    """Publish all rows from the master CSV in order."""
    log.info(f"Starting backfill mode from {csv_path} with batch_size {batch_size}")
    sent = 0
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        batch = []
        for row in reader:
            try:
                row["media_score"] = int(float(row.get("media_score", 0)))
            except (ValueError, TypeError):
                row["media_score"] = 0
            batch.append(row)
            if len(batch) >= batch_size:
                for rec in batch:
                    _publish(producer, topic, rec)
                    sent += 1
                producer.flush()
                log.info(f"Backfill batch flushed: {sent} total records sent")
                batch = []
        for rec in batch:
            _publish(producer, topic, rec)
            sent += 1
        producer.flush()
    log.info(f"Backfill complete: {sent} total records sent")

def run_live(producer: KafkaProducer, topic: str, interval: float) -> None:
    """Poll api_extractor in a loop and publish each observation."""
    try:
        from phase1.api_extractor import fetch_api_observations
    except ImportError:
        from api_extractor import fetch_api_observations

    log.info(f"Starting live mode with interval {interval}s")
    total = 0
    while True:
        observations = fetch_api_observations(count=10)
        for obs in observations:
            _publish(producer, topic, obs)
            total += 1
        producer.flush()
        log.info(f"Live batch published: {total} total observations")
        time.sleep(interval)

def run_synthetic(producer: KafkaProducer, topic: str, count: int, tps: float) -> None:
    """Produce `count` synthetic records at `tps` records/second."""
    log.info(f"Starting synthetic mode: count={count}, tps={tps}")
    delay = 1.0 / tps if tps > 0 else 0
    for i in range(count):
        rec = _make_synthetic_record()
        _publish(producer, topic, rec)
        if delay:
            time.sleep(delay)
        if (i + 1) % 100 == 0:
            producer.flush()
            log.info(f"Synthetic progress: {i + 1}/{count} sent")
    producer.flush()
    log.info(f"Synthetic mode complete: {count} sent")

def main() -> None:
    parser = argparse.ArgumentParser(description="Endangered Species Kafka Producer")
    parser.add_argument("--mode", choices=["backfill", "live", "synthetic"], default="backfill")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "species.observations"))
    parser.add_argument("--csv", default="phase1/master_endangered_species.csv")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between live polls")
    parser.add_argument("--count", type=int, default=1000, help="records for synthetic mode")
    parser.add_argument("--tps", type=float, default=50.0, help="target records/sec for synthetic")
    args = parser.parse_args()

    log.info(f"Producer starting with mode={args.mode}, bootstrap={args.bootstrap}, topic={args.topic}")
    producer = _build_producer(args.bootstrap)

    try:
        if args.mode == "backfill":
            run_backfill(producer, args.topic, Path(args.csv), args.batch_size)
        elif args.mode == "live":
            run_live(producer, args.topic, args.interval)
        elif args.mode == "synthetic":
            run_synthetic(producer, args.topic, args.count, args.tps)
    except KeyboardInterrupt:
        log.info("Producer interrupted by user")
    finally:
        producer.close()
        log.info("Producer closed")

if __name__ == "__main__":
    main()

