"""
Milestone M5 Verification: Dead-Letter Queue & Malformed/Outlier Record Routing.
Publishes deliberate invalid records alongside valid records to Kafka and verifies:
1. Valid records are ingested into fact_observations.
2. Invalid records are quarantined in dead_letter_queue with accurate failure reasons.
"""

import json
import time
import uuid
from kafka import KafkaProducer
import trino

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "species.observations")

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

def build_test_records():
    ts = time.strftime("%Y-%m-%d")
    
    # 2 Valid Records
    valid_1 = {
        "id": 88800001,
        "observed_on": ts,
        "season": "Summer (Mar-May)",
        "quality_grade": "research",
        "media_score": 6,
        "place_guess": "Kaziranga, Assam, India",
        "latitude": 26.58,
        "longitude": 93.17,
        "common_name": "One-Horned Rhinoceros",
        "scientific_name": "Rhinoceros unicornis",
        "animal_category": "One-Horned Rhinoceros",
        "data_quality_flag": "Clean (Research Grade)",
        "gps_precision": "High (<10m)",
        "image_url": "https://example.com/rhino.jpg",
        "sound_url": "",
        "has_audio": 0,
        "has_image": 1,
        "description": "Valid M5 test record 1",
        "country": "India"
    }
    
    valid_2 = {
        "id": 88800002,
        "observed_on": ts,
        "season": "Summer (Mar-May)",
        "quality_grade": "research",
        "media_score": 8,
        "place_guess": "Bandipur, Karnataka, India",
        "latitude": 11.66,
        "longitude": 76.63,
        "common_name": "Asian Elephant",
        "scientific_name": "Elephas maximus",
        "animal_category": "Asian Elephant",
        "data_quality_flag": "Clean (Research Grade)",
        "gps_precision": "High (<10m)",
        "image_url": "https://example.com/elephant.jpg",
        "sound_url": "",
        "has_audio": 0,
        "has_image": 1,
        "description": "Valid M5 test record 2",
        "country": "India"
    }

    # Invalid: Missing ID
    bad_id = dict(valid_1, id=None, description="Bad: Missing ID")

    # Invalid: Missing Date
    bad_date = dict(valid_1, id=88800003, observed_on=None, description="Bad: Missing observed_on")

    # Invalid: Coordinates Outlier (Lat 99.0 outside India bounding box [8, 35])
    bad_coords = dict(valid_1, id=88800004, latitude=99.0, longitude=120.0, description="Bad: Outlier Coordinates")

    # Invalid: Media Score Outlier (> 10)
    bad_score_high = dict(valid_1, id=88800005, media_score=15, description="Bad: Media Score 15")

    # Invalid: Media Score Negative (< 0)
    bad_score_low = dict(valid_1, id=88800006, media_score=-5, description="Bad: Media Score -5")

    return [
        ("VALID", valid_1),
        ("VALID", valid_2),
        ("BAD_ID", bad_id),
        ("BAD_DATE", bad_date),
        ("BAD_COORDS", bad_coords),
        ("BAD_SCORE_HIGH", bad_score_high),
        ("BAD_SCORE_LOW", bad_score_low),
    ]

def main():
    print("=" * 70)
    print("Milestone M5: Publishing Deliberate Test Records to Kafka")
    print("=" * 70)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    records = build_test_records()
    for category, rec in records:
        key = str(rec.get("id") or uuid.uuid4())
        producer.send(TOPIC, key=key, value=rec)
        print(f"Published: {category:<15} | id={rec.get('id')} | desc={rec.get('description')}")

    producer.flush()
    producer.close()
    print("\nPublished 2 valid records and 5 invalid test records.")
    print("Waiting 20 seconds for PySpark Structured Streaming to process...")
    time.sleep(20)

    # Query Trino
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="lakehouse"
    )
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("Dead Letter Queue (iceberg.lakehouse.dead_letter_queue) Verification")
    print("=" * 70)
    cur.execute("""
        SELECT failure_reason, COUNT(*) 
        FROM dead_letter_queue 
        GROUP BY failure_reason
        ORDER BY COUNT(*) DESC
    """)
    dlq_summary = cur.fetchall()
    for reason, cnt in dlq_summary:
        print(f"  Count: {cnt:<3} | Reason: {reason}")

    print("\nDetailed DLQ Records:")
    cur.execute("""
        SELECT id, failure_reason, latitude, longitude, media_score, ingestion_ts
        FROM dead_letter_queue
        ORDER BY ingestion_ts DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        print(f"  id={row[0]} | reason={row[1]} | lat={row[2]} | lon={row[3]} | score={row[4]}")

    print("\n" + "=" * 70)
    print("Fact Observations (iceberg.lakehouse.fact_observations) Verification")
    print("=" * 70)
    cur.execute("""
        SELECT id_original, common_name, state_province, ingestion_ts
        FROM fact_observations
        WHERE id_original IN (88800001, 88800002)
    """)
    valid_ingested = cur.fetchall()
    for row in valid_ingested:
        print(f"  Valid record ingested: id={row[0]} | species={row[1]} | state={row[2]}")

    cur.execute("SELECT COUNT(*) FROM fact_observations")
    total_fact = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dead_letter_queue")
    total_dlq = cur.fetchone()[0]

    print("\n" + "=" * 70)
    print(f"Total Ingested in fact_observations: {total_fact}")
    print(f"Total Quarantined in dead_letter_queue: {total_dlq}")
    print("=" * 70)

if __name__ == "__main__":
    main()
