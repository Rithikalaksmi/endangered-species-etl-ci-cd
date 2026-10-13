# Real-Time Lakehouse for Endangered Species Analytics (Phase II)

**Course**: 22MDCEL10 Data Engineering Laboratory  
**Project Phase II**: Production-Ready Real-Time Lakehouse Architecture

---

## 1. Executive Summary

This project upgrades the Phase I batch ETL pipeline (SQLite, star schema, single-node dashboard) into an enterprise-grade, continuous real-time Lakehouse:

```
Producer -> Kafka -> PySpark Structured Streaming -> Iceberg on MinIO -> Trino -> Superset + Phase I Dashboard
                                  |
                                  +-> Dead-Letter Queue (bad records)
                Airflow: table maintenance + health checks + alerts
```

### Key Capabilities
- **End-to-End Real-Time Ingestion**: New observations published to Apache Kafka are processed via PySpark Structured Streaming and served via Trino within ~15–30 seconds.
- **ACID & Merge-on-Read**: Apache Iceberg table format v2 with deterministic deduplication (`record_hash` SHA-256) and partition pruning on `month(observed_on)` and `iucn_status`.
- **Zero Data Loss**: Schema validation diverts malformed and out-of-boundary records to an Iceberg Dead-Letter Queue with failure diagnostics.
- **Automated Lifecycle**: Apache Airflow DAGs perform hourly file compaction (`OPTIMIZE`), snapshot pruning, and 5-minute health monitoring with alerting.
- **Full Phase I Parity**: All 28 Phase I tests and 20 Phase II parity tests pass (48/48 unit tests passing).

---

## 2. Quickstart

### Prerequisites
- Docker Desktop (with WSL2 enabled on Windows)
- Python 3.11+
- Git

### One-Command Launch (Lite Mode for Laptops)
```bash
# 1. Initialize environment
cp .env.example .env

# 2. Start the lakehouse stack in resource-efficient lite mode
make up-lite

# 3. Run demonstration backfill and verification
make demo
```

---

## 3. Services and Ports

| Service | Port | Description |
|---|---|---|
| **Phase I Dashboard** | `http://localhost:8050` | Trino-backed real-time interactive map and analytics |
| **Trino Query Engine** | `http://localhost:8080` | Distributed ANSI-SQL engine |
| **MinIO Console** | `http://localhost:9001` | S3 Object storage UI (`minioadmin` / `minioadmin`) |
| **MinIO S3 API** | `http://localhost:9000` | S3 API endpoint |
| **Kafka Broker** | `localhost:9092` | Apache Kafka event streaming broker |
| **Kafka UI** *(optional)* | `http://localhost:8085` | Kafka topic and consumer monitoring |
| **Apache Airflow** | `http://localhost:8088` | Pipeline orchestration and health checks |
| **MailHog UI** | `http://localhost:8025` | Local SMTP capture UI for alert emails |

---

## 4. Testing & Verification

Run the test suite:
```bash
python -m pytest -v
```
All 48 tests will execute:
- 28 Phase I batch extraction, transformation, and validation tests
- 20 Phase II parity tests verifying identical feature engineering and hash determinism

---

## 5. Documentation Directory

- [Architecture Design & Decisions](docs/design.md)
- [Data Lineage & Column Mappings](docs/lineage.md)
- [Viva Voce Defense Notes & Cheat Sheet](docs/viva_notes.md)
- [Live Demonstration Walkthrough](docs/demo_script.md)
- [Cloud Deployment Strategy](docs/cloud_deployment.md)
- [Architectural Deviations](docs/deviations.md)
