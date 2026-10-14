# Phase I vs Phase II Lakehouse Reconciliation Report

**Generated:** 2026-10-14T06:42:00Z  
**Verification Engine:** Trino 470 on Apache Iceberg REST/JDBC Catalog  

## 1. High-Level Record Reconciliation

| Data Pipeline Stage / Storage | Engine / Target | Table Name | Row Count | Status |
|---|---|---|---:|:---:|
| Raw CSV Source (Gold Source) | Flat File Parser | `master_endangered_species.csv` | 7,312 | Reference Baseline |
| Phase I Batch ETL | SQLite OLAP DW | `fact_endangered_observations` | 7,312 | Exact Parity |
| Phase I Quarantine | SQLite Quarantine | `quarantine_records` | 0 | Clean Data (0 errors) |
| Phase II Lakehouse (Continuous) | Apache Iceberg / MinIO | `iceberg.lakehouse.fact_observations` | 7,314 | Verified (+2 Live) |
| Phase II Dead-Letter Queue | Apache Iceberg / MinIO | `iceberg.lakehouse.dead_letter_queue` | 6 | Quarantined |

> [!NOTE]
> **Audit Reconciliation Note:**
> - The Phase I CSV full load contains **7,312 clean records**.
> - Phase I SQLite ingested all **7,312 records** with zero quarantined records.
> - Phase II Iceberg ingested all **7,312 backfill records** plus **2 real-time test observations** injected during live streaming verification = **7,314 records**.
> - Phase II Dead-Letter Queue contains **6 records** resulting from deliberate malformed/outlier injection tests during M5.

## 2. Species Category Breakdown Parity

| Animal Category | Phase I SQLite Count | Phase II Iceberg Count | Variance | Parity Status |
|---|---:|---:|---:|:---:|
| Asian Elephant | 2,506 | 2,507 | +1 | +1 (Live Inserted) |
| Bengal tiger | 2,157 | 2,157 | +0 | MATCH (Baseline) |
| Gharial | 424 | 424 | +0 | MATCH (Baseline) |
| Great Indian Bustard | 118 | 118 | +0 | MATCH (Baseline) |
| Lion-Tailed Macaque | 402 | 402 | +0 | MATCH (Baseline) |
| Nilgiri Tahr | 446 | 446 | +0 | MATCH (Baseline) |
| One-Horned Rhinoceros | 745 | 746 | +1 | +1 (Live Inserted) |
| White-rumped Vulture | 459 | 459 | +0 | MATCH (Baseline) |
| leopard | 23 | 23 | +0 | MATCH (Baseline) |
| red panda | 32 | 32 | +0 | MATCH (Baseline) |

## 3. Dead-Letter Queue (DLQ) Quarantine Breakdown

| Failure Reason | Quarantined Count | Action Taken |
|---|---:|---|
| `Null Check Failed: Missing Observation ID` | 2 | Diverted to DLQ, pipeline unblocked |
| `Null Check Failed: Missing Observation Date 'observed_on'` | 1 | Diverted to DLQ, pipeline unblocked |
| `Outlier Detected: Coordinates Lat (99.0), Lon (120.0) outside geographic bounding box` | 1 | Diverted to DLQ, pipeline unblocked |
| `Outlier Detected: Media score (15) outside expected [0, 10] range` | 1 | Diverted to DLQ, pipeline unblocked |
| `Outlier Detected: Media score (-5) outside expected [0, 10] range` | 1 | Diverted to DLQ, pipeline unblocked |

## 4. Architectural Parity & Data Integrity Conclusion

- **Zero Data Loss**: 100% of historical observations were ingested without dropped rows.
- **Business Rules Consistency**: Feature engineering (`lat_normalized`, `lon_normalized`, `season`, `is_weekend`, `iucn_status`, `state_province`) matches Phase I logic to 6 decimal places.
- **Idempotent Upserts**: Deterministic SHA-256 fingerprinting (`record_hash`) prevents double-counting upon stream replay.
- **ACID Guarantees**: Iceberg v2 snapshot isolation prevents partial batch reads by concurrent Trino queries.