# Architectural and Configuration Deviations

This document logs all deviations from initial specifications along with justifications and operational impacts as required by Rule 10.

| Item | Specification Default | Implemented Solution | Justification |
|---|---|---|---|
| MinIO Container Registry | `minio/minio` on Docker Hub | `cgr.dev/chainguard/minio` (and `cgr.dev/chainguard/minio-client`) | MinIO removed official open-source community edition image tags from Docker Hub (`pull access denied`). Chainguard provides enterprise-hardened, multi-arch open-source binaries for MinIO Server and MinIO Client with identical CLI syntax and S3 protocol compliance. |
| Trino JVM Heap (Lite Mode) | Generic default | `-Xmx1536M` | Docker Desktop host allocation on standard workstations is often constrained to ~4 GB RAM. Trino's heap is configured to 1.5 GB in lite mode to prevent OOM errors while delivering full Iceberg query capabilities. |
| Kafka Mode | KRaft | Apache Kafka 3.7.2 in KRaft Mode (`apache/kafka:3.7.2`) | Zero Zookeeper footprint, fast startup, and reduced memory usage (~256 MB heap). |
| Kafka Offset Tool | `kafka.tools.GetOffsetShell` | `org.apache.kafka.tools.GetOffsetShell` | In Apache Kafka 3.7+, legacy CLI tools were moved under `org.apache.kafka.tools.*`. Running with the full package name preserves exact functionality. |
| Trino 470 Memory Setting | `query.max-total-memory-per-node` | Removed | Defunct in Trino 470; Trino now uses `query.max-memory-per-node` directly for total query memory planning. |
| Spark Iceberg Writer Tuning | Generic Iceberg append with fanout | Coalesce to 1 writer, sort by partition keys `(year, month, iucn_status)`, disable fanout (`fanout-enabled=false`), 8MB row groups | In memory-constrained environments (lite mode 1.2 GB RAM), Iceberg's default fanout writer opens simultaneous Parquet writers for 100+ partition combinations in historical backfills, exhausting JVM heap. Sorting and single-file clustering maintains minimal heap usage without data loss. |
| Trino Open Partition Writer Limit | Default 100 open writers per task | `iceberg.max-partitions-per-writer=1000` | Historical backfills across 60+ years produce 101 distinct partition combinations. Increasing this property allows Trino's `ALTER TABLE ... EXECUTE optimize()` compaction to consolidate all partitions without throwing `ICEBERG_TOO_MANY_OPEN_PARTITIONS`. |
| Trino Host Port Mapping | Host port 8080 | Host port 8082 (`8082:8080`) | On Windows with WSL2, `wslrelay` grabs `127.0.0.1:8080` on the host loopback, causing browser connection drops ("empty reply from server"). Remapping to 8082 ensures conflict-free browser access to the Trino Web UI and JDBC while keeping internal container networking on `trino:8080`. |
| MailHog Host Port Mapping | Host port 8025 | Host port 8026 (`8026:8025`) | Same Windows WSL2 port conflict on port 8025. Remapped to 8026 for reliable browser access to MailHog UI. |
| Dashboard Real-Time Write Path | Kafka-only publish | Dual publish: Kafka topic `species.observations` + direct Iceberg DML insert | In the pure streaming architecture, Kafka microbatch flush latency (15s) caused immediate dashboard refreshes to miss newly submitted records. The dashboard server now performs dual publishing: sending events to Kafka and immediately committing to Iceberg via Trino, giving immediate UI updates for observation inserts, DLQ tests, and pipeline execution logs. |


