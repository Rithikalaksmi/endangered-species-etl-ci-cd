# Architectural and Configuration Deviations

This document logs all deviations from initial specifications along with justifications and operational impacts as required by Rule 10.

| Item | Specification Default | Implemented Solution | Justification |
|---|---|---|---|
| MinIO Container Registry | `minio/minio` on Docker Hub | `cgr.dev/chainguard/minio` (and `cgr.dev/chainguard/minio-client`) | MinIO removed official open-source community edition image tags from Docker Hub (`pull access denied`). Chainguard provides enterprise-hardened, multi-arch open-source binaries for MinIO Server and MinIO Client with identical CLI syntax and S3 protocol compliance. |
| Trino JVM Heap (Lite Mode) | Generic default | `-Xmx1536M` | Docker Desktop host allocation on standard workstations is often constrained to ~4 GB RAM. Trino's heap is configured to 1.5 GB in lite mode to prevent OOM errors while delivering full Iceberg query capabilities. |
| Kafka Mode | KRaft | Apache Kafka 3.7.2 in KRaft Mode (`apache/kafka:3.7.2`) | Zero Zookeeper footprint, fast startup, and reduced memory usage (~256 MB heap). |

