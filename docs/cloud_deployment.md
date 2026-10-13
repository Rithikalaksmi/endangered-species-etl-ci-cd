# Cloud Deployment Guide: Lakehouse in Production

**Course**: 22MDCEL10 Data Engineering Laboratory  
**Project**: Phase II Production-Ready Real-Time Lakehouse

---

## 1. Cloud Architecture Mapping

The local Docker compose stack maps directly to managed enterprise cloud services:

| Local Component | AWS Architecture | GCP Architecture | Azure Architecture |
|---|---|---|---|
| **Storage** (MinIO S3) | Amazon S3 | Google Cloud Storage (GCS) | Azure Data Lake Storage Gen2 (ADLS) |
| **Catalog** (Postgres JDBC) | AWS Glue Data Catalog / Amazon RDS | BigLake Metastore / Cloud SQL | Azure Purview / Azure Database for PostgreSQL |
| **Streaming** (Kafka) | Amazon MSK (Managed Kafka) | Google Cloud Pub/Sub / Managed Kafka | Azure Event Hubs (Kafka endpoint) |
| **Processing** (PySpark) | Amazon EMR Serverless / AWS Glue | Dataproc Serverless | Azure Synapse Spark / Databricks |
| **Query Engine** (Trino) | Amazon Athena (v3 engine) / Starburst Galaxy | Trino on GKE / BigQuery (via BigLake) | Trino on AKS / Azure Synapse Serverless SQL |
| **Orchestration** (Airflow)| Amazon MWAA (Managed Workflows) | Cloud Composer | Azure Data Factory Managed Airflow |
| **BI & Viz** (Superset) | Amazon QuickSight / Preset.io | Looker Studio / Looker | Power BI / Managed Superset |

---

## 2. Production Hardening Steps

### 2.1 Security & Networking
- Run all compute clusters inside private VPC subnets.
- Use IAM roles and service accounts with least privilege rather than static root credentials (`AWS_ACCESS_KEY_ID`).
- Enable SSE-S3 or SSE-KMS encryption at rest for S3 buckets and TLS 1.3 in transit.

### 2.2 Sizing & Auto-scaling
- **Streaming Spark**: Enable dynamic allocation with min 2, max 10 executors.
- **Trino Cluster**: Separate coordinator node from worker nodes. Set autoscaling policy based on CPU utilization and query queue depth.
- **Iceberg Table Maintenance**: Schedule automatic compaction after every 10 streaming commits or once per hour during off-peak windows.
