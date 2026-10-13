#!/usr/bin/env bash
# simulate_failure.sh
# Intentionally kill the Spark streaming process or container
# to demonstrate pipeline health detection and alerting.

set -e

echo "=== Simulating Spark Streaming Failure ==="
echo "Stopping lakehouse-spark container..."

docker stop lakehouse-spark

echo "Spark container stopped."
echo "Airflow pipeline_health DAG should detect this failure within 5 minutes."
echo "Check MailHog UI at http://localhost:8025 to view sent alert emails."
echo ""
echo "To restore the pipeline:"
echo "  docker start lakehouse-spark"

