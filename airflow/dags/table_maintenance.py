"""
Airflow DAG: table_maintenance
Runs every hour. Compacts small Iceberg files, expires old snapshots,
and collects table statistics for the Trino query planner.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": [os.getenv("ALERT_EMAIL", "admin@lakehouse.local")],
}

def run_trino_query(query: str, **context):
    """Execute a Trino SQL statement via the trino Python client."""
    import trino
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user="airflow",
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema="lakehouse",
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def compact_iceberg(**context):
    """Run Iceberg OPTIMIZE (rewrite_data_files) on fact_observations."""
    run_trino_query(
        "ALTER TABLE iceberg.lakehouse.fact_observations EXECUTE optimize(file_size_threshold => '128MB')",
        **context
    )

def expire_snapshots(**context):
    """Expire Iceberg snapshots older than 3 days."""
    run_trino_query(
        "ALTER TABLE iceberg.lakehouse.fact_observations EXECUTE expire_snapshots(retention_threshold => '3d')",
        **context
    )

def remove_orphan_files(**context):
    """Remove orphan files no longer referenced by active snapshots (Iceberg vacuuming)."""
    run_trino_query(
        "ALTER TABLE iceberg.lakehouse.fact_observations EXECUTE remove_orphan_files(retention_threshold => '1d')",
        **context
    )

def analyze_table(**context):
    """Collect Trino statistics on fact_observations."""
    run_trino_query(
        "ANALYZE iceberg.lakehouse.fact_observations",
        **context
    )

with DAG(
    dag_id="table_maintenance",
    default_args=DEFAULT_ARGS,
    description="Hourly Iceberg compaction, snapshot expiry, vacuuming, and stats collection",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "maintenance"],
) as dag:

    t_compact = PythonOperator(
        task_id="compact_iceberg_files",
        python_callable=compact_iceberg,
    )

    t_expire = PythonOperator(
        task_id="expire_old_snapshots",
        python_callable=expire_snapshots,
    )

    t_orphan = PythonOperator(
        task_id="remove_orphan_files",
        python_callable=remove_orphan_files,
    )

    t_analyze = PythonOperator(
        task_id="analyze_table_statistics",
        python_callable=analyze_table,
    )

    t_compact >> t_expire >> t_orphan >> t_analyze

