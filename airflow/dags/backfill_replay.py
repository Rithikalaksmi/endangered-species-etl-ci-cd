"""
Airflow DAG: backfill_replay
Manually-triggered DAG that replays the master CSV through Kafka.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [os.getenv("ALERT_EMAIL", "admin@lakehouse.local")],
}

def check_row_counts(**context):
    """Query Iceberg and report current row counts."""
    import trino
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user="airflow",
        catalog="lake",
        schema="lakehouse",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lake.lakehouse.fact_observations")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    context['task_instance'].xcom_push(key='total_rows', value=count)
    return f"Current fact_observations count: {count}"

with DAG(
    dag_id="backfill_replay",
    default_args=DEFAULT_ARGS,
    description="Manual backfill: replay CSV through Kafka -> Iceberg",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "backfill"],
) as dag:

    t_before = PythonOperator(
        task_id="count_rows_before_backfill",
        python_callable=check_row_counts,
    )

    t_produce = BashOperator(
        task_id="run_kafka_producer_backfill",
        bash_command=(
            "python -m producer.producer "
            "--mode backfill "
            "--csv /opt/spark/work-dir/phase1/master_endangered_species.csv "
            "--batch-size 200"
        ),
    )

    t_wait = BashOperator(
        task_id="wait_for_streaming_to_flush",
        bash_command="sleep 30",
    )

    t_after = PythonOperator(
        task_id="count_rows_after_backfill",
        python_callable=check_row_counts,
    )

    t_before >> t_produce >> t_wait >> t_after
