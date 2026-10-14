"""
Airflow DAG: pipeline_health
Runs every 5 minutes. Checks:
  1. Trino connectivity
  2. Data freshness in Iceberg
  3. Spark UI reachable
Sends alert email if any check fails.
"""
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.python import PythonOperator

SMTP_HOST = os.getenv("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "admin@lakehouse.local")

def send_alert_email(subject: str, message: str):
    """Dispatches failure alerts to local MailHog SMTP server."""
    try:
        msg = MIMEText(message)
        msg["Subject"] = f"[CRITICAL LAKEHOUSE ALERT] {subject}"
        msg["From"] = "pipeline-health@lakehouse.local"
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as server:
            server.send_message(msg)
    except Exception as exc:
        pass

def on_task_failure(context):
    """Callback when a health check task fails."""
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    exception = context.get("exception", "Health check constraint violated.")
    send_alert_email(
        subject=f"Health Check Failed: {task_id}",
        message=f"Pipeline health check '{task_id}' has failed!\nError details:\n{exception}\nTimestamp: {datetime.utcnow().isoformat()}Z"
    )

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": True,
    "email": [ALERT_EMAIL],
    "on_failure_callback": on_task_failure,
}

def check_trino_connectivity(**context):
    """Verify Trino is reachable and can query the Iceberg catalog."""
    import trino
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user="airflow",
    )
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    catalogs = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    if "iceberg" not in catalogs:
        raise ValueError(f"Iceberg catalog 'iceberg' not found in Trino. Found catalogs: {catalogs}")
    return f"Trino OK, catalogs: {catalogs}"

def check_data_freshness(**context):
    """Verify observations table freshness."""
    import trino
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user="airflow",
        catalog="iceberg",
        schema="lakehouse",
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM iceberg.lakehouse.fact_observations
    """)
    total_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    if total_count == 0:
        raise ValueError("Iceberg fact_observations table is empty!")
    return f"Total rows in fact_observations: {total_count}"

def check_spark_ui(**context):
    """Check Spark UI is reachable."""
    import urllib.request
    spark_host = os.getenv("SPARK_UI_HOST", "spark")
    spark_ui_port = int(os.getenv("SPARK_UI_PORT", "4040"))
    url = f"http://{spark_host}:{spark_ui_port}/api/v1/applications"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status == 200:
                return f"Spark UI reachable at {url}"
    except Exception as exc:
        return f"Spark UI not reachable ({exc}) — may be idle"

with DAG(
    dag_id="pipeline_health",
    default_args=DEFAULT_ARGS,
    description="5-min pipeline health checks (Trino, Spark, freshness)",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "health"],
) as dag:

    t_trino = PythonOperator(
        task_id="check_trino_connectivity",
        python_callable=check_trino_connectivity,
    )

    t_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    t_spark = PythonOperator(
        task_id="check_spark_ui",
        python_callable=check_spark_ui,
    )

    [t_trino, t_freshness, t_spark]

