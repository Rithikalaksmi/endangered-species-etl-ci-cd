"""
Initializes Lakehouse Schema, Iceberg Tables, and Views via Trino.
"""

import os
import time
import trino
from common.rules import setup_logger

logger = setup_logger("init_lakehouse", component="setup")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
SQL_FILE = os.path.join(os.path.dirname(__file__), "..", "sql", "create_tables.sql")


def wait_for_trino(max_retries=30, delay_sec=2):
    """Waits until Trino coordinator is healthy and accepting connections."""
    logger.info(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    for attempt in range(1, max_retries + 1):
        try:
            conn = trino.dbapi.connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user=TRINO_USER
            )
            cur = conn.cursor()
            cur.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            if "iceberg" in catalogs:
                logger.info(f"Connected to Trino successfully. Available catalogs: {catalogs}")
                return True
            else:
                logger.info(f"Trino is up, waiting for 'iceberg' catalog to load (attempt {attempt}/{max_retries}). Catalogs: {catalogs}")
        except Exception as e:
            logger.info(f"Waiting for Trino coordinator... (attempt {attempt}/{max_retries}): {e}")
        time.sleep(delay_sec)
    raise RuntimeError(f"Trino at {TRINO_HOST}:{TRINO_PORT} did not become ready in time.")


def execute_ddl(sql_file=SQL_FILE):
    """Executes DDL statements from the provided SQL file."""
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Split on semicolon, ignoring comments and blank statements
    statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER
    )
    cur = conn.cursor()

    for idx, stmt in enumerate(statements, 1):
        first_line = stmt.strip().split("\n")[0]
        logger.info(f"Executing DDL [{idx}/{len(statements)}]: {first_line}")
        try:
            cur.execute(stmt)
            # Fetch results if any
            if cur.description:
                cur.fetchall()
            logger.info(f"DDL [{idx}/{len(statements)}] completed successfully.")
        except Exception as e:
            logger.error(f"Failed executing DDL: {first_line} | Error: {e}")
            raise

    cur.close()
    conn.close()
    logger.info("All Lakehouse Iceberg tables and analytical views initialized successfully.")


if __name__ == "__main__":
    wait_for_trino()
    execute_ddl()

