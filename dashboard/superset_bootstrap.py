"""
Superset bootstrap script:
Configures Trino database connection and imports initial datasets and charts.
"""
from __future__ import annotations

import json
import os
import sys
import time
import requests

from common.rules import setup_logger

log = setup_logger("superset_bootstrap", component="superset_bootstrap")

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN_USERNAME", "admin")
SUPERSET_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")

def get_auth_token():
    url = f"{SUPERSET_URL}/api/v1/security/login"
    payload = {
        "username": SUPERSET_ADMIN,
        "password": SUPERSET_PASS,
        "provider": "db",
        "refresh": True
    }
    for attempt in range(10):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return resp.json()["access_token"]
        except Exception:
            pass
        log.info(f"Waiting for Superset to be ready (attempt {attempt+1}/10)...")
        time.sleep(5)
    return None

def create_trino_database(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    db_payload = {
        "database_name": "Trino Lakehouse",
        "sqlalchemy_uri": f"trino://admin@{TRINO_HOST}:{TRINO_PORT}/lake/lakehouse",
        "expose_in_sqllab": True
    }
    resp = requests.post(f"{SUPERSET_URL}/api/v1/database/", json=db_payload, headers=headers)
    if resp.status_code in (200, 201):
        log.info("Trino database connection registered in Superset")
    else:
        log.warning(f"Database registration response: {resp.status_code} {resp.text}")

def main():
    log.info("Starting Superset bootstrapping")
    token = get_auth_token()
    if not token:
        log.error("Could not authenticate with Superset")
        sys.exit(1)
    create_trino_database(token)
    log.info("Superset bootstrapping completed successfully")

if __name__ == "__main__":
    main()
