import urllib.request
import json
import time

BASE_URL = "http://localhost:8050"

ENDPOINTS = [
    ("HTML UI", "/"),
    ("CSS Stylesheet", "/styles.css"),
    ("JS Application", "/app.js"),
    ("KPIs Endpoint", "/api/analytics/kpis"),
    ("Charts Aggregations", "/api/analytics/charts"),
    ("GIS Map Points", "/api/analytics/map?limit=100"),
    ("OLAP Cube View", "/api/analytics/cube?category=All&season=All"),
    ("CDC Snapshot Stream", "/api/cdc/logs"),
    ("Pipeline Health Status", "/api/pipeline/status"),
]

print("=" * 70)
print(f"{'Endpoint':<25} | {'Status':<6} | {'Latency':<9} | {'Data Payload'}")
print("=" * 70)

latencies = []

for label, path in ENDPOINTS:
    url = BASE_URL + path
    t0 = time.perf_counter()
    req = urllib.request.urlopen(url, timeout=10)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    latencies.append(elapsed_ms)
    status = req.status
    body = req.read()
    content_type = req.headers.get("Content-Type", "")
    
    summary = f"{len(body):,} bytes"
    if "application/json" in content_type:
        try:
            data = json.loads(body.decode("utf-8"))
            if isinstance(data, dict):
                summary = f"dict({len(data)} keys)"
                if "total_observations" in data:
                    summary += f" [total_obs={data['total_observations']}]"
            elif isinstance(data, list):
                summary = f"list({len(data)} items)"
        except Exception:
            pass
            
    print(f"{label:<25} | {status:<6} | {elapsed_ms:>6.1f} ms | {summary}")

print("=" * 70)
print(f"Average Endpoint Latency: {sum(latencies)/len(latencies):.1f} ms")
print(f"Max Endpoint Latency:     {max(latencies):.1f} ms")
print(f"Min Endpoint Latency:     {min(latencies):.1f} ms")
print("=" * 70)
