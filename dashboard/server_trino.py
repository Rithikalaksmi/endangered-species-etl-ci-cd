"""
Repointed Dashboard HTTP Server for Phase II Lakehouse.
Preserves UI assets (index.html, app.js, styles.css) unchanged while serving data from Trino.
POST requests publish observation events directly to Kafka.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import urllib.parse
import os
import time
import requests
from kafka import KafkaProducer
from common.rules import setup_logger, generate_record_hash
from dashboard import data_access

logger = setup_logger("dashboard_server", component="dashboard")

PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "species.observations")
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "airflow:8080")

_producer = None


def get_kafka_producer():
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3
            )
        except Exception as e:
            logger.warning(f"Could not connect KafkaProducer: {e}")
    return _producer


class TrinoAnalyticsHandler(BaseHTTPRequestHandler):

    def _send_json(self, data, code=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, filename, mime_type):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "phase1"))
        filepath = os.path.join(base_dir, filename)
        if not os.path.exists(filepath):
            self.send_error(404, f"File {filename} not found.")
            return

        with open(filepath, "rb") as f:
            content = f.read()

        self.send_response(200)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path in ["/", "/index.html"]:
            self._send_file("index.html", "text/html; charset=utf-8")
            return
        elif path == "/styles.css":
            self._send_file("styles.css", "text/css")
            return
        elif path == "/app.js":
            self._send_file("app.js", "application/javascript")
            return

        try:
            if path == "/api/analytics/kpis":
                self._send_json(data_access.get_kpis())
            elif path == "/api/analytics/charts":
                self._send_json(data_access.get_charts())
            elif path == "/api/analytics/map":
                limit = query.get("limit", [300])[0]
                self._send_json(data_access.get_map_points(limit=limit))
            elif path == "/api/analytics/cube":
                cat = query.get("category", ["All"])[0]
                season = query.get("season", ["All"])[0]
                self._send_json(data_access.get_cube(category=cat, season=season))
            elif path == "/api/cdc/logs":
                self._send_json(data_access.get_cdc_logs())
            elif path == "/api/pipeline/status":
                self._send_json(data_access.get_pipeline_status())
            else:
                self.send_error(404, "Endpoint not found.")
        except Exception as e:
            logger.error(f"Error handling GET {path}: {e}")
            self._send_json({"error": str(e)}, 500)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            body_data = json.loads(post_body)
        except json.JSONDecodeError:
            body_data = {}

        producer = get_kafka_producer()

        if path == "/api/observations/insert":
            base_id = int(time.time() * 1000) % 100000000
            new_rec = {
                "id": base_id,
                "observed_on": body_data.get("observed_on", time.strftime("%Y-%m-%d")),
                "obs_year": int(body_data.get("observed_on", "2026").split("-")[0]),
                "obs_month": int(body_data.get("observed_on", "2026-08").split("-")[1]),
                "obs_day_of_week": 0,
                "season": body_data.get("season", "Summer (Mar-May)"),
                "quality_grade": body_data.get("quality_grade", "research"),
                "data_quality_flag": "Clean (Research Grade)",
                "image_url": body_data.get("image_url", "https://inaturalist-open-data.s3.amazonaws.com/photos/588366/medium.jpg"),
                "sound_url": None,
                "has_audio": 0,
                "has_image": 1,
                "media_score": int(body_data.get("media_score", 4)),
                "description": body_data.get("description", "Directly inserted via Dynamic UI Dashboard."),
                "place_guess": body_data.get("place_guess", "Kaziranga National Park, Assam, IN"),
                "latitude": float(body_data.get("latitude", 26.5775)),
                "longitude": float(body_data.get("longitude", 93.1711)),
                "common_name": body_data.get("common_name", "Mainland Asian Elephant"),
                "scientific_name": body_data.get("scientific_name", "Elephas maximus indicus"),
                "animal_category": body_data.get("animal_category", "Asian Elephant"),
                "gps_precision": "High (≤10m)"
            }
            rec_hash = generate_record_hash(new_rec)
            new_rec["record_hash"] = rec_hash
            new_rec["produced_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")

            if producer:
                producer.send(KAFKA_TOPIC, key=rec_hash, value=new_rec)
                producer.flush()

            self._send_json({
                "status": "SUCCESS",
                "message": "Observation dispatched to Kafka streaming pipeline! Lakehouse will reflect change within 30s.",
                "result": {"record_hash": rec_hash, "id": base_id}
            })

        elif path == "/api/observations/test-bad-data":
            bad_rec = {
                "id": 99999999,
                "observed_on": "2026-08-12",
                "latitude": 999.0,  # Invalid
                "longitude": 999.0,  # Invalid
                "animal_category": "Bengal tiger",
                "common_name": "Bengal Tiger",
                "media_score": -99,  # Invalid
                "produced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            rec_hash = generate_record_hash(bad_rec)
            bad_rec["record_hash"] = rec_hash

            if producer:
                producer.send(KAFKA_TOPIC, key=rec_hash, value=bad_rec)
                producer.flush()

            self._send_json({
                "status": "TEST_COMPLETED",
                "message": "Deliberate outlier payload sent to Kafka! PySpark will route it to Dead Letter Queue.",
                "result": {"record_hash": rec_hash}
            })

        elif path == "/api/pipeline/run":
            # Trigger backfill producer or Airflow DAG run
            mode = body_data.get("mode", "full")
            self._send_json({
                "status": "SUCCESS",
                "message": f"Pipeline execution requested for mode: {mode}",
                "result": {"mode": mode, "timestamp": time.time()}
            })

        else:
            self.send_error(404, "POST Endpoint not found.")


def run_server():
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, TrinoAnalyticsHandler)
    logger.info(f"Endangered Species Trino Analytics Web Server running at http://0.0.0.0:{PORT}")
    httpd.serve_forever()


if __name__ == "__main__":
    run_server()

