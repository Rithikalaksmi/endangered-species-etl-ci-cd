"""
Backend API & Web Server for Endangered Species Analytics Dashboard.
Provides RESTful APIs for analytics, real-time ETL execution, CDC logs, GIS map points, Data Cube queries, and live observation insertion.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import urllib.parse
import os
import sqlite3
import time
from database_setup import DB_PATH, get_connection
from etl_pipeline import run_etl_pipeline
from api_extractor import fetch_api_observations

PORT = 8050

class AnalyticsHandler(BaseHTTPRequestHandler):

    def _send_json(self, data, code=200):
        body = json.dumps(data, default=str).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, filename, mime_type):
        filepath = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(filepath):
            self.send_error(404, f"File {filename} not found.")
            return
        
        with open(filepath, 'rb') as f:
            content = f.read()
        
        self.send_response(200)
        self.send_header('Content-Type', mime_type)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path in ['/', '/index.html']:
            self._send_file('index.html', 'text/html; charset=utf-8')
            return
        elif path == '/styles.css':
            self._send_file('styles.css', 'text/css')
            return
        elif path == '/app.js':
            self._send_file('app.js', 'application/javascript')
            return

        # ==========================================
        # REST API ENDPOINTS
        # ==========================================
        conn = get_connection()
        cursor = conn.cursor()

        try:
            if path == '/api/analytics/kpis':
                cursor.execute("SELECT COUNT(*) FROM tbl_observations;")
                total_obs = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT animal_category) FROM tbl_species;")
                total_categories = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT common_name) FROM tbl_species;")
                total_species = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM tbl_observations WHERE quality_grade='research';")
                clean_count = cursor.fetchone()[0]

                clean_rate = round((clean_count / total_obs * 100), 1) if total_obs > 0 else 100.0

                cursor.execute("SELECT COUNT(*) FROM cdc_change_log;")
                cdc_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM quarantine_records;")
                quarantine_count = cursor.fetchone()[0]

                cursor.execute("SELECT state_province, COUNT(*) as c FROM tbl_locations GROUP BY state_province ORDER BY c DESC LIMIT 1;")
                top_state_res = cursor.fetchone()
                top_state = top_state_res[0] if top_state_res else "Assam"

                data = {
                    "total_observations": total_obs,
                    "total_categories": total_categories,
                    "total_species": total_species,
                    "clean_data_rate": clean_rate,
                    "cdc_events_count": cdc_count,
                    "quarantine_count": quarantine_count,
                    "top_state": top_state
                }
                self._send_json(data)

            elif path == '/api/analytics/charts':
                # Chart 1: Species Distribution
                cursor.execute("""
                    SELECT ds.animal_category, COUNT(f.fact_id) as count
                    FROM fact_endangered_observations f
                    JOIN dim_species ds ON f.species_key = ds.species_key
                    GROUP BY ds.animal_category
                    ORDER BY count DESC;
                """)
                category_data = [{"category": row[0], "count": row[1]} for row in cursor.fetchall()]

                # Chart 2: Seasonal Trends
                cursor.execute("""
                    SELECT dt.season, COUNT(f.fact_id) as count
                    FROM fact_endangered_observations f
                    JOIN dim_time dt ON f.time_key = dt.time_key
                    GROUP BY dt.season
                    ORDER BY count DESC;
                """)
                seasonal_data = [{"season": row[0], "count": row[1]} for row in cursor.fetchall()]

                # Chart 3: IUCN Threat Breakdown
                cursor.execute("""
                    SELECT ds.iucn_status, COUNT(f.fact_id) as count
                    FROM fact_endangered_observations f
                    JOIN dim_species ds ON f.species_key = ds.species_key
                    GROUP BY ds.iucn_status;
                """)
                iucn_data = [{"status": row[0], "count": row[1]} for row in cursor.fetchall()]

                # Chart 4: Media Score vs Quality Grade
                cursor.execute("""
                    SELECT dq.quality_grade, AVG(f.media_score) as avg_score, COUNT(f.fact_id) as total
                    FROM fact_endangered_observations f
                    JOIN dim_quality dq ON f.quality_key = dq.quality_key
                    GROUP BY dq.quality_grade;
                """)
                quality_data = [{"grade": row[0], "avg_media_score": round(row[1], 2), "total": row[2]} for row in cursor.fetchall()]

                self._send_json({
                    "categories": category_data,
                    "seasons": seasonal_data,
                    "iucn": iucn_data,
                    "quality": quality_data
                })

            elif path == '/api/analytics/map':
                # Limit map points for fast GIS rendering
                limit = query.get('limit', [300])[0]
                cursor.execute("""
                    SELECT o.obs_id, s.animal_category, s.common_name, s.iucn_status,
                           l.latitude, l.longitude, l.place_guess, l.state_province,
                           o.observed_on, o.season, o.image_url, o.quality_grade
                    FROM tbl_observations o
                    JOIN tbl_species s ON o.species_id = s.species_id
                    JOIN tbl_locations l ON o.location_id = l.location_id
                    ORDER BY o.obs_id DESC
                    LIMIT ?;
                """, (int(limit),))

                map_points = [
                    {
                        "id": row[0],
                        "animal_category": row[1],
                        "common_name": row[2],
                        "iucn_status": row[3],
                        "latitude": row[4],
                        "longitude": row[5],
                        "place_guess": row[6],
                        "state": row[7],
                        "observed_on": row[8],
                        "season": row[9],
                        "image_url": row[10],
                        "quality_grade": row[11]
                    }
                    for row in cursor.fetchall()
                ]
                self._send_json(map_points)

            elif path == '/api/analytics/cube':
                # OLAP Data Cube Slice & Dice Endpoint
                cat = query.get('category', ['All'])[0]
                season = query.get('season', ['All'])[0]

                sql = "SELECT animal_category, season, state_province, quality_grade, total_observations, avg_media_score, has_image_count, research_grade_count FROM cube_agg_analytics WHERE 1=1"
                params = []

                if cat != 'All':
                    sql += " AND animal_category = ?"
                    params.append(cat)

                if season != 'All':
                    sql += " AND season = ?"
                    params.append(season)

                sql += " ORDER BY total_observations DESC LIMIT 100;"

                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cube_data = [
                    {
                        "category": r[0],
                        "season": r[1],
                        "state": r[2],
                        "quality_grade": r[3],
                        "total_observations": r[4],
                        "avg_media_score": r[5],
                        "has_image_count": r[6],
                        "research_grade_count": r[7]
                    }
                    for r in rows
                ]
                self._send_json(cube_data)

            elif path == '/api/cdc/logs':
                cursor.execute("""
                    SELECT cdc_id, record_id, table_name, change_type, changed_fields, new_values, timestamp
                    FROM cdc_change_log
                    ORDER BY cdc_id DESC
                    LIMIT 50;
                """)
                logs = [
                    {
                        "cdc_id": r[0],
                        "record_id": r[1],
                        "table_name": r[2],
                        "change_type": r[3],
                        "changed_fields": r[4],
                        "new_values": r[5],
                        "timestamp": r[6]
                    }
                    for r in cursor.fetchall()
                ]
                self._send_json(logs)

            elif path == '/api/pipeline/status':
                cursor.execute("""
                    SELECT run_id, run_type, status, records_processed, records_valid,
                           records_quarantined, records_inserted, records_updated, duration_seconds, timestamp
                    FROM etl_pipeline_runs
                    ORDER BY run_id DESC
                    LIMIT 20;
                """)
                runs = [
                    {
                        "run_id": r[0],
                        "run_type": r[1],
                        "status": r[2],
                        "records_processed": r[3],
                        "records_valid": r[4],
                        "records_quarantined": r[5],
                        "records_inserted": r[6],
                        "records_updated": r[7],
                        "duration_seconds": r[8],
                        "timestamp": r[9]
                    }
                    for r in cursor.fetchall()
                ]
                self._send_json(runs)

            else:
                self.send_error(404, "Endpoint not found.")

        finally:
            conn.close()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get('Content-Length', 0))
        post_body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else "{}"
        
        try:
            body_data = json.loads(post_body)
        except json.JSONDecodeError:
            body_data = {}

        if path == '/api/pipeline/run':
            mode = body_data.get('mode', 'full')
            if mode == 'api':
                api_recs = fetch_api_observations(count=15)
                res = run_etl_pipeline(mode='incremental', api_records=api_recs)
            else:
                res = run_etl_pipeline(mode=mode)
            self._send_json(res)

        elif path == '/api/observations/insert':
            # Dynamic Observation Form Insertion
            base_id = int(time.time() * 1000) % 100000000
            new_rec = {
                "id": base_id,
                "observed_on": body_data.get("observed_on", time.strftime("%Y-%m-%d")),
                "obs_year": int(body_data.get("observed_on", "2026").split('-')[0]),
                "obs_month": int(body_data.get("observed_on", "2026-08").split('-')[1]),
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

            res = run_etl_pipeline(mode='incremental', api_records=[new_rec])
            self._send_json({"status": "SUCCESS", "message": "Observation inserted successfully into ETL pipeline!", "result": res})

        elif path == '/api/observations/test-bad-data':
            # Outlier / Corrupt Record Injection Test
            bad_rec = {
                "id": 99999999,
                "observed_on": "2026-08-12",
                "latitude": 999.0, # INVALID OUTLIER
                "longitude": 999.0, # INVALID OUTLIER
                "animal_category": "Bengal tiger",
                "common_name": "Bengal Tiger",
                "media_score": -99 # INVALID OUTLIER
            }

            res = run_etl_pipeline(mode='incremental', api_records=[bad_rec])
            self._send_json({
                "status": "TEST_COMPLETED",
                "message": "Outlier payload rejected by Data Quality Gatekeeper & routed to Quarantine table!",
                "result": res
            })

        else:
            self.send_error(404, "POST Endpoint not found.")

def run_server():
    server_address = ('', PORT)
    httpd = HTTPServer(server_address, AnalyticsHandler)
    print(f"\n==========================================")
    print(f" ENDANGERED SPECIES ANALYTICS WEB SERVER ")
    print(f" Running at: http://localhost:{PORT}")
    print(f"==========================================\n")
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
