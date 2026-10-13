"""
Single source of business rules and transformations for Endangered Species Lakehouse.
Shared between Phase I reconciliation, Kafka producer, PySpark Structured Streaming, and test suites.
"""

import hashlib
import json
import logging
from datetime import datetime
import pandas as pd

# IUCN Threat Classification
IUCN_MAP = {
    "Asian Elephant": "Endangered (EN)",
    "Bengal tiger": "Endangered (EN)",
    "One-Horned Rhinoceros": "Vulnerable (VU)",
    "White-rumped Vulture": "Critically Endangered (CR)",
    "Nilgiri Tahr": "Endangered (EN)",
    "Gharial": "Critically Endangered (CR)",
    "Lion-Tailed Macaque": "Endangered (EN)",
    "Great Indian Bustard": "Critically Endangered (CR)",
    "red panda": "Endangered (EN)",
    "leopard": "Vulnerable (VU)"
}

INDIAN_STATES = [
    "Assam", "Tamil Nadu", "Kerala", "Uttarakhand", "Karnataka",
    "West Bengal", "Arunachal Pradesh", "Madhya Pradesh", "Maharashtra",
    "Rajasthan", "Uttar Pradesh", "Chhattisgarh", "Ladakh", "Himachal Pradesh",
    "Meghalaya", "Odisha", "Tripura"
]

# Validation bounds
LAT_MIN = 6.0
LAT_MAX = 38.0
LON_MIN = 68.0
LON_MAX = 98.0
MEDIA_SCORE_MIN = 0
MEDIA_SCORE_MAX = 10

# Normalization constants
LAT_NORM_MIN = 8.0
LAT_NORM_MAX = 35.0
LON_NORM_MIN = 68.0
LON_NORM_MAX = 97.0

# Exact failure reasons matching Phase I
REASON_MISSING_ID = "Null Check Failed: Missing Observation ID"
REASON_MISSING_DATE = "Null Check Failed: Missing Observation Date 'observed_on'"
REASON_MALFORMED_JSON = "Schema Mismatch: malformed JSON"
REASON_SCHEMA_LATLON = "Schema Mismatch: Invalid or non-numeric latitude/longitude format"
REASON_SCHEMA_MEDIA = "Schema Mismatch: Invalid non-integer media score format"


def get_reason_bbox(lat, lon):
    return f"Outlier Detected: Coordinates Lat ({lat}), Lon ({lon}) outside geographic bounding box"


def get_reason_media(score):
    return f"Outlier Detected: Media score ({score}) outside expected [0, 10] range"


def generate_record_hash(rec):
    """
    Computes deterministic SHA-256 fingerprint hash for a record.
    raw_str = '{id}|{observed_on}|{latitude}|{longitude}|{common_name}'
    """
    raw_str = f"{rec.get('id')}|{rec.get('observed_on')}|{rec.get('latitude')}|{rec.get('longitude')}|{rec.get('common_name')}"
    return hashlib.sha256(raw_str.encode('utf-8')).hexdigest()


def extract_state(place_str):
    """
    Extracts Indian State/Province name from location description string.
    """
    if not place_str or pd.isna(place_str):
        return "Unknown Region"
    
    for state in INDIAN_STATES:
        if state.lower() in str(place_str).lower():
            return state
    return "Other Region"


def validate_record(rec):
    """
    Pure Python validation function with exact Phase I failure semantics.
    Returns (is_valid: bool, failure_reason: str).
    """
    # 1. Null Checks
    if rec.get('id') is None or pd.isna(rec.get('id')):
        return False, REASON_MISSING_ID
    
    if not rec.get('observed_on') or pd.isna(rec.get('observed_on')):
        return False, REASON_MISSING_DATE

    # 2. Coordinates validation
    try:
        lat = float(rec['latitude'])
        lon = float(rec['longitude'])
        if lat < LAT_MIN or lat > LAT_MAX or lon < LON_MIN or lon > LON_MAX:
            return False, get_reason_bbox(lat, lon)
    except (ValueError, TypeError, KeyError):
        return False, REASON_SCHEMA_LATLON

    # 3. Media score validation
    try:
        score = int(rec.get('media_score', 0))
        if score < MEDIA_SCORE_MIN or score > MEDIA_SCORE_MAX:
            return False, get_reason_media(score)
    except (ValueError, TypeError):
        return False, REASON_SCHEMA_MEDIA

    return True, "Valid"


def transform_record(rec):
    """
    Pure Python feature engineering function producing exact Phase I output dictionary.
    """
    cat = str(rec.get('animal_category', 'Unknown')).strip()
    c_name = str(rec.get('common_name', 'Unknown')).strip()
    s_name = str(rec.get('scientific_name', 'Unknown')).strip()
    place = str(rec.get('place_guess', 'India')).strip()
    state = extract_state(place)

    obs_date_str = str(rec.get('observed_on')).split('T')[0]
    try:
        dt = datetime.strptime(obs_date_str, "%Y-%m-%d")
        year = dt.year
        month = dt.month
        day = dt.day
        day_of_week = dt.weekday()
        time_key = year * 10000 + month * 100 + day
        is_weekend = 1 if day_of_week in (5, 6) else 0
    except ValueError:
        dt = datetime.now()
        year = int(rec.get('obs_year', dt.year))
        month = int(rec.get('obs_month', dt.month))
        day = 1
        day_of_week = int(rec.get('obs_day_of_week', 0))
        time_key = year * 10000 + month * 100 + day
        is_weekend = 1 if day_of_week in (5, 6) else 0

    season = str(rec.get('season', 'Summer (Mar-May)')).strip()
    quality_grade = str(rec.get('quality_grade', 'research')).strip()
    data_quality_flag = str(rec.get('data_quality_flag', 'Clean (Research Grade)')).strip()
    gps_precision = str(rec.get('gps_precision', 'Medium (≤100m)')).strip()
    if pd.isna(gps_precision) or gps_precision == 'nan':
        gps_precision = 'Medium (≤100m)'

    iucn_status = IUCN_MAP.get(cat, "Threatened")

    lat = float(rec['latitude'])
    lon = float(rec['longitude'])
    lat_norm = round((lat - LAT_NORM_MIN) / (LAT_NORM_MAX - LAT_NORM_MIN), 6)
    lon_norm = round((lon - LON_NORM_MIN) / (LON_NORM_MAX - LON_NORM_MIN), 6)

    rec_hash = generate_record_hash(rec)

    return {
        'id_original': int(rec['id']),
        'observed_on': obs_date_str,
        'obs_year': year,
        'obs_month': month,
        'obs_day': day,
        'obs_day_of_week': day_of_week,
        'time_key': time_key,
        'is_weekend': is_weekend,
        'season': season,
        'quality_grade': quality_grade,
        'data_quality_flag': data_quality_flag,
        'gps_precision': gps_precision,
        'image_url': str(rec.get('image_url', '')),
        'sound_url': str(rec.get('sound_url', '')),
        'has_audio': int(rec.get('has_audio', 0)),
        'has_image': int(rec.get('has_image', 1)),
        'media_score': int(rec.get('media_score', 1)),
        'description': str(rec.get('description', '')),
        'place_guess': place,
        'state_province': state,
        'country': 'India',
        'latitude': lat,
        'longitude': lon,
        'lat_normalized': lat_norm,
        'lon_normalized': lon_norm,
        'common_name': c_name,
        'scientific_name': s_name,
        'animal_category': cat,
        'iucn_status': iucn_status,
        'record_hash': rec_hash
    }


class StructuredJsonFormatter(logging.Formatter):
    """
    JSON log formatter with standard fields:
    ts, level, component, batch_id, message.
    """
    def __init__(self, component="lakehouse"):
        super().__init__()
        self.component = component

    def format(self, record):
        log_obj = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "component": getattr(record, "component", self.component),
            "batch_id": getattr(record, "batch_id", None),
            "message": record.getMessage()
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def setup_logger(name, component="lakehouse", level=logging.INFO):
    """Configures and returns a logger with StructuredJsonFormatter."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJsonFormatter(component=component))
        logger.addHandler(handler)
    return logger

