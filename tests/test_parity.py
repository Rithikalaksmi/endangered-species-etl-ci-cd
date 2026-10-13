"""
Phase I / Phase II parity test.
Verifies that common/rules.py produces IDENTICAL output to
phase1/etl_pipeline.py for the same input records.
Must always pass. Run with: pytest tests/test_parity.py -v
"""
import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Phase I originals
from phase1.etl_pipeline import (
    IUCN_MAP as P1_IUCN_MAP,
    INDIAN_STATES as P1_INDIAN_STATES,
    generate_record_hash as p1_generate_record_hash,
    extract_state as p1_extract_state,
    validate_record as p1_validate_record,
    transform_record as p1_transform_record,
)

# Phase II shared rules
from common.rules import (
    IUCN_MAP as P2_IUCN_MAP,
    INDIAN_STATES as P2_INDIAN_STATES,
    generate_record_hash as p2_generate_record_hash,
    extract_state as p2_extract_state,
    validate_record as p2_validate_record,
    transform_record as p2_transform_record,
)

VALID_RECORD = {
    "id": 12345678,
    "observed_on": "2024-03-15",
    "season": "Spring (Mar-May)",
    "quality_grade": "research",
    "media_score": 7,
    "place_guess": "Assam, India",
    "latitude": 26.2,
    "longitude": 91.7,
    "common_name": "Asian Elephant",
    "scientific_name": "Elephas maximus",
    "animal_category": "Asian Elephant",
    "data_quality_flag": "Clean (Research Grade)",
    "gps_precision": "High (<10m)",
    "image_url": "",
    "sound_url": "",
    "has_audio": 0,
    "has_image": 1,
    "description": "Test record",
    "country": "India",
}

INVALID_LAT = dict(VALID_RECORD, latitude=99.0)
INVALID_SCORE = dict(VALID_RECORD, media_score=15)
MISSING_ID = dict(VALID_RECORD, id=None)

class TestConstantParity:
    def test_iucn_map_identical(self):
        assert P1_IUCN_MAP == P2_IUCN_MAP

    def test_indian_states_identical(self):
        assert set(P1_INDIAN_STATES) == set(P2_INDIAN_STATES)

class TestHashParity:
    def test_hash_valid_record(self):
        h1 = p1_generate_record_hash(VALID_RECORD)
        h2 = p2_generate_record_hash(VALID_RECORD)
        assert h1 == h2, f"Hash mismatch: P1={h1}, P2={h2}"

    def test_hash_length(self):
        h = p2_generate_record_hash(VALID_RECORD)
        assert len(h) == 64

class TestValidationParity:
    def test_valid_record(self):
        v1, r1 = p1_validate_record(VALID_RECORD)
        v2, r2 = p2_validate_record(VALID_RECORD)
        assert v1 == v2, f"Validity mismatch: P1={v1}, P2={v2}"

    def test_invalid_latitude(self):
        v1, r1 = p1_validate_record(INVALID_LAT)
        v2, r2 = p2_validate_record(INVALID_LAT)
        assert v1 == v2 == False
        assert r1 == r2

    def test_invalid_score(self):
        v1, r1 = p1_validate_record(INVALID_SCORE)
        v2, r2 = p2_validate_record(INVALID_SCORE)
        assert v1 == v2 == False
        assert r1 == r2

    def test_missing_id(self):
        v1, r1 = p1_validate_record(MISSING_ID)
        v2, r2 = p2_validate_record(MISSING_ID)
        assert v1 == v2 == False
        assert r1 == r2

class TestTransformParity:
    def test_transform_output_keys(self):
        t1 = p1_transform_record(VALID_RECORD)
        t2 = p2_transform_record(VALID_RECORD)
        assert set(t1.keys()) == set(t2.keys()), (
            f"Key mismatch. P1-only: {set(t1.keys())-set(t2.keys())} "
            f"P2-only: {set(t2.keys())-set(t1.keys())}"
        )

    def test_transform_output_values(self):
        t1 = p1_transform_record(VALID_RECORD)
        t2 = p2_transform_record(VALID_RECORD)
        mismatches = {k: (t1[k], t2[k]) for k in t1 if t1[k] != t2[k]}
        assert not mismatches, f"Value mismatches: {mismatches}"

    def test_iucn_status_assigned(self):
        t = p2_transform_record(VALID_RECORD)
        assert t["iucn_status"] == "Endangered (EN)"

    def test_state_extracted(self):
        t = p2_transform_record(VALID_RECORD)
        assert t["state_province"] == "Assam"

    def test_lat_normalized_range(self):
        t = p2_transform_record(VALID_RECORD)
        assert 0.0 <= t["lat_normalized"] <= 1.0

    def test_lon_normalized_range(self):
        t = p2_transform_record(VALID_RECORD)
        assert 0.0 <= t["lon_normalized"] <= 1.0

    def test_record_hash_deterministic(self):
        t1 = p2_transform_record(VALID_RECORD)
        t2 = p2_transform_record(VALID_RECORD)
        assert t1["record_hash"] == t2["record_hash"]

class TestExtractStateParity:
    @pytest.mark.parametrize("place,expected", [
        ("Jim Corbett, Uttarakhand, India", "Uttarakhand"),
        ("Kaziranga, Assam, India", "Assam"),
        ("Somewhere in Karnataka", "Karnataka"),
        ("Unknown place XYZ", "Other Region"),
        (None, "Unknown Region"),
    ])
    def test_extract_state_parity(self, place, expected):
        r1 = p1_extract_state(place)
        r2 = p2_extract_state(place)
        assert r1 == r2 == expected, f"place={place!r}: P1={r1!r}, P2={r2!r}, expected={expected!r}"

