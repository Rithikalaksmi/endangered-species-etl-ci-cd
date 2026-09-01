"""
Unit tests for the transformation & data-quality logic in etl_pipeline.py.

These tests exercise pure functions only (no DB writes), which is what makes
them fast and safe to run on every commit in CI.
"""
import math
import pytest

from etl_pipeline import (
    generate_record_hash,
    extract_state,
    validate_record,
    transform_record,
    IUCN_MAP,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def valid_record():
    """A well-formed observation, shaped like what fetch_api_observations()
    or the CSV extractor would produce."""
    return {
        "id": 12345,
        "observed_on": "2024-07-15",
        "obs_year": 2024,
        "obs_month": 7,
        "obs_day_of_week": 0,
        "season": "Monsoon (Jun-Sep)",
        "quality_grade": "research",
        "data_quality_flag": "Clean (Research Grade)",
        "image_url": "https://example.com/photo.jpg",
        "sound_url": None,
        "has_audio": 0,
        "has_image": 1,
        "media_score": 4,
        "description": "Test sighting",
        "place_guess": "Kaziranga National Park, Assam, IN",
        "latitude": 26.5775,
        "longitude": 93.1711,
        "common_name": "Mainland Asian Elephant",
        "scientific_name": "Elephas maximus indicus",
        "animal_category": "Asian Elephant",
        "gps_precision": "High (\u226410m)",
    }


# ---------------------------------------------------------------------------
# generate_record_hash — Exercise 5: Idempotency
# ---------------------------------------------------------------------------
class TestGenerateRecordHash:
    def test_hash_is_deterministic(self, valid_record):
        h1 = generate_record_hash(valid_record)
        h2 = generate_record_hash(valid_record)
        assert h1 == h2

    def test_hash_is_sha256_hex_digest(self, valid_record):
        h = generate_record_hash(valid_record)
        assert len(h) == 64
        int(h, 16)  # raises ValueError if not valid hex

    def test_hash_changes_when_key_fields_change(self, valid_record):
        h1 = generate_record_hash(valid_record)
        changed = dict(valid_record, latitude=11.0)
        h2 = generate_record_hash(changed)
        assert h1 != h2


# ---------------------------------------------------------------------------
# extract_state — location parsing
# ---------------------------------------------------------------------------
class TestExtractState:
    @pytest.mark.parametrize(
        "place,expected",
        [
            ("Kaziranga National Park, Assam, IN", "Assam"),
            ("Mudumalai National Park, Tamil Nadu, IN", "Tamil Nadu"),
            ("Somewhere with no state info", "Other Region"),
            (None, "Unknown Region"),
            ("", "Unknown Region"),
        ],
    )
    def test_extract_state(self, place, expected):
        assert extract_state(place) == expected


# ---------------------------------------------------------------------------
# validate_record — Exercise 5: Data Quality Gatekeeper
# ---------------------------------------------------------------------------
class TestValidateRecord:
    def test_valid_record_passes(self, valid_record):
        is_valid, reason = validate_record(valid_record)
        assert is_valid is True
        assert reason == "Valid"

    def test_missing_id_fails(self, valid_record):
        bad = dict(valid_record, id=None)
        is_valid, reason = validate_record(bad)
        assert is_valid is False
        assert "Observation ID" in reason

    def test_missing_observed_on_fails(self, valid_record):
        bad = dict(valid_record, observed_on=None)
        is_valid, reason = validate_record(bad)
        assert is_valid is False
        assert "Observation Date" in reason

    def test_coordinates_outside_india_bbox_fail(self, valid_record):
        bad = dict(valid_record, latitude=999.0, longitude=999.0)
        is_valid, reason = validate_record(bad)
        assert is_valid is False
        assert "Outlier Detected" in reason

    def test_non_numeric_coordinates_fail(self, valid_record):
        bad = dict(valid_record, latitude="not-a-number")
        is_valid, reason = validate_record(bad)
        assert is_valid is False
        assert "Schema Mismatch" in reason

    def test_media_score_out_of_range_fails(self, valid_record):
        bad = dict(valid_record, media_score=99)
        is_valid, reason = validate_record(bad)
        assert is_valid is False
        assert "Outlier Detected" in reason


# ---------------------------------------------------------------------------
# transform_record — Exercise 1 & 2: Cleansing + Feature Engineering
# ---------------------------------------------------------------------------
class TestTransformRecord:
    def test_returns_expected_keys(self, valid_record):
        out = transform_record(valid_record)
        for key in ("lat_normalized", "lon_normalized", "iucn_status", "record_hash", "state_province"):
            assert key in out

    def test_iucn_status_is_mapped_correctly(self, valid_record):
        out = transform_record(valid_record)
        assert out["iucn_status"] == IUCN_MAP["Asian Elephant"]

    def test_unknown_species_maps_to_threatened(self, valid_record):
        rec = dict(valid_record, animal_category="Unlisted Species")
        out = transform_record(rec)
        assert out["iucn_status"] == "Threatened"

    def test_spatial_normalization_is_within_unit_range(self, valid_record):
        out = transform_record(valid_record)
        assert 0.0 <= out["lat_normalized"] <= 1.0
        assert 0.0 <= out["lon_normalized"] <= 1.0

    def test_state_is_extracted_from_place_guess(self, valid_record):
        out = transform_record(valid_record)
        assert out["state_province"] == "Assam"

    def test_weekend_flag_for_saturday(self, valid_record):
        # 2024-07-13 is a Saturday
        rec = dict(valid_record, observed_on="2024-07-13")
        out = transform_record(rec)
        assert out["is_weekend"] == 1

    def test_weekend_flag_for_weekday(self, valid_record):
        # 2024-07-15 is a Monday
        out = transform_record(valid_record)
        assert out["is_weekend"] == 0

    def test_whitespace_and_missing_fields_are_cleaned(self, valid_record):
        rec = dict(valid_record, common_name="  Mainland Asian Elephant  ")
        rec.pop("scientific_name")
        out = transform_record(rec)
        assert out["common_name"] == "Mainland Asian Elephant"
        assert out["scientific_name"] == "Unknown"
