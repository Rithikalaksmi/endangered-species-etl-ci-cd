"""
Mock tests for api_extractor.py.

fetch_api_observations() stands in for a real REST API client. We mock out
its sources of randomness/time (random.choice, random.randint, random.uniform,
time.time) so the "API response" is fully deterministic and we can assert on
exact output — the same pattern you'd use to mock a real requests.get() call
to an external biodiversity API (e.g. iNaturalist / GBIF).
"""
from unittest.mock import patch

import api_extractor
from api_extractor import fetch_api_observations, SPECIES_METADATA


def _fake_randint(a, b):
    """Deterministic stand-in for random.randint(a, b): always returns the
    lower bound, regardless of which call site (days_ago, image id,
    media_score) invoked it."""
    return a


class TestFetchApiObservationsMocked:
    @patch("api_extractor.time.time", return_value=1_700_000_000.0)
    @patch("api_extractor.random.uniform", return_value=0.0)
    @patch("api_extractor.random.randint", side_effect=_fake_randint)
    @patch("api_extractor.random.choice")
    def test_fetch_returns_requested_count(self, mock_choice, mock_randint, mock_uniform, mock_time):
        # random.choice is called for species, hotspot, season, quality_grade
        # and gps_precision, in that order, inside the loop body.
        mock_choice.side_effect = [
            SPECIES_METADATA[0],
            SPECIES_METADATA[0]["hotspots"][0],
            "Monsoon (Jun-Sep)",
            "research",
            "High (\u226410m)",
        ] * 5

        records = fetch_api_observations(count=3)

        assert len(records) == 3

    @patch("api_extractor.time.time", return_value=1_700_000_000.0)
    @patch("api_extractor.random.uniform", return_value=0.0)
    @patch("api_extractor.random.randint", side_effect=_fake_randint)
    @patch("api_extractor.random.choice")
    def test_fetch_response_has_expected_schema(self, mock_choice, mock_randint, mock_uniform, mock_time):
        mock_choice.side_effect = [
            SPECIES_METADATA[0],
            SPECIES_METADATA[0]["hotspots"][0],
            "Monsoon (Jun-Sep)",
            "research",
            "High (\u226410m)",
        ]

        records = fetch_api_observations(count=1)
        rec = records[0]

        required_fields = {
            "id", "observed_on", "obs_year", "obs_month", "season",
            "quality_grade", "latitude", "longitude", "common_name",
            "scientific_name", "animal_category", "media_score",
        }
        assert required_fields.issubset(rec.keys())

    @patch("api_extractor.time.time", return_value=1_700_000_000.0)
    @patch("api_extractor.random.uniform", return_value=0.0)
    @patch("api_extractor.random.randint", side_effect=_fake_randint)
    @patch("api_extractor.random.choice")
    def test_coordinates_match_the_chosen_hotspot_when_jitter_is_zero(
        self, mock_choice, mock_randint, mock_uniform, mock_time
    ):
        hotspot = SPECIES_METADATA[1]["hotspots"][0]
        mock_choice.side_effect = [
            SPECIES_METADATA[1],
            hotspot,
            "Winter (Dec-Feb)",
            "research",
            "High (\u226410m)",
        ]

        records = fetch_api_observations(count=1)
        rec = records[0]

        # With random.uniform mocked to 0.0, no jitter is applied.
        assert rec["latitude"] == round(hotspot["lat"], 6)
        assert rec["longitude"] == round(hotspot["lon"], 6)

    @patch("api_extractor.time.time", return_value=1_700_000_000.0)
    @patch("api_extractor.random.uniform", return_value=0.0)
    @patch("api_extractor.random.randint", side_effect=_fake_randint)
    @patch("api_extractor.random.choice")
    def test_ids_are_unique_within_a_batch(self, mock_choice, mock_randint, mock_uniform, mock_time):
        mock_choice.side_effect = [
            SPECIES_METADATA[0],
            SPECIES_METADATA[0]["hotspots"][0],
            "Summer (Mar-May)",
            "research",
            "High (\u226410m)",
        ] * 5

        records = fetch_api_observations(count=5)
        ids = [r["id"] for r in records]

        assert len(ids) == len(set(ids))

    def test_fetch_zero_records_returns_empty_list(self):
        # No mocking needed — count=0 means the loop body (and therefore
        # random/time calls) never executes.
        assert fetch_api_observations(count=0) == []
