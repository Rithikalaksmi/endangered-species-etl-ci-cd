"""
API Extractor Module for Endangered Species Analytics.
Simulates & handles REST API data ingestion from biodiversity data sources (e.g. iNaturalist API / GBIF API).
"""

import random
import time
from datetime import datetime, timedelta
import json

SPECIES_METADATA = [
    {
        "common_name": "Mainland Asian Elephant",
        "scientific_name": "Elephas maximus indicus",
        "animal_category": "Asian Elephant",
        "hotspots": [
            {"place": "Kaziranga National Park, Assam, IN", "lat": 26.5775, "lon": 93.1711},
            {"place": "Mudumalai National Park, Tamil Nadu, IN", "lat": 11.5623, "lon": 76.5342},
            {"place": "Wayanad Wildlife Sanctuary, Kerala, IN", "lat": 11.6854, "lon": 76.2412},
            {"place": "Corbett National Park, Uttarakhand, IN", "lat": 29.5300, "lon": 78.7747}
        ]
    },
    {
        "common_name": "Bengal Tiger",
        "scientific_name": "Panthera tigris tigris",
        "animal_category": "Bengal tiger",
        "hotspots": [
            {"place": "Ranthambore National Park, Rajasthan, IN", "lat": 26.0173, "lon": 76.5026},
            {"place": "Sundarbans National Park, West Bengal, IN", "lat": 21.9497, "lon": 88.9126},
            {"place": "Bandhavgarh National Park, Madhya Pradesh, IN", "lat": 23.7228, "lon": 81.0258},
            {"place": "Tadoba Andhari Reserve, Maharashtra, IN", "lat": 20.2144, "lon": 79.3562}
        ]
    },
    {
        "common_name": "Indian Rhinoceros",
        "scientific_name": "Rhinoceros unicornis",
        "animal_category": "One-Horned Rhinoceros",
        "hotspots": [
            {"place": "Kaziranga National Park, Assam, IN", "lat": 26.5775, "lon": 93.1711},
            {"place": "Manas National Park, Assam, IN", "lat": 26.6594, "lon": 90.9392},
            {"place": "Pobitora Wildlife Sanctuary, Assam, IN", "lat": 26.2442, "lon": 92.0514}
        ]
    },
    {
        "common_name": "Gharial",
        "scientific_name": "Gavialis gangeticus",
        "animal_category": "Gharial",
        "hotspots": [
            {"place": "National Chambal Sanctuary, Madhya Pradesh, IN", "lat": 26.4951, "lon": 78.4352},
            {"place": "Katerniaghat Wildlife Sanctuary, Uttar Pradesh, IN", "lat": 28.3752, "lon": 81.1215}
        ]
    },
    {
        "common_name": "Snow Leopard",
        "scientific_name": "Panthera uncia",
        "animal_category": "leopard",
        "hotspots": [
            {"place": "Hemis National Park, Ladakh, IN", "lat": 33.9167, "lon": 77.4167},
            {"place": "Spiti Valley Wildlife Reserve, Himachal Pradesh, IN", "lat": 32.2461, "lon": 78.0349}
        ]
    },
    {
        "common_name": "Himalayan Red Panda",
        "scientific_name": "Ailurus fulgens fulgens",
        "animal_category": "red panda",
        "hotspots": [
            {"place": "Singalila National Park, West Bengal, IN", "lat": 27.0392, "lon": 88.0772},
            {"place": "Nokrek National Park, Meghalaya, IN", "lat": 25.4667, "lon": 90.3167}
        ]
    }
]

SEASONS = [
    "Winter (Dec-Feb)",
    "Summer (Mar-May)",
    "Monsoon (Jun-Sep)",
    "Post-Monsoon (Oct-Nov)"
]

QUALITY_GRADES = ["research", "research", "research", "needs_id"]
GPS_PRECISION_OPTS = ["High (≤10m)", "Medium (≤100m)", "Low (≤1km)", "Very Low (>1km)"]

def fetch_api_observations(count=10):
    """
    Simulates fetching real-time species observation payloads from a REST API endpoint.
    Returns a list of dictionary payloads.
    """
    records = []
    base_id = int(time.time() * 1000) % 100000000

    for i in range(count):
        spec = random.choice(SPECIES_METADATA)
        hotspot = random.choice(spec["hotspots"])

        # Generate realistic date in recent 30 days
        days_ago = random.randint(0, 30)
        obs_date = datetime.now() - timedelta(days=days_ago)

        lat = round(hotspot["lat"] + random.uniform(-0.05, 0.05), 6)
        lon = round(hotspot["lon"] + random.uniform(-0.05, 0.05), 6)

        rec = {
            "id": base_id + i,
            "observed_on": obs_date.strftime("%Y-%m-%d"),
            "obs_year": obs_date.year,
            "obs_month": obs_date.month,
            "obs_day_of_week": obs_date.weekday(),
            "season": random.choice(SEASONS),
            "quality_grade": random.choice(QUALITY_GRADES),
            "data_quality_flag": "Clean (Research Grade)" if random.random() > 0.1 else "Needs Verification",
            "image_url": f"https://inaturalist-open-data.s3.amazonaws.com/photos/{random.randint(1000000, 9000000)}/medium.jpg",
            "sound_url": None,
            "has_audio": 0,
            "has_image": 1,
            "media_score": random.randint(1, 5),
            "description": f"Live REST API observation of {spec['common_name']} recorded in {hotspot['place']}.",
            "place_guess": hotspot["place"],
            "latitude": lat,
            "longitude": lon,
            "common_name": spec["common_name"],
            "scientific_name": spec["scientific_name"],
            "animal_category": spec["animal_category"],
            "gps_precision": random.choice(GPS_PRECISION_OPTS),
            "lat_normalized": round((lat - 8.0) / (35.0 - 8.0), 6),
            "lon_normalized": round((lon - 68.0) / (97.0 - 68.0), 6)
        }
        records.append(rec)

    return records

if __name__ == '__main__':
    sample = fetch_api_observations(3)
    print(f"Generated {len(sample)} sample API records:")
    print(json.dumps(sample, indent=2))
