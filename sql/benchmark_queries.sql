-- Benchmark queries Q1–Q7 for Trino on Iceberg fact_observations
-- Run via: python benchmarks/run_benchmark.py
-- All queries use the lake.lakehouse catalog/schema

-- Q1: Total observations by species (category scan + group)
SELECT animal_category, COUNT(*) AS total_obs
FROM lake.lakehouse.fact_observations
GROUP BY animal_category
ORDER BY total_obs DESC;

-- Q2: Observations per year and IUCN status (partition pruning test)
SELECT obs_year, iucn_status, COUNT(*) AS cnt
FROM lake.lakehouse.fact_observations
WHERE obs_year >= 2020
GROUP BY obs_year, iucn_status
ORDER BY obs_year, cnt DESC;

-- Q3: Top 10 states by sighting count
SELECT state_province, COUNT(*) AS sightings
FROM lake.lakehouse.fact_observations
GROUP BY state_province
ORDER BY sightings DESC
LIMIT 10;

-- Q4: Average media score by quality grade (aggregate)
SELECT quality_grade, ROUND(AVG(CAST(media_score AS double)), 3) AS avg_media, COUNT(*) AS n
FROM lake.lakehouse.fact_observations
GROUP BY quality_grade
ORDER BY avg_media DESC;

-- Q5: Monthly trend for a specific species (partition + filter)
SELECT obs_year, obs_month, COUNT(*) AS monthly_count
FROM lake.lakehouse.fact_observations
WHERE animal_category = 'Asian Elephant'
GROUP BY obs_year, obs_month
ORDER BY obs_year, obs_month;

-- Q6: Weekend vs weekday distribution
SELECT
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS observations,
    ROUND(AVG(CAST(media_score AS double)), 3) AS avg_media
FROM lake.lakehouse.fact_observations
GROUP BY is_weekend;

-- Q7: Count distinct species per state (heavy aggregation)
SELECT state_province, COUNT(DISTINCT animal_category) AS species_count,
       COUNT(*) AS total_obs
FROM lake.lakehouse.fact_observations
GROUP BY state_province
ORDER BY species_count DESC, total_obs DESC;
