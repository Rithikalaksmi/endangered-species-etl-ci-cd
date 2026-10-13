-- ========================================================================================
-- LAKEHOUSE DDL & VIEWS FOR ENDANGERED SPECIES ANALYTICS
-- Catalog: iceberg, Schema: wildlife
-- ========================================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.wildlife
WITH (location = 's3://lakehouse/silver/');

-- 1. Silver Fact Table: fact_observations
-- Wide denormalized table partitioned by month and iucn_status, sorted by state and category
CREATE TABLE IF NOT EXISTS iceberg.wildlife.fact_observations (
    obs_id BIGINT,
    observed_on DATE,
    obs_year INTEGER,
    obs_month INTEGER,
    obs_day_of_week INTEGER,
    is_weekend INTEGER,
    season VARCHAR,
    quality_grade VARCHAR,
    data_quality_flag VARCHAR,
    gps_precision VARCHAR,
    has_image INTEGER,
    has_audio INTEGER,
    media_score INTEGER,
    place_guess VARCHAR,
    state_province VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    lat_normalized DOUBLE,
    lon_normalized DOUBLE,
    common_name VARCHAR,
    scientific_name VARCHAR,
    animal_category VARCHAR,
    iucn_status VARCHAR,
    record_hash VARCHAR,
    ingested_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(observed_on)', 'iucn_status'],
    sorted_by = ARRAY['state_province', 'animal_category']
);

-- 2. Dead Letter Queue Table for rejected records
CREATE TABLE IF NOT EXISTS iceberg.wildlife.dead_letter_queue (
    raw_payload VARCHAR,
    failure_reason VARCHAR,
    kafka_offset BIGINT,
    failed_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- 3. Pipeline Metrics Table for streaming observability and health
CREATE TABLE IF NOT EXISTS iceberg.wildlife.pipeline_metrics (
    batch_id BIGINT,
    rows_in BIGINT,
    rows_valid BIGINT,
    rows_rejected BIGINT,
    duplicates_skipped BIGINT,
    batch_duration_ms BIGINT,
    max_event_lag_seconds DOUBLE,
    processed_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- 4. Bronze Raw Ingestion Table (Replayable audit lake)
CREATE TABLE IF NOT EXISTS iceberg.wildlife.observations_raw (
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP(6),
    raw_payload VARCHAR
)
WITH (
    format = 'PARQUET'
);

-- ========================================================================================
-- ANALYTICAL VIEWS REPLICATING PHASE I OLAP STAR SCHEMA & DATA CUBE
-- ========================================================================================

-- Species Dimension View
CREATE OR REPLACE VIEW iceberg.wildlife.v_dim_species AS
SELECT DISTINCT
    animal_category,
    common_name,
    scientific_name,
    iucn_status
FROM iceberg.wildlife.fact_observations;

-- Location Dimension View
CREATE OR REPLACE VIEW iceberg.wildlife.v_dim_location AS
SELECT DISTINCT
    place_guess,
    state_province,
    latitude,
    longitude,
    lat_normalized,
    lon_normalized
FROM iceberg.wildlife.fact_observations;

-- Time Dimension View
CREATE OR REPLACE VIEW iceberg.wildlife.v_dim_time AS
SELECT DISTINCT
    observed_on,
    obs_year,
    obs_month,
    obs_day_of_week,
    season,
    is_weekend
FROM iceberg.wildlife.fact_observations;

-- Multi-Dimensional Data Cube (Rollup & Slicing/Dicing)
CREATE OR REPLACE VIEW iceberg.wildlife.v_cube AS
SELECT
    COALESCE(animal_category, 'All') AS category,
    COALESCE(season, 'All') AS season,
    COALESCE(state_province, 'All') AS state,
    COALESCE(quality_grade, 'All') AS quality_grade,
    COUNT(*) AS total_observations,
    ROUND(AVG(media_score), 2) AS avg_media_score,
    SUM(has_image) AS has_image_count,
    SUM(CASE WHEN quality_grade = 'research' THEN 1 ELSE 0 END) AS research_grade_count
FROM iceberg.wildlife.fact_observations
GROUP BY GROUPING SETS (
    (animal_category, season, state_province, quality_grade),
    (animal_category, season),
    (animal_category),
    (season),
    ()
);

-- Global KPI Rollup View
CREATE OR REPLACE VIEW iceberg.wildlife.v_kpis AS
SELECT
    COUNT(*) AS total_observations,
    COUNT(DISTINCT animal_category) AS total_categories,
    COUNT(DISTINCT common_name) AS total_species,
    ROUND(CAST(SUM(CASE WHEN quality_grade = 'research' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) * 100.0, 1) AS clean_data_rate,
    MAX(ingested_at) AS latest_ingest_time
FROM iceberg.wildlife.fact_observations;

