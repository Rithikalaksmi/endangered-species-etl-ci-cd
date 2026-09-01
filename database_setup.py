"""
Database Setup & Schema DDL Manager for Endangered Species Analytics ETL Pipeline.
Implements OLTP (3NF), OLAP (Star Schema), Data Cube, CDC Logging, Quarantine, and Audit tables.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'endangered_species_dw.db')

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    print(f"Initializing SQLite Database at: {DB_PATH}")
    conn = get_connection()
    cursor = conn.cursor()

    # Enable Foreign Keys & Write-Ahead Logging for performance & atomicity
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA journal_mode = WAL;")

    # ==========================================
    # 1. OLTP RELATIONAL SCHEMA (3NF)
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tbl_species (
        species_id INTEGER PRIMARY KEY AUTOINCREMENT,
        common_name TEXT NOT NULL UNIQUE,
        scientific_name TEXT NOT NULL,
        animal_category TEXT NOT NULL,
        iucn_status TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tbl_locations (
        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
        place_guess TEXT NOT NULL,
        state_province TEXT,
        country TEXT DEFAULT 'India',
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        lat_normalized REAL,
        lon_normalized REAL,
        UNIQUE(place_guess, latitude, longitude)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tbl_observations (
        obs_id INTEGER PRIMARY KEY,
        id_original INTEGER,
        species_id INTEGER REFERENCES tbl_species(species_id),
        location_id INTEGER REFERENCES tbl_locations(location_id),
        observed_on DATE NOT NULL,
        obs_year INTEGER,
        obs_month INTEGER,
        obs_day_of_week INTEGER,
        season TEXT,
        quality_grade TEXT,
        data_quality_flag TEXT,
        has_image INTEGER DEFAULT 0,
        has_audio INTEGER DEFAULT 0,
        media_score INTEGER DEFAULT 0,
        description TEXT,
        image_url TEXT,
        sound_url TEXT,
        gps_precision TEXT,
        record_hash TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_staging_observations (
        staging_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER,
        observed_on TEXT,
        obs_year INTEGER,
        obs_month INTEGER,
        obs_day_of_week INTEGER,
        season TEXT,
        quality_grade TEXT,
        data_quality_flag TEXT,
        image_url TEXT,
        sound_url TEXT,
        has_audio INTEGER,
        has_image INTEGER,
        media_score INTEGER,
        description TEXT,
        place_guess TEXT,
        latitude REAL,
        longitude REAL,
        common_name TEXT,
        scientific_name TEXT,
        animal_category TEXT,
        gps_precision TEXT,
        lat_normalized REAL,
        lon_normalized REAL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ==========================================
    # 2. OLAP DATA WAREHOUSE (STAR SCHEMA)
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_species (
        species_key INTEGER PRIMARY KEY AUTOINCREMENT,
        common_name TEXT UNIQUE,
        scientific_name TEXT,
        animal_category TEXT,
        iucn_status TEXT,
        taxonomic_class TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_key INTEGER PRIMARY KEY AUTOINCREMENT,
        place_guess TEXT,
        state_province TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        biodiversity_zone TEXT,
        UNIQUE(place_guess, latitude, longitude)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_time (
        time_key INTEGER PRIMARY KEY, -- YYYYMMDD format
        full_date DATE UNIQUE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week INTEGER,
        season TEXT,
        is_weekend INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_quality (
        quality_key INTEGER PRIMARY KEY AUTOINCREMENT,
        quality_grade TEXT,
        data_quality_flag TEXT,
        gps_precision TEXT,
        UNIQUE(quality_grade, data_quality_flag, gps_precision)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_endangered_observations (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        obs_id INTEGER UNIQUE,
        species_key INTEGER REFERENCES dim_species(species_key),
        location_key INTEGER REFERENCES dim_location(location_key),
        time_key INTEGER REFERENCES dim_time(time_key),
        quality_key INTEGER REFERENCES dim_quality(quality_key),
        media_score INTEGER,
        has_image INTEGER,
        has_audio INTEGER,
        is_outlier INTEGER DEFAULT 0,
        observation_count INTEGER DEFAULT 1,
        record_hash TEXT UNIQUE
    );
    """)

    # ==========================================
    # 3. OLAP DATA CUBE TABLE (Slice & Dice Aggregations)
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cube_agg_analytics (
        cube_id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_category TEXT,
        season TEXT,
        state_province TEXT,
        quality_grade TEXT,
        total_observations INTEGER,
        avg_media_score REAL,
        has_image_count INTEGER,
        research_grade_count INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(animal_category, season, state_province, quality_grade)
    );
    """)

    # ==========================================
    # 4. SYSTEM TABLES: CDC, QUARANTINE, AUDIT
    # ==========================================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cdc_change_log (
        cdc_id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id INTEGER,
        table_name TEXT NOT NULL,
        change_type TEXT NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
        changed_fields TEXT,
        old_values TEXT,
        new_values TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quarantine_records (
        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_record_id INTEGER,
        failure_reason TEXT NOT NULL,
        raw_data_json TEXT NOT NULL,
        quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_type TEXT NOT NULL, -- 'FULL', 'INCREMENTAL', 'API_INGEST'
        status TEXT NOT NULL, -- 'SUCCESS', 'FAILED', 'PARTIAL'
        records_processed INTEGER DEFAULT 0,
        records_valid INTEGER DEFAULT 0,
        records_quarantined INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        duration_seconds REAL DEFAULT 0.0,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    conn.close()
    print("Database Schema DDL setup completed successfully!")

if __name__ == '__main__':
    init_db()
