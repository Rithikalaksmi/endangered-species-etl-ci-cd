"""
PySpark Structured Streaming job: Kafka -> Iceberg (via MinIO) + dead-letter table.

Run inside the Spark container:
    /opt/spark/bin/spark-submit \\
        --master local[*] \\
        /opt/spark/work-dir/streaming/stream_job.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

sys.path.insert(0, "/opt/spark/work-dir")
from common.rules import (
    IUCN_MAP,
    INDIAN_STATES,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
    LAT_NORM_MIN, LAT_NORM_MAX, LON_NORM_MIN, LON_NORM_MAX,
    MEDIA_SCORE_MIN, MEDIA_SCORE_MAX,
    setup_logger,
)
from streaming.schemas import OBSERVATION_SCHEMA

log = setup_logger("stream_job", component="stream_job")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "species.observations")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
JDBC_URL        = os.getenv("ICEBERG_CATALOG_JDBC_URL",
    "jdbc:postgresql://postgres:5432/iceberg_catalog?user=lakehouse&password=lakehouse123")
WAREHOUSE       = os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/")
CHECKPOINT_DIR  = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://lakehouse/checkpoints/stream_job")
TRIGGER_SEC     = int(os.getenv("STREAM_TRIGGER_SECONDS", "15"))

def build_spark() -> SparkSession:
    jars_dir = "/opt/spark/work-dir/spark/jars"
    jar_files = ",".join(str(p) for p in Path(jars_dir).glob("*.jar")) if Path(jars_dir).exists() else ""

    builder = (
        SparkSession.builder
        .appName("EndangeredSpecies-Streaming")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "jdbc")
        .config("spark.sql.catalog.lake.uri", JDBC_URL)
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.lake.jdbc.schema-version", "V1")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if jar_files:
        builder = builder.config("spark.jars", jar_files)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def _extract_state_udf_impl(place):
    if not place:
        return "Unknown Region"
    for s in INDIAN_STATES:
        if s.lower() in str(place).lower():
            return s
    return "Other Region"

def _iucn_udf_impl(category):
    return IUCN_MAP.get(str(category).strip(), "Threatened")

def _record_hash_udf_impl(rec_id, observed_on, latitude, longitude, common_name):
    import hashlib
    raw = f"{rec_id}|{observed_on}|{latitude}|{longitude}|{common_name}"
    return hashlib.sha256(raw.encode()).hexdigest()

def transform_batch(spark: SparkSession):
    extract_state = F.udf(_extract_state_udf_impl, StringType())
    iucn_udf      = F.udf(_iucn_udf_impl, StringType())
    hash_udf      = F.udf(_record_hash_udf_impl, StringType())

    def _transform(df, epoch_id):
        input_count = df.count()
        log.info(f"Processing batch epoch={epoch_id}, input_rows={input_count}")
        if input_count == 0:
            return

        valid_mask = (
            F.col("id").isNotNull() &
            F.col("observed_on").isNotNull() &
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull() &
            (F.col("latitude") >= LAT_MIN) & (F.col("latitude") <= LAT_MAX) &
            (F.col("longitude") >= LON_MIN) & (F.col("longitude") <= LON_MAX) &
            F.col("media_score").isNotNull() &
            (F.col("media_score") >= MEDIA_SCORE_MIN) &
            (F.col("media_score") <= MEDIA_SCORE_MAX)
        )

        valid_df   = df.filter(valid_mask)
        invalid_df = df.filter(~valid_mask)

        dlq_df = invalid_df.withColumn(
            "failure_reason",
            F.when(F.col("id").isNull(), "Null Check Failed: Missing Observation ID")
             .when(F.col("observed_on").isNull(), "Null Check Failed: Missing Observation Date 'observed_on'")
             .when(
                 (F.col("latitude") < LAT_MIN) | (F.col("latitude") > LAT_MAX) |
                 (F.col("longitude") < LON_MIN) | (F.col("longitude") > LON_MAX),
                 F.concat(
                     F.lit("Outlier Detected: Coordinates Lat ("),
                     F.col("latitude").cast(StringType()),
                     F.lit("), Lon ("),
                     F.col("longitude").cast(StringType()),
                     F.lit(") outside geographic bounding box")
                 )
             )
             .otherwise("Schema Mismatch: malformed JSON")
        ).withColumn("ingestion_ts", F.current_timestamp()) \
         .withColumn("kafka_topic", F.lit(KAFKA_TOPIC)) \
         .withColumn("batch_id", F.lit(str(epoch_id)))

        dlq_count = dlq_df.count()
        if dlq_count > 0:
            dlq_df.write.format("iceberg") \
                .mode("append") \
                .saveAsTable("lake.lakehouse.dead_letter_queue")
            log.info(f"Dead-letter records written: {dlq_count}")

        valid_count = valid_df.count()
        if valid_count == 0:
            log.info("No valid records in batch")
            return

        enriched = valid_df \
            .withColumn("obs_date", F.to_date(F.col("observed_on"), "yyyy-MM-dd")) \
            .withColumn("obs_year", F.year("obs_date")) \
            .withColumn("obs_month", F.month("obs_date")) \
            .withColumn("obs_day", F.dayofmonth("obs_date")) \
            .withColumn("obs_day_of_week", F.dayofweek("obs_date") - 2) \
            .withColumn("time_key",
                F.col("obs_year") * 10000 +
                F.col("obs_month") * 100 +
                F.col("obs_day")
            ) \
            .withColumn("is_weekend",
                F.when(F.dayofweek("obs_date").isin(1, 7), 1).otherwise(0)
            ) \
            .withColumn("state_province", extract_state(F.col("place_guess"))) \
            .withColumn("iucn_status", iucn_udf(F.col("animal_category"))) \
            .withColumn("lat_normalized",
                F.round((F.col("latitude") - LAT_NORM_MIN) / (LAT_NORM_MAX - LAT_NORM_MIN), 6)
            ) \
            .withColumn("lon_normalized",
                F.round((F.col("longitude") - LON_NORM_MIN) / (LON_NORM_MAX - LON_NORM_MIN), 6)
            ) \
            .withColumn("record_hash",
                hash_udf(
                    F.col("id").cast(StringType()),
                    F.col("observed_on"),
                    F.col("latitude").cast(StringType()),
                    F.col("longitude").cast(StringType()),
                    F.col("common_name")
                )
            ) \
            .withColumn("ingestion_ts", F.current_timestamp()) \
            .withColumn("id_original", F.col("id").cast("long")) \
            .withColumn("country", F.coalesce(F.col("country"), F.lit("India")))

        output_cols = [
            "id_original", "observed_on", "obs_year", "obs_month", "obs_day",
            "obs_day_of_week", "time_key", "is_weekend", "season", "quality_grade",
            "data_quality_flag", "gps_precision", "image_url", "sound_url",
            "has_audio", "has_image", "media_score", "description",
            "place_guess", "state_province", "country",
            "latitude", "longitude", "lat_normalized", "lon_normalized",
            "common_name", "scientific_name", "animal_category",
            "iucn_status", "record_hash", "ingestion_ts"
        ]
        final_df = enriched.select(*output_cols)

        final_df.createOrReplaceTempView("batch_data")
        spark.sql("""
            MERGE INTO lake.lakehouse.fact_observations t
            USING batch_data s
            ON t.record_hash = s.record_hash
            WHEN NOT MATCHED THEN INSERT *
        """)
        log.info(f"Batch written to Iceberg: {valid_count} valid records inserted/merged")

    return _transform

def main():
    log.info(f"Stream job starting: kafka={KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}, trigger={TRIGGER_SEC}s")
    spark = build_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.lakehouse")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), OBSERVATION_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .trigger(processingTime=f"{TRIGGER_SEC} seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(transform_batch(spark))
        .start()
    )

    log.info(f"Streaming query started with query_id={query.id}")
    query.awaitTermination()

if __name__ == "__main__":
    main()

