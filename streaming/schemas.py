"""
PySpark StructType schema for species observations arriving on Kafka.
Used by stream_job.py to deserialise the JSON payload.
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

OBSERVATION_SCHEMA = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("observed_on", StringType(), nullable=True),
    StructField("season", StringType(), nullable=True),
    StructField("quality_grade", StringType(), nullable=True),
    StructField("media_score", IntegerType(), nullable=True),
    StructField("place_guess", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("common_name", StringType(), nullable=True),
    StructField("scientific_name", StringType(), nullable=True),
    StructField("animal_category", StringType(), nullable=True),
    StructField("data_quality_flag", StringType(), nullable=True),
    StructField("gps_precision", StringType(), nullable=True),
    StructField("image_url", StringType(), nullable=True),
    StructField("sound_url", StringType(), nullable=True),
    StructField("has_audio", IntegerType(), nullable=True),
    StructField("has_image", IntegerType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
])

