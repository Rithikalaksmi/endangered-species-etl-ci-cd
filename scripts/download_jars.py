"""
Downloads pinned JAR dependencies for Spark Lakehouse JDBC & S3 & Kafka integration.
"""
import os
import urllib.request
import sys

JARS = [
    ("https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar", "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"),
    ("https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar", "spark-sql-kafka-0-10_2.12-3.5.3.jar"),
    ("https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar", "kafka-clients-3.7.1.jar"),
    ("https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar", "spark-token-provider-kafka-0-10_2.12-3.5.3.jar"),
    ("https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar", "commons-pool2-2.12.0.jar"),
    ("https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar", "hadoop-aws-3.3.4.jar"),
    ("https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar", "aws-java-sdk-bundle-1.12.262.jar"),
    ("https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar", "postgresql-42.7.4.jar"),
    ("https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar", "iceberg-aws-bundle-1.5.2.jar")
]

def download_jars(target_dir="spark/jars"):
    os.makedirs(target_dir, exist_ok=True)
    for url, filename in JARS:
        path = os.path.join(target_dir, filename)
        if os.path.exists(path) and os.path.getsize(path) > 1000:
            print(f"Already exists: {filename} ({os.path.getsize(path)} bytes)")
            continue
        print(f"Downloading {filename}...")
        try:
            urllib.request.urlretrieve(url, path)
            print(f"Downloaded: {filename} ({os.path.getsize(path)} bytes)")
        except Exception as e:
            print(f"Failed to download {filename}: {e}", file=sys.stderr)
            raise

if __name__ == "__main__":
    download_jars()

