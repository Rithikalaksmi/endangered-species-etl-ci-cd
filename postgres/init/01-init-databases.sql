-- Initialize databases for Iceberg JDBC catalog, Airflow metadata, and Superset metadata
CREATE DATABASE iceberg_catalog;
CREATE DATABASE airflow;
CREATE DATABASE superset;

GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO lakehouse;
GRANT ALL PRIVILEGES ON DATABASE airflow TO lakehouse;
GRANT ALL PRIVILEGES ON DATABASE superset TO lakehouse;

