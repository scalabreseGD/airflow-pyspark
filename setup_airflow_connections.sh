#!/bin/bash

# Setup Airflow connections for Spark, MinIO, and PostgreSQL
# Run this after Airflow is up and running

echo "Setting up Airflow connections..."
echo "=================================="

# Create Spark connection
echo "Creating Spark connection (spark_default)..."
docker-compose exec airflow-scheduler airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' \
    --conn-extra '{"queue": "root.default", "deploy-mode": "client"}'

# Create MinIO/S3 connection (useful for S3 sensors and operators)
echo "Creating MinIO/S3 connection (minio_default)..."
docker-compose exec airflow-scheduler airflow connections add 'minio_default' \
    --conn-type 'aws' \
    --conn-login 'admin' \
    --conn-password 'admin123' \
    --conn-extra '{"endpoint_url": "http://minio:9000", "aws_access_key_id": "admin", "aws_secret_access_key": "admin123"}'

# Create PostgreSQL connection for Hive Metastore (useful for metadata queries)
echo "Creating PostgreSQL connection (postgres_hive)..."
docker-compose exec airflow-scheduler airflow connections add 'postgres_hive' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-login 'hive' \
    --conn-password 'hive123' \
    --conn-schema 'metastore'

echo "=================================="
echo "✓ Airflow connections created successfully!"
echo ""
echo "To verify connections, visit:"
echo "  http://localhost:8082/connection/list/"
echo ""
echo "Or run:"
echo "  docker-compose exec airflow-scheduler airflow connections list"