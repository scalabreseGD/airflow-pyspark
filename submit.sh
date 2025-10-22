#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: ./submit.sh <python_file>"
    echo "Example: ./submit.sh complete_test.py"
    exit 1
fi

SCRIPT_NAME=$1

# Check if file exists
if [ ! -f "spark-apps/$SCRIPT_NAME" ]; then
    echo "Error: File 'spark-apps/$SCRIPT_NAME' not found"
    exit 1
fi

echo "Submitting Spark job: $SCRIPT_NAME"
echo "========================================"
echo

docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.sql.warehouse.dir=s3a://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=admin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    /opt/spark-apps/$SCRIPT_NAME

echo
echo "========================================"
echo "Job completed!"
