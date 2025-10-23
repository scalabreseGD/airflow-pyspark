"""
IPython startup script to automatically initialize Spark session.
This script runs when each notebook kernel starts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

# Create Spark session connected to the cluster
spark = SparkSession.builder \
    .appName("Jupyter Spark Demo") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 60)
print("Spark Session Initialized")
print("=" * 60)
print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print("=" * 60)
print("Available objects: spark, col, count, avg, spark_sum")
print("Common types: StructType, StructField, StringType, IntegerType, DoubleType")
print("=" * 60)