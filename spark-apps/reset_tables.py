"""
Reset Tables - Drop existing tables to start fresh
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ResetTables") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE test_db")
spark.sql("DROP TABLE IF EXISTS employees")
spark.sql("DROP TABLE IF EXISTS high_performers")
print("✓ Tables dropped successfully")

spark.stop()
