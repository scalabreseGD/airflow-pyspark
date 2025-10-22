"""
Create Sample Data - Write employee data to MinIO via S3A

This script creates sample employee data and writes it directly to MinIO
using the S3A protocol. The data can then be read by other Spark jobs.

Output: s3a://data/employees/ (Parquet format)

Usage:
    ./submit.sh create_sample_data.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

print("=" * 80)
print("  Creating Sample Employee Data in MinIO")
print("=" * 80)

spark = SparkSession.builder \
    .appName("CreateSampleData") \
    .getOrCreate()

try:
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("department", StringType(), False),
        StructField("salary", IntegerType(), False)
    ])

    # Sample employee data
    data = [
        (1, "Alice", 29, "Engineering", 85000),
        (2, "Bob", 35, "Sales", 75000),
        (3, "Charlie", 28, "Engineering", 95000),
        (4, "Diana", 32, "Marketing", 70000),
        (5, "Eve", 41, "Engineering", 110000),
        (6, "Frank", 27, "Sales", 65000),
        (7, "Grace", 38, "Marketing", 80000),
        (8, "Henry", 30, "Engineering", 90000),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    print(f"\n[1/3] Created DataFrame with {df.count()} employees")
    print("\nSample data:")
    df.show()

    # Write to MinIO via S3A
    output_path = "s3a://data/employees"
    print(f"\n[2/3] Writing data to MinIO: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    print("✓ Data written successfully to MinIO")

    # Verify by reading it back
    print(f"\n[3/3] Verifying data in MinIO...")
    verification_df = spark.read.parquet(output_path)
    record_count = verification_df.count()

    print(f"✓ Verified: {record_count} records in MinIO")
    print("\nData stored in MinIO:")
    verification_df.show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Sample data is ready in MinIO")
    print("=" * 80)
    print(f"\nData location: {output_path}")
    print("View in MinIO Console: http://localhost:9001 (admin/admin123)")
    print("\nYou can now run: ./submit.sh filter_employees.py")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()
