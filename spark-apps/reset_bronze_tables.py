"""
Reset Bronze Tables - Drop all bronze tables and optionally clean data

This script drops all bronze layer tables and optionally deletes the
underlying data files from MinIO. Use this to start fresh with bronze
data ingestion.

WARNING: This will remove all bronze tables and optionally delete data!

Usage:
    ./submit.sh reset_bronze_tables.py
"""

from pyspark.sql import SparkSession
from datetime import datetime
import sys

print("=" * 80)
print("  Reset Bronze Tables")
print("=" * 80)

spark = SparkSession.builder \
    .appName("ResetBronzeTables") \
    .enableHiveSupport() \
    .getOrCreate()

# Bronze tables to drop
bronze_tables = [
    "transactions_raw",
    "transaction_items_raw",
    "subscriptions_raw",
    "customer_interactions_raw",
    "product_catalog_raw",
    "inventory_snapshots_raw",
    "marketing_campaigns_raw",
    "campaign_events_raw"
]

try:
    print(f"\nStarting reset at: {datetime.now()}")
    print(f"Tables to drop: {len(bronze_tables)}\n")

    dropped_tables = []
    failed_tables = []

    # Drop all bronze tables
    for idx, table_name in enumerate(bronze_tables, 1):
        full_table_name = f"bronze.{table_name}"
        print(f"[{idx}/{len(bronze_tables)}] Dropping table: {full_table_name}")

        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            print(f"  ✓ Table dropped: {full_table_name}")
            dropped_tables.append(table_name)
        except Exception as e:
            print(f"  ❌ Failed to drop {full_table_name}: {str(e)}")
            failed_tables.append(table_name)

    print()

    # Clean up data files from MinIO
    print("Cleaning up data files from MinIO...")
    cleaned_paths = []
    failed_paths = []

    for table_name in bronze_tables:
        data_path = f"s3a://data/bronze/{table_name}"
        print(f"  Deleting: {data_path}")

        try:
            # Check if path exists
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(data_path),
                hadoop_conf
            )
            path = spark._jvm.org.apache.hadoop.fs.Path(data_path)

            if fs.exists(path):
                fs.delete(path, True)  # True = recursive delete
                print(f"  ✓ Deleted: {data_path}")
                cleaned_paths.append(data_path)
            else:
                print(f"  - Path does not exist (skipped): {data_path}")
                cleaned_paths.append(data_path)

        except Exception as e:
            print(f"  ❌ Failed to delete {data_path}: {str(e)}")
            failed_paths.append(data_path)

    # Summary
    print("\n" + "=" * 80)
    print("  Reset Summary")
    print("=" * 80)
    print(f"Tables dropped: {len(dropped_tables)}/{len(bronze_tables)}")
    print(f"Data paths cleaned: {len(cleaned_paths)}/{len(bronze_tables)}")

    if dropped_tables:
        print("\n✓ Dropped tables:")
        for table in dropped_tables:
            print(f"  - bronze.{table}")

    if failed_tables:
        print("\n❌ Failed to drop:")
        for table in failed_tables:
            print(f"  - bronze.{table}")

    if failed_paths:
        print("\n❌ Failed to clean paths:")
        for path in failed_paths:
            print(f"  - {path}")

    print("\n" + "=" * 80)
    if len(failed_tables) == 0 and len(failed_paths) == 0:
        print("  SUCCESS! Bronze layer reset complete")
        print("=" * 80)
        print("\nNext steps:")
        print("  1. Run create_bronze_tables.ipynb to recreate tables")
        print("  2. Run ./submit.sh ingest_bronze_data.py to load data")
    else:
        print("  COMPLETED WITH ERRORS")
        print("=" * 80)
        print(f"\nSome operations failed. Check logs above.")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()