"""
Reset Gold Tables - Drop all gold tables and optionally clean data

This script drops all gold layer tables and optionally deletes the
underlying data files from MinIO. Use this to start fresh with gold
layer analytics tables.

WARNING: This will remove all gold tables and optionally delete data!

Usage:
    ./submit.sh reset_gold_tables.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Reset Gold Tables")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name
if app_name is None:
    app_name = 'ResetGoldTables'

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Gold tables to drop
gold_tables = [
    "basket_analysis",
    "campaign_roi_analysis",
    "category_brand_performance",
    "channel_attribution",
    "cohort_analysis",
    "customer_360",
    "product_performance",
    "store_performance",
    "subscription_health"
]

try:
    print(f"\nStarting reset at: {datetime.now()}")
    print(f"Tables to drop: {len(gold_tables)}\n")

    dropped_tables = []
    failed_tables = []

    # Drop all gold tables
    for idx, table_name in enumerate(gold_tables, 1):
        full_table_name = f"gold.{table_name}"
        print(f"[{idx}/{len(gold_tables)}] Dropping table: {full_table_name}")

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

    for table_name in gold_tables:
        data_path = f"s3a://data/gold/{table_name}"
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
    print(f"Tables dropped: {len(dropped_tables)}/{len(gold_tables)}")
    print(f"Data paths cleaned: {len(cleaned_paths)}/{len(gold_tables)}")

    if dropped_tables:
        print("\n✓ Dropped tables:")
        for table in dropped_tables:
            print(f"  - gold.{table}")

    if failed_tables:
        print("\n❌ Failed to drop:")
        for table in failed_tables:
            print(f"  - gold.{table}")

    if failed_paths:
        print("\n❌ Failed to clean paths:")
        for path in failed_paths:
            print(f"  - {path}")

    print("\n" + "=" * 80)
    if len(failed_tables) == 0 and len(failed_paths) == 0:
        print("  SUCCESS! Gold layer reset complete")
        print("=" * 80)
        print("\nNext steps:")
        print("  1. Ensure silver tables are populated")
        print("  2. Run gold_*.py scripts to recreate gold tables")
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
