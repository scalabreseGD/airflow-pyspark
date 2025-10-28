"""
Validate Bronze Data - Check all bronze tables have data

This script validates that all bronze tables contain data before
proceeding with silver transformations. It will FAIL if any table is empty.

Bronze tables checked:
- transactions_raw
- transaction_items_raw
- subscriptions_raw
- product_catalog_raw
- inventory_snapshots_raw
- customer_interactions_raw

Usage:
    ./submit.sh validate_bronze_data.py

Exit codes:
    0 - All tables have data (SUCCESS)
    1 - One or more tables are empty (FAILURE)
"""

import argparse
import sys

from pyspark.sql import SparkSession

print("=" * 80)
print("  BRONZE DATA VALIDATION")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Neo4j
try:
    from lineage_listener import register_lineage_listener

    register_lineage_listener(spark)
except Exception as e:
    raise e

# Define the bronze tables and their corresponding CSV files
bronze_tables = [
    "transactions_raw",
    "transaction_items_raw",
    "subscriptions_raw",
    "product_catalog_raw",
    "inventory_snapshots_raw",
    "customer_interactions_raw"
]

validation_results = {}
all_valid = True

print(f"\nValidating {len(bronze_tables)} bronze tables...\n")

# Check each table
for table in bronze_tables:
    full_table_name = f"bronze.{table}"

    try:
        # Get record count
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']

        if count == 0:
            validation_results[table] = {
                'status': 'EMPTY',
                'count': count,
                'valid': False
            }
            all_valid = False
            print(f"❌ {table:<30} EMPTY (0 records)")
        else:
            validation_results[table] = {
                'status': 'OK',
                'count': count,
                'valid': True
            }
            print(f"✓  {table:<30} OK ({count:,} records)")

    except Exception as e:
        validation_results[table] = {
            'status': 'ERROR',
            'count': 0,
            'valid': False,
            'error': str(e)
        }
        all_valid = False
        print(f"❌ {table:<30} ERROR: {str(e)}")

# Summary
print("\n" + "=" * 80)
print("  VALIDATION SUMMARY")
print("=" * 80)

valid_count = sum(1 for r in validation_results.values() if r['valid'])
total_count = len(validation_results)
total_records = sum(r['count'] for r in validation_results.values())

print(f"\nTables validated: {valid_count}/{total_count}")
print(f"Total records across all tables: {total_records:,}")

# List any problems
empty_tables = [table for table, result in validation_results.items() if result['status'] == 'EMPTY']
error_tables = [table for table, result in validation_results.items() if result['status'] == 'ERROR']

if empty_tables:
    print(f"\n⚠️  EMPTY TABLES ({len(empty_tables)}):")
    for table in empty_tables:
        print(f"   - bronze.{table}")

if error_tables:
    print(f"\n⚠️  TABLES WITH ERRORS ({len(error_tables)}):")
    for table in error_tables:
        error = validation_results[table].get('error', 'Unknown error')
        print(f"   - bronze.{table}: {error}")

print("\n" + "=" * 80)

# Final result
if all_valid:
    print("  ✓ SUCCESS - All bronze tables have data")
    print("=" * 80)
    spark.stop()
    sys.exit(0)
else:
    print("  ❌ FAILURE - One or more bronze tables are empty or have errors")
    print("=" * 80)
    print("\nAction required:")
    print("  - Run bronze data ingestion DAG first")
    print("  - Verify data source availability")
    print("  - Check ingestion logs for errors")
    spark.stop()
    sys.exit(1)
