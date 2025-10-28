"""
Ingest Bronze Data - Load CSV data from MinIO into Bronze tables

This script reads CSV files from MinIO (s3a://data/source_data/) and
writes them to the bronze layer tables in Parquet format. The CSV files
already contain metadata columns (_source_system, _ingestion_timestamp,
_file_name, _record_offset).

Bronze tables must be created first using create_bronze_tables.ipynb.

Input: s3a://data/source_data/*.csv
Output: s3a://data/bronze/* (Parquet format, partitioned by _ingestion_timestamp)

Usage:
    ./submit.sh ingest_bronze_data.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp

print("=" * 80)
print("  Bronze Data Ingestion")
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
    {
        "name": "transactions_raw",
        "csv_file": "bronze_transactions_raw.csv",
        "database": "bronze"
    },
    {
        "name": "transaction_items_raw",
        "csv_file": "bronze_transaction_items_raw.csv",
        "database": "bronze"
    },
    {
        "name": "subscriptions_raw",
        "csv_file": "bronze_subscriptions_raw.csv",
        "database": "bronze"
    },
    {
        "name": "customer_interactions_raw",
        "csv_file": "bronze_customer_interactions_raw.csv",
        "database": "bronze"
    },
    {
        "name": "product_catalog_raw",
        "csv_file": "bronze_product_catalog_raw.csv",
        "database": "bronze"
    },
    {
        "name": "inventory_snapshots_raw",
        "csv_file": "bronze_inventory_snapshots_raw.csv",
        "database": "bronze"
    },
    {
        "name": "marketing_campaigns_raw",
        "csv_file": "bronze_marketing_campaigns_raw.csv",
        "database": "bronze"
    },
    {
        "name": "campaign_events_raw",
        "csv_file": "bronze_campaign_events_raw.csv",
        "database": "bronze"
    }
]

try:
    print(f"\nStarting ingestion at: {datetime.now()}")
    print(f"Total tables to process: {len(bronze_tables)}\n")

    total_records = 0
    successful_tables = []
    failed_tables = []

    for idx, table_config in enumerate(bronze_tables, 1):
        table_name = table_config["name"]
        csv_file = table_config["csv_file"]
        database = table_config["database"]
        full_table_name = f"{database}.{table_name}"

        print(f"[{idx}/{len(bronze_tables)}] Processing: {full_table_name}")
        print(f"  Source: s3a://data/source_data/{csv_file}")

        try:
            # Read CSV from MinIO
            input_path = f"s3a://data/source_data/{csv_file}"
            df = spark.read.csv(path=input_path,
                                sep='|',
                                header=True,
                                inferSchema=False, quote='"')
            record_count = df.count()
            print(f"  ✓ Read {record_count} records from CSV")

            df: DataFrame = df.withColumn("_ingestion_timestamp_casted",
                                          to_timestamp(col("_ingestion_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            df: DataFrame = df.withColumn('_record_offset_casted', df['_record_offset'].astype('bigint'))
            df = df.drop('_record_offset').withColumnRenamed('_record_offset_casted', '_record_offset') \
                .drop('_ingestion_timestamp').withColumnRenamed('_ingestion_timestamp_casted', '_ingestion_timestamp')

            cols = [col.col_name for col in
                    spark.sql(f"SHOW COLUMNS IN {full_table_name}").select('col_name').collect()]

            df = df.select([df[col] for col in cols])

            print(f"  Inserting into table: {full_table_name}")

            df.write \
                .insertInto(full_table_name, overwrite=True)

            print(f"  ✓ Successfully inserted {record_count} records into {full_table_name}\n")

            total_records += record_count
            successful_tables.append(table_name)

        except Exception as e:
            print(f"  ❌ Failed to process {full_table_name}: {str(e)}\n")
            failed_tables.append(table_name)
            continue

    # Summary
    print("=" * 80)
    print("  Ingestion Summary")
    print("=" * 80)
    print(f"Total records ingested: {total_records:,}")
    print(f"Successful tables: {len(successful_tables)}/{len(bronze_tables)}")

    if successful_tables:
        print("\n✓ Successfully ingested:")
        for table in successful_tables:
            print(f"  - {table}")

    if failed_tables:
        print("\n❌ Failed to ingest:")
        for table in failed_tables:
            print(f"  - {table}")

    print("\n" + "=" * 80)
    if len(failed_tables) == 0:
        print("  SUCCESS! All bronze tables ingested")
        print("=" * 80)
        print("\nView data in MinIO Console: http://localhost:9001 (admin/admin123)")
        print("\nNext steps:")
        print("  - Run create_silver_tables.ipynb to transform data to silver layer")
        print("  - Query tables: spark.sql('SELECT * FROM bronze.transactions_raw LIMIT 10').show()")
    else:
        print("  COMPLETED WITH ERRORS")
        print("=" * 80)
        print(f"\n{len(failed_tables)} table(s) failed to ingest. Check logs above.")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
