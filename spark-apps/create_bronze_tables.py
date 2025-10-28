"""
Create Bronze Tables - Initialize Bronze Layer Schema

This script creates the bronze database and all bronze layer tables with raw data schemas.
The bronze layer stores data exactly as received from source systems with audit columns.

Tables created:
- transactions_raw
- transaction_items_raw
- subscriptions_raw
- customer_interactions_raw
- product_catalog_raw
- inventory_snapshots_raw
- marketing_campaigns_raw
- campaign_events_raw

Usage:
    ./submit.sh create_bronze_tables.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Create Bronze Tables")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name
if app_name is None:
    app_name = 'CreateBronzeTables'

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Memgraph
try:
    from lineage_listener import register_lineage_listener

    register_lineage_listener(spark)
except Exception as e:
    print(f"Warning: Could not register lineage listener: {e}")

try:
    print(f"\nStarting table creation at: {datetime.now()}")

    # Create bronze database
    print("\n[1/9] Creating bronze database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    print("  ✓ Bronze database ready")

    # Table definitions
    tables = {
        "transactions_raw": """
            CREATE TABLE IF NOT EXISTS bronze.transactions_raw
            (
                transaction_id STRING,
                customer_id STRING,
                transaction_timestamp STRING,
                channel STRING,
                store_id STRING,
                payment_method STRING,
                payment_status STRING,
                subtotal STRING,
                tax_amount STRING,
                shipping_cost STRING,
                discount_amount STRING,
                total_amount STRING,
                currency STRING,
                loyalty_points_earned STRING,
                loyalty_points_redeemed STRING,
                coupon_codes STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/transactions_raw'
        """,
        "transaction_items_raw": """
            CREATE TABLE IF NOT EXISTS bronze.transaction_items_raw
            (
                transaction_item_id STRING,
                transaction_id STRING,
                product_id STRING,
                variant_id STRING,
                quantity STRING,
                unit_price STRING,
                discount_percentage STRING,
                tax_rate STRING,
                return_quantity STRING,
                return_reason STRING,
                fulfillment_status STRING,
                warehouse_id STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/transaction_items_raw'
        """,
        "subscriptions_raw": """
            CREATE TABLE IF NOT EXISTS bronze.subscriptions_raw
            (
                subscription_id STRING,
                customer_id STRING,
                subscription_type STRING,
                plan_id STRING,
                start_date STRING,
                end_date STRING,
                status STRING,
                billing_frequency STRING,
                subscription_amount STRING,
                next_billing_date STRING,
                auto_renewal STRING,
                cancellation_date STRING,
                cancellation_reason STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/subscriptions_raw'
        """,
        "customer_interactions_raw": """
            CREATE TABLE IF NOT EXISTS bronze.customer_interactions_raw
            (
                interaction_id STRING,
                customer_id STRING,
                interaction_timestamp STRING,
                interaction_type STRING,
                channel STRING,
                agent_id STRING,
                category STRING,
                subcategory STRING,
                sentiment_score STRING,
                resolution_time_minutes STRING,
                satisfaction_rating STRING,
                notes STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/customer_interactions_raw'
        """,
        "product_catalog_raw": """
            CREATE TABLE IF NOT EXISTS bronze.product_catalog_raw
            (
                product_id STRING,
                product_name STRING,
                category_level1 STRING,
                category_level2 STRING,
                category_level3 STRING,
                brand STRING,
                manufacturer STRING,
                unit_cost STRING,
                list_price STRING,
                margin_percentage STRING,
                supplier_id STRING,
                lead_time_days STRING,
                weight_kg STRING,
                dimensions STRING,
                tags STRING,
                launch_date STRING,
                discontinuation_date STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/product_catalog_raw'
        """,
        "inventory_snapshots_raw": """
            CREATE TABLE IF NOT EXISTS bronze.inventory_snapshots_raw
            (
                snapshot_id STRING,
                snapshot_timestamp STRING,
                product_id STRING,
                variant_id STRING,
                warehouse_id STRING,
                quantity_on_hand STRING,
                quantity_reserved STRING,
                quantity_available STRING,
                reorder_point STRING,
                reorder_quantity STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/inventory_snapshots_raw'
        """,
        "marketing_campaigns_raw": """
            CREATE TABLE IF NOT EXISTS bronze.marketing_campaigns_raw
            (
                campaign_id STRING,
                campaign_name STRING,
                campaign_type STRING,
                channel STRING,
                start_date STRING,
                end_date STRING,
                budget STRING,
                target_audience STRING,
                creative_id STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/marketing_campaigns_raw'
        """,
        "campaign_events_raw": """
            CREATE TABLE IF NOT EXISTS bronze.campaign_events_raw
            (
                event_id STRING,
                campaign_id STRING,
                customer_id STRING,
                event_timestamp STRING,
                event_type STRING,
                device_type STRING,
                location STRING,
                attributed_revenue STRING,
                _source_system STRING,
                _ingestion_timestamp TIMESTAMP,
                _file_name STRING,
                _record_offset LONG
            )
            USING PARQUET
            PARTITIONED BY (_ingestion_timestamp)
            LOCATION 's3a://data/bronze/campaign_events_raw'
        """
    }

    # Create each table
    table_list = list(tables.keys())
    created_tables = []
    failed_tables = []

    for idx, (table_name, ddl) in enumerate(tables.items(), 2):
        print(f"\n[{idx}/9] Creating table: bronze.{table_name}")
        try:
            spark.sql(ddl)
            print(f"  ✓ Table created: bronze.{table_name}")
            created_tables.append(table_name)
        except Exception as e:
            print(f"  ❌ Failed to create {table_name}: {str(e)}")
            failed_tables.append(table_name)

    # Show created tables
    print("\n" + "=" * 80)
    print("  Bronze Tables Summary")
    print("=" * 80)

    tables_df = spark.sql("SHOW TABLES IN bronze")
    table_count = tables_df.count()
    print(f"\nTotal tables in bronze database: {table_count}")
    print("\nTables:")
    tables_df.show(truncate=False)

    # Final summary
    print("\n" + "=" * 80)
    if len(failed_tables) == 0:
        print("  SUCCESS! All bronze tables created")
        print("=" * 80)
        print(f"\nCreated {len(created_tables)}/{len(table_list)} tables:")
        for table in created_tables:
            print(f"  - bronze.{table}")
        print("\nNext steps:")
        print("  1. Run ./submit.sh ingest_bronze_data.py to load data")
        print("  2. Run ./submit.sh validate_bronze_data.py to validate")
    else:
        print("  COMPLETED WITH ERRORS")
        print("=" * 80)
        print(f"\nCreated: {len(created_tables)}/{len(table_list)} tables")
        print(f"Failed: {len(failed_tables)}/{len(table_list)} tables")
        if failed_tables:
            print("\nFailed tables:")
            for table in failed_tables:
                print(f"  - bronze.{table}")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
