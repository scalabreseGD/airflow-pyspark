"""
Create Silver Tables - Initialize Silver Layer Schema

This script creates the silver database and all silver layer tables with cleaned,
validated, and typed schemas. The silver layer contains business-ready data with
proper data types and transformations applied.

Tables created:
- transactions
- transaction_items
- subscriptions
- customer_interactions
- product_catalog
- inventory_snapshots

Usage:
    ./submit.sh create_silver_tables.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Create Silver Tables")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name
if app_name is None:
    app_name = 'CreateSilverTables'

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

    # Create silver database
    print("\n[1/7] Creating silver database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    print("  ✓ Silver database ready")

    # Table definitions
    tables = {
        "transactions": """
            CREATE TABLE IF NOT EXISTS silver.transactions (
                transaction_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                transaction_timestamp TIMESTAMP,
                transaction_date DATE,
                channel STRING,
                store_id BIGINT,
                payment_method STRING,
                payment_status STRING,
                subtotal DECIMAL(12,2),
                tax_amount DECIMAL(12,2),
                shipping_cost DECIMAL(12,2),
                discount_amount DECIMAL(12,2),
                total_amount DECIMAL(12,2),
                currency STRING,
                loyalty_points_earned INT,
                loyalty_points_redeemed INT,
                coupon_codes ARRAY<STRING>,
                transaction_year INT,
                transaction_month INT,
                transaction_quarter INT,
                transaction_day_of_week INT,
                transaction_hour INT,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                fiscal_year INT,
                fiscal_quarter INT,
                is_first_purchase BOOLEAN,
                is_return_transaction BOOLEAN,
                has_discount BOOLEAN,
                has_coupon BOOLEAN,
                source_system STRING,
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (transaction_year, transaction_month)
            LOCATION 's3a://data/silver/transactions'
        """,
        "transaction_items": """
            CREATE TABLE IF NOT EXISTS silver.transaction_items (
                transaction_item_id BIGINT NOT NULL,
                transaction_id BIGINT NOT NULL,
                product_id BIGINT NOT NULL,
                variant_id BIGINT,
                quantity INT,
                return_quantity INT,
                net_quantity INT,
                unit_price DECIMAL(10,2),
                unit_cost DECIMAL(10,2),
                discount_percentage DECIMAL(5,2),
                tax_rate DECIMAL(5,4),
                line_subtotal DECIMAL(12,2),
                line_discount DECIMAL(12,2),
                line_tax DECIMAL(12,2),
                line_total DECIMAL(12,2),
                line_cost DECIMAL(12,2),
                line_profit DECIMAL(12,2),
                line_margin_percentage DECIMAL(5,2),
                return_reason STRING,
                fulfillment_status STRING,
                warehouse_id BIGINT,
                source_system STRING,
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (transaction_id)
            LOCATION 's3a://data/silver/transaction_items'
        """,
        "subscriptions": """
            CREATE TABLE IF NOT EXISTS silver.subscriptions (
                subscription_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                subscription_type STRING,
                plan_id BIGINT,
                start_date DATE,
                end_date DATE,
                cancellation_date DATE,
                next_billing_date DATE,
                status STRING,
                billing_frequency STRING,
                auto_renewal BOOLEAN,
                cancellation_reason STRING,
                subscription_amount DECIMAL(10,2),
                subscription_duration_days INT,
                total_payments_made INT,
                lifetime_value DECIMAL(12,2),
                is_churned BOOLEAN,
                churn_risk_score DECIMAL(3,2),
                effective_start_date TIMESTAMP,
                effective_end_date TIMESTAMP,
                is_current BOOLEAN,
                source_system STRING,
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (status, is_current)
            LOCATION 's3a://data/silver/subscriptions'
        """,
        "customer_interactions": """
            CREATE TABLE IF NOT EXISTS silver.customer_interactions (
                interaction_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                interaction_timestamp TIMESTAMP,
                interaction_date DATE,
                interaction_type STRING,
                channel STRING,
                agent_id BIGINT,
                category STRING,
                subcategory STRING,
                sentiment_score DECIMAL(3,2),
                resolution_time_minutes INT,
                satisfaction_rating INT,
                interaction_year INT,
                interaction_month INT,
                is_resolved BOOLEAN,
                is_escalated BOOLEAN,
                source_system STRING,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (interaction_year, interaction_month)
            LOCATION 's3a://data/silver/customer_interactions'
        """,
        "product_catalog": """
            CREATE TABLE IF NOT EXISTS silver.product_catalog (
                product_id BIGINT NOT NULL,
                product_name STRING,
                category_level1 STRING,
                category_level2 STRING,
                category_level3 STRING,
                full_category_path STRING,
                brand STRING,
                manufacturer STRING,
                unit_cost DECIMAL(10,2),
                list_price DECIMAL(10,2),
                margin_percentage DECIMAL(5,2),
                price_tier STRING,
                supplier_id BIGINT,
                lead_time_days INT,
                weight_kg DECIMAL(8,3),
                launch_date DATE,
                discontinuation_date DATE,
                is_active BOOLEAN,
                product_age_days INT,
                lifecycle_stage STRING,
                tags ARRAY<STRING>,
                effective_start_date TIMESTAMP,
                effective_end_date TIMESTAMP,
                is_current BOOLEAN,
                source_system STRING,
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (category_level1, is_current)
            LOCATION 's3a://data/silver/product_catalog'
        """,
        "inventory_snapshots": """
            CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
                snapshot_id BIGINT NOT NULL,
                snapshot_timestamp TIMESTAMP,
                snapshot_date DATE,
                product_id BIGINT NOT NULL,
                variant_id BIGINT,
                warehouse_id BIGINT NOT NULL,
                quantity_on_hand INT,
                quantity_reserved INT,
                quantity_available INT,
                reorder_point INT,
                reorder_quantity INT,
                days_of_supply INT,
                stock_status STRING,
                is_stockout BOOLEAN,
                stockout_duration_days INT,
                source_system STRING,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (snapshot_date, warehouse_id)
            LOCATION 's3a://data/silver/inventory_snapshots'
        """
    }

    # Create each table
    table_list = list(tables.keys())
    created_tables = []
    failed_tables = []

    for idx, (table_name, ddl) in enumerate(tables.items(), 2):
        print(f"\n[{idx}/7] Creating table: silver.{table_name}")
        try:
            spark.sql(ddl)
            print(f"  ✓ Table created: silver.{table_name}")
            created_tables.append(table_name)
        except Exception as e:
            print(f"  ❌ Failed to create {table_name}: {str(e)}")
            failed_tables.append(table_name)

    # Show created tables
    print("\n" + "=" * 80)
    print("  Silver Tables Summary")
    print("=" * 80)

    tables_df = spark.sql("SHOW TABLES IN silver")
    table_count = tables_df.count()
    print(f"\nTotal tables in silver database: {table_count}")
    print("\nTables:")
    tables_df.show(truncate=False)

    # Final summary
    print("\n" + "=" * 80)
    if len(failed_tables) == 0:
        print("  SUCCESS! All silver tables created")
        print("=" * 80)
        print(f"\nCreated {len(created_tables)}/{len(table_list)} tables:")
        for table in created_tables:
            print(f"  - silver.{table}")
        print("\nNext steps:")
        print("  1. Ensure bronze tables are populated")
        print("  2. Run bronze_to_silver_*.py scripts to load data")
    else:
        print("  COMPLETED WITH ERRORS")
        print("=" * 80)
        print(f"\nCreated: {len(created_tables)}/{len(table_list)} tables")
        print(f"Failed: {len(failed_tables)}/{len(table_list)} tables")
        if failed_tables:
            print("\nFailed tables:")
            for table in failed_tables:
                print(f"  - silver.{table}")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
