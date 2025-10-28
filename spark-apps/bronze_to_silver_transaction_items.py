"""
Bronze to Silver - Transform Transaction Items

This script reads raw transaction items data from bronze.transaction_items_raw,
applies data quality transformations, type casting, and derives additional
business metrics to create the silver.transaction_items table.

Transformations applied:
- Type casting (string -> proper types: BIGINT, DECIMAL, INT, etc.)
- Extract numeric IDs from string prefixes
- Calculate derived quantities (net_quantity)
- Calculate line-level financial metrics (subtotal, discount, tax, total, profit, margin)
- Add metadata timestamps

Input: bronze.transaction_items_raw
Output: silver.transaction_items (Parquet, partitioned by transaction_id)

Usage:
    ./submit.sh bronze_to_silver_transaction_items.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, current_timestamp, lit, expr, coalesce
)

print("=" * 80)
print("  Bronze to Silver - Transaction Items Transformation")
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

try:
    print(f"\nStarting transformation at: {datetime.now()}")

    # Read from bronze
    print("\n[1/4] Reading from bronze.transaction_items_raw...")
    bronze_df = spark.table("bronze.transaction_items_raw")

    initial_count = bronze_df.count()
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Data transformations
    print("\n[2/4] Applying transformations...")

    # Cast types and create base fields
    # Extract numeric part from IDs (TXN004 -> 4, PROD001 -> 1, etc.)
    silver_df = bronze_df \
        .withColumn("transaction_item_id", expr("CAST(regexp_extract(transaction_item_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("transaction_id", expr("CAST(regexp_extract(transaction_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("product_id", expr("CAST(regexp_extract(product_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("variant_id", expr("CAST(regexp_extract(variant_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("warehouse_id", expr("CAST(regexp_extract(warehouse_id, '[0-9]+', 0) AS BIGINT)"))

    print("  ✓ ID extraction and casting complete")

    # Cast numeric fields
    silver_df = silver_df \
        .withColumn("quantity", col("quantity").cast("INT")) \
        .withColumn("return_quantity", coalesce(col("return_quantity").cast("INT"), lit(0))) \
        .withColumn("unit_price", col("unit_price").cast("DECIMAL(10,2)")) \
        .withColumn("discount_percentage", coalesce(col("discount_percentage").cast("DECIMAL(5,2)"), lit(0.0))) \
        .withColumn("tax_rate", coalesce(col("tax_rate").cast("DECIMAL(5,4)"), lit(0.0)))

    # Derive unit_cost (not in bronze - estimate as 70% of unit_price)
    # In production, this would come from a product cost table
    silver_df = silver_df \
        .withColumn("unit_cost", (col("unit_price") * lit(0.70)).cast("DECIMAL(10,2)"))

    print("  ✓ Numeric field casting complete")
    print("  ✓ Derived unit_cost (estimated as 70% of unit_price)")

    # Calculate derived quantities
    silver_df = silver_df \
        .withColumn("net_quantity", col("quantity") - col("return_quantity"))

    print("  ✓ Calculated net quantity")

    # Calculate line-level financial metrics
    # line_subtotal = quantity * unit_price
    silver_df = silver_df \
        .withColumn("line_subtotal", col("quantity") * col("unit_price"))

    # line_discount = line_subtotal * (discount_percentage / 100)
    silver_df = silver_df \
        .withColumn("line_discount", col("line_subtotal") * (col("discount_percentage") / 100))

    # line_tax = (line_subtotal - line_discount) * tax_rate
    silver_df = silver_df \
        .withColumn("line_tax", (col("line_subtotal") - col("line_discount")) * col("tax_rate"))

    # line_total = line_subtotal - line_discount + line_tax
    silver_df = silver_df \
        .withColumn("line_total", col("line_subtotal") - col("line_discount") + col("line_tax"))

    # line_cost = net_quantity * unit_cost
    silver_df = silver_df \
        .withColumn("line_cost", col("net_quantity") * col("unit_cost"))

    # line_profit = line_total - line_cost
    silver_df = silver_df \
        .withColumn("line_profit", col("line_total") - col("line_cost"))

    # line_margin_percentage = (line_profit / line_total) * 100
    # Handle division by zero
    silver_df = silver_df \
        .withColumn("line_margin_percentage",
                    when(col("line_total") > 0,
                         (col("line_profit") / col("line_total")) * 100)
                    .otherwise(lit(0.0)))

    print("  ✓ Calculated line-level financial metrics")

    # Add metadata
    silver_df = silver_df \
        .withColumn("source_system", col("_source_system")) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("updated_timestamp", current_timestamp())

    print("  ✓ Added metadata timestamps")

    # Select final columns in the correct order
    final_df = silver_df.select(
        "transaction_item_id",
        "transaction_id",
        "product_id",
        "variant_id",
        "quantity",
        "return_quantity",
        "net_quantity",
        "unit_price",
        "unit_cost",
        "discount_percentage",
        "tax_rate",
        "line_subtotal",
        "line_discount",
        "line_tax",
        "line_total",
        "line_cost",
        "line_profit",
        "line_margin_percentage",
        "return_reason",
        "fulfillment_status",
        "warehouse_id",
        "source_system",
        "created_timestamp",
        "updated_timestamp"
    )

    print("\n[3/4] Data quality checks...")
    final_count = final_df.count()
    null_item_ids = final_df.filter(col("transaction_item_id").isNull()).count()
    null_transaction_ids = final_df.filter(col("transaction_id").isNull()).count()
    null_product_ids = final_df.filter(col("product_id").isNull()).count()

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null transaction_item_id: {null_item_ids}")
    print(f"  ✓ Records with null transaction_id: {null_transaction_ids}")
    print(f"  ✓ Records with null product_id: {null_product_ids}")

    if null_item_ids > 0 or null_transaction_ids > 0 or null_product_ids > 0:
        print("  ⚠️  Warning: Found null values in key fields")

    # Calculate data quality percentage
    valid_records = final_count - max(null_item_ids, null_transaction_ids, null_product_ids)
    quality_pct = (valid_records / final_count * 100) if final_count > 0 else 0

    # Write to silver layer
    print("\n[4/4] Writing to silver.transaction_items...")
    print("  Partitioning by: transaction_id")

    cols = [col.col_name for col in
            spark.sql(f"SHOW COLUMNS IN silver.transaction_items").select('col_name').collect()]

    final_df = final_df.select([final_df[col] for col in cols])

    final_df.write.insertInto("silver.transaction_items", overwrite=True)

    print(f"  ✓ Successfully wrote {final_count:,} records to silver.transaction_items")

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.transaction_items")
    print("=" * 80)
    spark.table("silver.transaction_items").select(
        "transaction_item_id", "transaction_id", "product_id",
        "quantity", "unit_price", "line_total", "line_profit",
        "line_margin_percentage"
    ).show(5, truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Transformation Summary")
    print("=" * 80)
    print(f"Input records (bronze):  {initial_count:,}")
    print(f"Output records (silver): {final_count:,}")
    print(f"Data quality: {quality_pct:.2f}% valid records")

    # Financial metrics summary
    print("\nFinancial Metrics Summary:")
    metrics_df = spark.table("silver.transaction_items").selectExpr(
        "SUM(line_subtotal) as total_subtotal",
        "SUM(line_discount) as total_discount",
        "SUM(line_tax) as total_tax",
        "SUM(line_total) as total_revenue",
        "SUM(line_cost) as total_cost",
        "SUM(line_profit) as total_profit",
        "AVG(line_margin_percentage) as avg_margin_pct"
    )
    metrics_df.show(truncate=False)

    # Partition statistics
    print("\nPartition distribution:")
    spark.table("silver.transaction_items") \
        .groupBy("transaction_id") \
        .count() \
        .orderBy("transaction_id") \
        .show(10)

    print("\n" + "=" * 80)
    print("  SUCCESS! Transaction items transformed to silver layer")
    print("=" * 80)
    print("\nQuery example:")
    print("  spark.sql('SELECT * FROM silver.transaction_items WHERE line_profit > 100 LIMIT 10').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
