"""
Bronze to Silver - Transform Transactions

This script reads raw transaction data from bronze.transactions_raw,
applies data quality transformations, type casting, and derives additional
business fields to create the silver.transactions table.

Transformations applied:
- Type casting (string -> proper types: BIGINT, TIMESTAMP, DECIMAL, etc.)
- Parse JSON coupon_codes array
- Derive temporal fields (year, month, quarter, day_of_week, hour, is_weekend, fiscal_year, fiscal_quarter)
- Add business flags (is_first_purchase, has_discount, has_coupon, is_return_transaction)
- Add metadata timestamps

Input: bronze.transactions_raw
Output: silver.transactions (Parquet, partitioned by transaction_year, transaction_month)

Usage:
    ./submit.sh bronze_to_silver_transactions.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, when,
    year, month, quarter, dayofweek, hour,
    current_timestamp, lit, expr, from_json
)
from pyspark.sql.types import ArrayType, StringType

print("=" * 80)
print("  Bronze to Silver - Transactions Transformation")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Memgraph
try:
    from lineage_listener import register_lineage_listener
    register_lineage_listener(spark)
except Exception as e:
    raise e

try:
    print(f"\nStarting transformation at: {datetime.now()}")

    # Read from bronze
    print("\n[1/4] Reading from bronze.transactions_raw...")
    bronze_df = spark.table("bronze.transactions_raw")

    initial_count = bronze_df.count()
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Data transformations
    print("\n[2/4] Applying transformations...")

    # Cast types and create base fields
    # Extract numeric part from IDs (TXN004 -> 4, CUST10004 -> 10004)
    silver_df = bronze_df \
        .withColumn("transaction_id", expr("CAST(regexp_extract(transaction_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("customer_id", expr("CAST(regexp_extract(customer_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("transaction_date", to_date(col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("store_id", col("store_id").cast("BIGINT")) \
        .withColumn("subtotal", col("subtotal").cast("DECIMAL(12,2)")) \
        .withColumn("tax_amount", col("tax_amount").cast("DECIMAL(12,2)")) \
        .withColumn("shipping_cost", col("shipping_cost").cast("DECIMAL(12,2)")) \
        .withColumn("discount_amount", col("discount_amount").cast("DECIMAL(12,2)")) \
        .withColumn("total_amount", col("total_amount").cast("DECIMAL(12,2)")) \
        .withColumn("loyalty_points_earned", col("loyalty_points_earned").cast("INT")) \
        .withColumn("loyalty_points_redeemed", col("loyalty_points_redeemed").cast("INT"))

    print("  ✓ Type casting complete")

    # Parse JSON coupon_codes array
    # Assuming coupon_codes is stored as JSON array string like '["CODE1", "CODE2"]'
    silver_df = silver_df \
        .withColumn("coupon_codes",
                    when(col("coupon_codes").isNotNull() & (col("coupon_codes") != ""),
                         from_json(col("coupon_codes"), ArrayType(StringType())))
                    .otherwise(lit(None).cast(ArrayType(StringType()))))

    print("  ✓ Parsed JSON coupon_codes array")

    # Derive temporal fields
    silver_df = silver_df \
        .withColumn("transaction_year", year(col("transaction_timestamp"))) \
        .withColumn("transaction_month", month(col("transaction_timestamp"))) \
        .withColumn("transaction_quarter", quarter(col("transaction_timestamp"))) \
        .withColumn("transaction_day_of_week", dayofweek(col("transaction_timestamp"))) \
        .withColumn("transaction_hour", hour(col("transaction_timestamp"))) \
        .withColumn("is_weekend", when(col("transaction_day_of_week").isin([1, 7]), True).otherwise(False))

    print("  ✓ Derived temporal fields")

    # Calculate fiscal year and quarter (assuming fiscal year starts in April)
    # Fiscal year: April to March (adjust as needed for your business)
    silver_df = silver_df \
        .withColumn("fiscal_year",
                    when(month(col("transaction_timestamp")) >= 4,
                         year(col("transaction_timestamp")))
                    .otherwise(year(col("transaction_timestamp")) - 1)) \
        .withColumn("fiscal_quarter",
                    when(month(col("transaction_timestamp")).isin([4, 5, 6]), 1)
                    .when(month(col("transaction_timestamp")).isin([7, 8, 9]), 2)
                    .when(month(col("transaction_timestamp")).isin([10, 11, 12]), 3)
                    .otherwise(4))

    print("  ✓ Calculated fiscal year and quarter")

    # Business flags
    silver_df = silver_df \
        .withColumn("has_discount", when(col("discount_amount") > 0, True).otherwise(False)) \
        .withColumn("has_coupon", when(col("coupon_codes").isNotNull(), True).otherwise(False)) \
        .withColumn("is_return_transaction", lit(False))  # Default to False; would need transaction_items to determine

    print("  ✓ Added business flags")

    # Determine is_first_purchase using window function
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

    silver_df = silver_df \
        .withColumn("row_num", expr("row_number() OVER (PARTITION BY customer_id ORDER BY transaction_timestamp)")) \
        .withColumn("is_first_purchase", when(col("row_num") == 1, True).otherwise(False)) \
        .drop("row_num")

    print("  ✓ Calculated is_first_purchase flag")

    # Add is_holiday flag (simplified - marking common US holidays)
    # In production, you'd join with a holiday calendar dimension table
    silver_df = silver_df \
        .withColumn("is_holiday",
                    when((month(col("transaction_date")) == 1) & (dayofweek(col("transaction_date")) == 1),
                         True)  # New Year's Day
                    .when((month(col("transaction_date")) == 7) & (dayofweek(col("transaction_date")) == 4),
                          True)  # Independence Day
                    .when((month(col("transaction_date")) == 12) & (dayofweek(col("transaction_date")) == 25),
                          True)  # Christmas
                    .otherwise(False))

    print("  ✓ Added holiday indicator")

    # Add metadata
    silver_df = silver_df \
        .withColumn("source_system", col("_source_system")) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("updated_timestamp", current_timestamp())

    print("  ✓ Added metadata timestamps")

    # Select final columns in the correct order
    final_df = silver_df.select(
        "transaction_id",
        "customer_id",
        "transaction_timestamp",
        "transaction_date",
        "channel",
        "store_id",
        "payment_method",
        "payment_status",
        "subtotal",
        "tax_amount",
        "shipping_cost",
        "discount_amount",
        "total_amount",
        "currency",
        "loyalty_points_earned",
        "loyalty_points_redeemed",
        "coupon_codes",
        "transaction_year",
        "transaction_month",
        "transaction_quarter",
        "transaction_day_of_week",
        "transaction_hour",
        "is_weekend",
        "is_holiday",
        "fiscal_year",
        "fiscal_quarter",
        "is_first_purchase",
        "is_return_transaction",
        "has_discount",
        "has_coupon",
        "source_system",
        "created_timestamp",
        "updated_timestamp"
    )

    print("\n[3/4] Data quality checks...")
    final_count = final_df.count()
    null_transaction_ids = final_df.filter(col("transaction_id").isNull()).count()
    null_customer_ids = final_df.filter(col("customer_id").isNull()).count()

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null transaction_id: {null_transaction_ids}")
    print(f"  ✓ Records with null customer_id: {null_customer_ids}")

    if null_transaction_ids > 0 or null_customer_ids > 0:
        print("  ⚠️  Warning: Found null values in key fields")

    # Write to silver layer
    print("\n[4/4] Writing to silver.transactions...")
    print("  Partitioning by: transaction_year, transaction_month")

    final_df.write \
        .mode("overwrite") \
        .partitionBy("transaction_year", "transaction_month") \
        .format("parquet") \
        .saveAsTable("silver.transactions")

    print(f"  ✓ Successfully wrote {final_count:,} records to silver.transactions")

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.transactions")
    print("=" * 80)
    spark.table("silver.transactions").select(
        "transaction_id", "customer_id", "transaction_date",
        "total_amount", "has_discount", "is_first_purchase",
        "transaction_year", "transaction_month"
    ).show(5, truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Transformation Summary")
    print("=" * 80)
    print(f"Input records (bronze):  {initial_count:,}")
    print(f"Output records (silver): {final_count:,}")
    print(
        f"Data quality: {((final_count - null_transaction_ids - null_customer_ids) / final_count * 100):.2f}% valid records")

    # Partition statistics
    print("\nPartition distribution:")
    spark.table("silver.transactions") \
        .groupBy("transaction_year", "transaction_month") \
        .count() \
        .orderBy("transaction_year", "transaction_month") \
        .show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Transactions transformed to silver layer")
    print("=" * 80)
    print("\nQuery example:")
    print("  spark.sql('SELECT * FROM silver.transactions WHERE is_first_purchase = true LIMIT 10').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
