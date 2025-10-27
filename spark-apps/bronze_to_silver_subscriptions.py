"""
Bronze to Silver - Transform Subscriptions

This script reads raw subscription data from bronze.subscriptions_raw,
applies data quality transformations, type casting, and calculates business
metrics to create the silver.subscriptions table with SCD Type 2 support.

Transformations applied:
- Type casting (string -> proper types: BIGINT, DATE, DECIMAL, BOOLEAN, etc.)
- Extract numeric IDs from string prefixes
- Calculate subscription metrics (duration, payments, lifetime value)
- Calculate churn analysis (is_churned, churn_risk_score)
- Add SCD Type 2 fields (effective dates, is_current)
- Add metadata timestamps

Input: bronze.subscriptions_raw
Output: silver.subscriptions (Parquet, partitioned by status, is_current)

Usage:
    ./submit.sh bronze_to_silver_subscriptions.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, current_timestamp, lit, expr,
    coalesce, datediff, floor
)

print("=" * 80)
print("  Bronze to Silver - Subscriptions Transformation")
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
    print("\n[1/5] Reading from bronze.subscriptions_raw...")
    bronze_df = spark.table("bronze.subscriptions_raw")

    initial_count = bronze_df.count()
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Data transformations
    print("\n[2/5] Applying transformations...")

    # Extract numeric IDs from string prefixes
    silver_df = bronze_df \
        .withColumn("subscription_id", expr("CAST(regexp_extract(subscription_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("customer_id", expr("CAST(regexp_extract(customer_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("plan_id", expr("CAST(regexp_extract(plan_id, '[0-9]+', 0) AS BIGINT)"))

    print("  ✓ ID extraction and casting complete")

    # Cast date fields
    silver_df = silver_df \
        .withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd")) \
        .withColumn("end_date", to_date(col("end_date"), "yyyy-MM-dd")) \
        .withColumn("cancellation_date",
                    when(col("cancellation_date").isNotNull() & (col("cancellation_date") != ""),
                         to_date(col("cancellation_date"), "yyyy-MM-dd"))
                    .otherwise(lit(None))) \
        .withColumn("next_billing_date", to_date(col("next_billing_date"), "yyyy-MM-dd"))

    print("  ✓ Date casting complete")

    # Cast numeric and boolean fields
    silver_df = silver_df \
        .withColumn("subscription_amount", col("subscription_amount").cast("DECIMAL(10,2)")) \
        .withColumn("auto_renewal",
                    when(col("auto_renewal").isin(["true", "True", "1", "yes", "Yes"]), True)
                    .otherwise(False))

    print("  ✓ Numeric and boolean casting complete")

    # Calculate subscription_duration_days
    silver_df = silver_df \
        .withColumn("subscription_duration_days",
                    coalesce(datediff(col("end_date"), col("start_date")), lit(0)))

    print("  ✓ Calculated subscription duration")

    # Calculate total_payments_made based on billing_frequency and duration
    # Assuming monthly = 1 payment per month, quarterly = 1 per 3 months, annually = 1 per year
    silver_df = silver_df \
        .withColumn("total_payments_made",
                    when(col("billing_frequency") == "monthly",
                         floor(col("subscription_duration_days") / 30))
                    .when(col("billing_frequency") == "quarterly",
                          floor(col("subscription_duration_days") / 90))
                    .when(col("billing_frequency") == "annually",
                          floor(col("subscription_duration_days") / 365))
                    .otherwise(0)
                    .cast("INT"))

    # Ensure at least 1 payment if subscription has started
    silver_df = silver_df \
        .withColumn("total_payments_made",
                    when((col("subscription_duration_days") > 0) & (col("total_payments_made") == 0), 1)
                    .otherwise(col("total_payments_made")))

    print("  ✓ Calculated total payments made")

    # Calculate lifetime_value
    silver_df = silver_df \
        .withColumn("lifetime_value",
                    (col("subscription_amount") * col("total_payments_made")).cast("DECIMAL(12,2)"))

    print("  ✓ Calculated lifetime value")

    print("\n[3/5] Churn analysis...")

    # Determine is_churned (if subscription is cancelled or ended)
    silver_df = silver_df \
        .withColumn("is_churned",
                    when((col("cancellation_date").isNotNull()) |
                         (col("status").isin(["cancelled", "expired"])), True)
                    .otherwise(False))

    # Calculate churn_risk_score (0.0 to 1.0)
    # Higher risk if:
    # - No auto_renewal
    # - Status is pending cancellation
    # - Close to end_date
    # - Low number of payments
    silver_df = silver_df \
        .withColumn("days_until_end",
                    coalesce(datediff(col("end_date"), current_timestamp().cast("date")), lit(999))) \
        .withColumn("churn_risk_score",
                    # Start with base risk
                    when(col("is_churned"), lit(1.0))
                    # High risk if status is concerning
                    .when(col("status").isin(["pending_cancellation", "payment_failed"]), lit(0.8))
                    # Medium-high risk if no auto renewal and close to end
                    .when((col("auto_renewal") == False) & (col("days_until_end") < 30), lit(0.7))
                    # Medium risk if no auto renewal
                    .when(col("auto_renewal") == False, lit(0.5))
                    # Low-medium risk if few payments made
                    .when(col("total_payments_made") < 3, lit(0.4))
                    # Low risk otherwise
                    .otherwise(lit(0.2))
                    .cast("DECIMAL(3,2)")) \
        .drop("days_until_end")

    print("  ✓ Calculated churn indicators")

    print("\n[4/5] Adding SCD Type 2 fields...")

    # SCD Type 2 - All current records
    # In production, you'd implement proper SCD logic with historical tracking
    silver_df = silver_df \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast("TIMESTAMP")) \
        .withColumn("is_current", lit(True))

    print("  ✓ Added SCD Type 2 fields")

    # Add metadata
    silver_df = silver_df \
        .withColumn("source_system", col("_source_system")) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("updated_timestamp", current_timestamp())

    print("  ✓ Added metadata timestamps")

    # Select final columns in the correct order
    final_df = silver_df.select(
        "subscription_id",
        "customer_id",
        "subscription_type",
        "plan_id",
        "start_date",
        "end_date",
        "cancellation_date",
        "next_billing_date",
        "status",
        "billing_frequency",
        "auto_renewal",
        "cancellation_reason",
        "subscription_amount",
        "subscription_duration_days",
        "total_payments_made",
        "lifetime_value",
        "is_churned",
        "churn_risk_score",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "source_system",
        "created_timestamp",
        "updated_timestamp"
    )

    print("\n[5/5] Data quality checks...")
    final_count = final_df.count()
    null_subscription_ids = final_df.filter(col("subscription_id").isNull()).count()
    null_customer_ids = final_df.filter(col("customer_id").isNull()).count()

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null subscription_id: {null_subscription_ids}")
    print(f"  ✓ Records with null customer_id: {null_customer_ids}")

    if null_subscription_ids > 0 or null_customer_ids > 0:
        print("  ⚠️  Warning: Found null values in key fields")

    # Calculate data quality percentage
    valid_records = final_count - max(null_subscription_ids, null_customer_ids)
    quality_pct = (valid_records / final_count * 100) if final_count > 0 else 0

    # Write to silver layer
    print("\n[6/6] Writing to silver.subscriptions...")
    print("  Partitioning by: status, is_current")

    final_df.write \
        .mode("overwrite") \
        .partitionBy("status", "is_current") \
        .format("parquet") \
        .saveAsTable("silver.subscriptions")

    print(f"  ✓ Successfully wrote {final_count:,} records to silver.subscriptions")

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.subscriptions")
    print("=" * 80)
    spark.table("silver.subscriptions").select(
        "subscription_id", "customer_id", "subscription_type", "status",
        "subscription_amount", "lifetime_value", "is_churned", "churn_risk_score"
    ).show(5, truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Transformation Summary")
    print("=" * 80)
    print(f"Input records (bronze):  {initial_count:,}")
    print(f"Output records (silver): {final_count:,}")
    print(f"Data quality: {quality_pct:.2f}% valid records")

    # Business metrics summary
    print("\nBusiness Metrics Summary:")
    metrics_df = spark.table("silver.subscriptions").selectExpr(
        "COUNT(*) as total_subscriptions",
        "SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) as churned_count",
        "AVG(subscription_amount) as avg_subscription_amount",
        "SUM(lifetime_value) as total_lifetime_value",
        "AVG(churn_risk_score) as avg_churn_risk",
        "AVG(subscription_duration_days) as avg_duration_days"
    )
    metrics_df.show(truncate=False)

    # Status distribution
    print("\nStatus distribution:")
    spark.table("silver.subscriptions") \
        .groupBy("status") \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    # Churn analysis
    print("\nChurn analysis:")
    spark.table("silver.subscriptions") \
        .groupBy("is_churned") \
        .agg({"*": "count", "churn_risk_score": "avg"}) \
        .show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Subscriptions transformed to silver layer")
    print("=" * 80)
    print("\nQuery example:")
    print("  spark.sql('SELECT * FROM silver.subscriptions WHERE churn_risk_score > 0.6 LIMIT 10').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
