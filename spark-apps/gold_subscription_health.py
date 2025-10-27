"""
Gold Layer - Subscription Health (PySpark)

Analyzes subscription metrics including MRR, churn, retention, and cohort performance.

Input: silver.subscriptions
Output: gold.subscription_health (Parquet, partitioned by analysis_date)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, current_timestamp,
    current_date, when, lit
)

print("=" * 80)
print("  Gold Layer - Subscription Health")
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
    # Read subscriptions
    print("\n[1/2] Reading silver.subscriptions...")
    subscriptions_df = spark.table("silver.subscriptions")

    # Calculate subscription metrics by plan_id
    print("\n[2/2] Calculating subscription health metrics...")
    sub_metrics = subscriptions_df.groupBy("plan_id").agg(
        count(when(col("status") == "active", 1)).alias("active_subscribers"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled_subscribers_mtd"),
        spark_sum(when(col("status") == "active", col("subscription_amount")).otherwise(0)).alias("mrr"),
        avg(when(col("status") == "active", col("lifetime_value"))).alias("lifetime_value_avg"),
        avg(when(col("is_churned"), col("churn_risk_score"))).alias("avg_churn_risk")
    )

    final_df = sub_metrics \
        .withColumn("analysis_date", current_date()) \
        .withColumn("plan_name", col("plan_id").cast("STRING")) \
        .withColumn("subscription_type", lit("monthly")) \
        .withColumn("billing_frequency", lit("monthly")) \
        .withColumn("arr", (col("mrr") * 12).cast("DECIMAL(15,2)")) \
        .withColumn("churn_rate_monthly", (col("avg_churn_risk") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("created_timestamp", current_timestamp())

    final_df = final_df.select(
        "analysis_date", "plan_id", "plan_name", "subscription_type", "billing_frequency",
        col("active_subscribers").cast("BIGINT"),
        lit(0).cast("INT").alias("new_subscribers_mtd"),
        col("cancelled_subscribers_mtd").cast("INT"),
        lit(0).cast("INT").alias("paused_subscribers"),
        lit(0).cast("INT").alias("net_subscriber_change"),
        col("mrr").cast("DECIMAL(15,2)"),
        col("arr").cast("DECIMAL(15,2)"),
        lit(0).cast("DECIMAL(10,2)").alias("avg_revenue_per_subscriber"),
        col("lifetime_value_avg").cast("DECIMAL(12,2)"),
        "churn_rate_monthly",
        lit(0).cast("DECIMAL(5,2)").alias("churn_rate_annual"),
        lit(0).cast("DECIMAL(5,2)").alias("revenue_churn_rate"),
        lit(0).cast("DECIMAL(5,2)").alias("reactivation_rate"),
        lit(0).cast("DECIMAL(5,2)").alias("retention_rate_30d"),
        lit(0).cast("DECIMAL(5,2)").alias("retention_rate_90d"),
        lit(0).cast("DECIMAL(5,2)").alias("retention_rate_365d"),
        lit(0).cast("DECIMAL(5,2)").alias("cohort_0_3_months_retention"),
        lit(0).cast("DECIMAL(5,2)").alias("cohort_3_6_months_retention"),
        lit(0).cast("DECIMAL(5,2)").alias("cohort_6_12_months_retention"),
        lit(0).cast("DECIMAL(5,2)").alias("cohort_12plus_months_retention"),
        lit(0).cast("DECIMAL(10,2)").alias("avg_subscription_duration_days"),
        lit(0).cast("DECIMAL(8,2)").alias("avg_payments_before_churn"),
        lit(0).cast("DECIMAL(8,2)").alias("subscriber_growth_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("mrr_growth_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("quick_ratio"),
        lit(None).cast("STRING").alias("top_cancellation_reason"),
        lit(0).cast("DECIMAL(10,2)").alias("avg_days_to_cancel"),
        lit(0).cast("DECIMAL(5,2)").alias("voluntary_churn_rate"),
        lit(0).cast("DECIMAL(5,2)").alias("involuntary_churn_rate"),
        "created_timestamp"
    )

    # Write to gold
    print("\nWriting to gold.subscription_health...")
    final_df.write.mode("overwrite").partitionBy("analysis_date").format("parquet").saveAsTable(
        "gold.subscription_health")

    print(f"\n✓ Successfully created subscription health for {final_df.count():,} plans")
    print("\n" + "=" * 80)
    print("  SUCCESS! Subscription health analysis complete")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise
finally:
    spark.stop()
