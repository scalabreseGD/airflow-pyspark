"""
Gold Layer - Cohort Analysis (PySpark)

Analyzes customer cohorts based on their first transaction date.
Tracks cohort retention, lifetime value, and behavior over time.

Metrics calculated:
- Cohort size (number of customers per cohort)
- Monthly retention and churn rates (period-over-period and cumulative)
- Cohort revenue and orders (period and cumulative)
- Average revenue and orders per customer
- Repeat purchase rate and frequency
- Channel distribution (online, mobile, in-store percentages)
- Subscription adoption metrics

Input: silver.transactions, silver.subscriptions
Output: gold.cohort_analysis (Parquet, partitioned by cohort_month)

Usage:
    ./submit.sh gold_cohort_analysis.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, count, sum as spark_sum, avg, countDistinct,
    datediff, current_timestamp, lit, months_between, floor,
    when, last_day, add_months, to_date
)
from pyspark.sql.window import Window

print("=" * 80)
print("  Gold Layer - Cohort Analysis")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

try:
    print(f"\nStarting analysis at: {datetime.now()}")

    # Read silver transactions
    print("\n[1/7] Reading silver.transactions...")
    transactions_df = spark.table("silver.transactions")

    initial_count = transactions_df.count()
    print(f"  ✓ Read {initial_count:,} transactions")

    # Try to read subscriptions
    try:
        subscriptions_df = spark.table("silver.subscriptions")
        print(f"  ✓ Read subscriptions")
        has_subscriptions = True
    except:
        print("  ⚠ No subscriptions table found")
        has_subscriptions = False

    # Define cohort as the month of first purchase
    print("\n[2/7] Defining customer cohorts...")

    # Find first transaction date for each customer
    first_purchase_df = transactions_df.groupBy("customer_id").agg(
        spark_min("transaction_date").alias("first_purchase_date")
    )

    # Add cohort month (first day of the month)
    cohort_df = first_purchase_df \
        .withColumn("cohort_month", to_date(add_months(last_day(col("first_purchase_date")), -1).cast("date") + 1))

    cohort_count = cohort_df.count()
    print(f"  ✓ Identified {cohort_count:,} unique customers")

    # Show cohort distribution
    cohort_sizes = cohort_df.groupBy("cohort_month") \
        .count() \
        .orderBy("cohort_month")

    print("\nCohort sizes:")
    cohort_sizes.show(10, truncate=False)

    # Join transactions with cohort info
    print("\n[3/7] Analyzing cohort behavior...")

    transactions_with_cohort = transactions_df \
        .join(cohort_df, on="customer_id", how="left") \
        .withColumn("transaction_month", to_date(add_months(last_day(col("transaction_date")), -1).cast("date") + 1)) \
        .withColumn("months_since_first_purchase",
                    floor(months_between(col("transaction_month"), col("cohort_month"))).cast("INT"))

    # Get cohort initial size (period 0)
    cohort_initial_sizes = transactions_with_cohort \
        .filter(col("months_since_first_purchase") == 0) \
        .groupBy("cohort_month").agg(
        countDistinct("customer_id").alias("cohort_size")
    )

    print("  ✓ Calculated cohort initial sizes")

    # Aggregate metrics by cohort and period
    print("\n[4/7] Computing retention and revenue metrics...")

    cohort_metrics = transactions_with_cohort.groupBy(
        "cohort_month",
        "months_since_first_purchase"
    ).agg(
        countDistinct("customer_id").alias("customers_active"),
        count("transaction_id").alias("total_orders"),
        spark_sum("total_amount").alias("cohort_revenue"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct(when(col("channel") == "online", col("customer_id"))).alias("online_customers"),
        countDistinct(when(col("channel") == "mobile", col("customer_id"))).alias("mobile_customers"),
        countDistinct(when(col("channel") == "store", col("customer_id"))).alias("instore_customers"),
        avg(datediff(col("transaction_date"), col("first_purchase_date"))).alias("avg_days_between_purchases")
    )

    # Join with cohort initial sizes to calculate retention
    cohort_analysis_df = cohort_metrics \
        .join(cohort_initial_sizes, on="cohort_month", how="left") \
        .withColumn("retention_rate",
                    (col("customers_active") / col("cohort_size") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("churn_rate",
                    (100 - col("retention_rate")).cast("DECIMAL(5,2)")) \
        .withColumn("avg_revenue_per_customer",
                    (col("cohort_revenue") / col("customers_active")).cast("DECIMAL(10,2)")) \
        .withColumn("avg_orders_per_customer",
                    (col("total_orders") / col("customers_active")).cast("DECIMAL(8,2)")) \
        .withColumn("online_percentage",
                    (col("online_customers") / col("customers_active") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("mobile_percentage",
                    (col("mobile_customers") / col("customers_active") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("instore_percentage",
                    (col("instore_customers") / col("customers_active") * 100).cast("DECIMAL(5,2)"))

    print("  ✓ Calculated retention rates and channel distribution")

    # Calculate cumulative metrics
    print("\n[5/7] Calculating cumulative metrics...")

    window_spec = Window.partitionBy("cohort_month") \
        .orderBy("months_since_first_purchase") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Calculate repeat purchase rate (customers with > 1 order in this period)
    repeat_customers = transactions_with_cohort.groupBy(
        "cohort_month", "months_since_first_purchase", "customer_id"
    ).agg(
        count("*").alias("customer_orders")
    ).filter(col("customer_orders") > 1) \
        .groupBy("cohort_month", "months_since_first_purchase").agg(
        count("*").alias("repeat_customers_count")
    )

    cohort_analysis_df = cohort_analysis_df.join(
        repeat_customers, on=["cohort_month", "months_since_first_purchase"], how="left"
    ).withColumn("repeat_customers_count",
                 when(col("repeat_customers_count").isNull(), 0).otherwise(col("repeat_customers_count")))

    cohort_analysis_df = cohort_analysis_df \
        .withColumn("cumulative_orders",
                    spark_sum("total_orders").over(window_spec)) \
        .withColumn("cumulative_cohort_revenue",
                    spark_sum("cohort_revenue").over(window_spec)) \
        .withColumn("cumulative_avg_revenue_per_customer",
                    (col("cumulative_cohort_revenue") / col("cohort_size")).cast("DECIMAL(10,2)")) \
        .withColumn("cumulative_avg_orders_per_customer",
                    (col("cumulative_orders") / col("cohort_size")).cast("DECIMAL(8,2)")) \
        .withColumn("cumulative_retention_rate",
                    avg("retention_rate").over(window_spec).cast("DECIMAL(5,2)")) \
        .withColumn("cumulative_churn_rate",
                    (100 - col("cumulative_retention_rate")).cast("DECIMAL(5,2)")) \
        .withColumn("repeat_purchase_rate",
                    (col("repeat_customers_count") / col("customers_active") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("avg_purchase_frequency",
                    (col("total_orders") / col("customers_active")).cast("DECIMAL(8,2)"))

    print("  ✓ Added cumulative metrics")

    # Add subscription adoption if available
    print("\n[6/7] Adding subscription adoption metrics...")

    if has_subscriptions:
        # Calculate subscription adoption by cohort and period
        subscription_adoption = subscriptions_df.alias("s").join(
            cohort_df.alias("c"), on="customer_id", how="left"
        ).withColumn("subscription_month", to_date(add_months(last_day(col("start_date")), -1).cast("date") + 1)) \
            .withColumn("months_since_first_purchase",
                        floor(months_between(col("subscription_month"), col("cohort_month"))).cast("INT")) \
            .groupBy("cohort_month", "months_since_first_purchase").agg(
            countDistinct("customer_id").alias("subscribers"),
            avg("subscription_amount").alias("avg_sub_value")
        )

        final_df = cohort_analysis_df.join(
            subscription_adoption, on=["cohort_month", "months_since_first_purchase"], how="left"
        )

        final_df = final_df \
            .withColumn("subscribers", when(col("subscribers").isNull(), 0).otherwise(col("subscribers"))) \
            .withColumn("subscription_adoption_rate",
                        (col("subscribers") / col("customers_active") * 100).cast("DECIMAL(5,2)")) \
            .withColumn("avg_subscription_value",
                        col("avg_sub_value").cast("DECIMAL(10,2)"))
    else:
        final_df = cohort_analysis_df \
            .withColumn("subscription_adoption_rate", lit(0).cast("DECIMAL(5,2)")) \
            .withColumn("avg_subscription_value", lit(0).cast("DECIMAL(10,2)"))

    # Add metadata
    final_df = final_df.withColumn("created_timestamp", current_timestamp())

    # Select final columns matching DDL
    final_df = final_df.select(
        "cohort_month",
        "months_since_first_purchase",
        "cohort_size",
        "customers_active",
        "retention_rate",
        "cumulative_retention_rate",
        "churn_rate",
        "cumulative_churn_rate",
        col("cohort_revenue").cast("DECIMAL(15,2)"),
        col("cumulative_cohort_revenue").cast("DECIMAL(15,2)"),
        "avg_revenue_per_customer",
        "cumulative_avg_revenue_per_customer",
        "total_orders",
        "cumulative_orders",
        "avg_orders_per_customer",
        "cumulative_avg_orders_per_customer",
        "repeat_purchase_rate",
        "avg_purchase_frequency",
        col("avg_days_between_purchases").cast("DECIMAL(8,2)"),
        "online_percentage",
        "mobile_percentage",
        "instore_percentage",
        "subscription_adoption_rate",
        "avg_subscription_value",
        "created_timestamp"
    )

    print("  ✓ Finalized cohort metrics")

    # Write to gold layer
    print("\n[7/7] Writing to gold.cohort_analysis...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("cohort_month") \
        .format("parquet") \
        .saveAsTable("gold.cohort_analysis")

    final_count = final_df.count()
    print(f"  ✓ Successfully wrote {final_count:,} cohort period records")

    # Display retention analysis
    print("\n" + "=" * 80)
    print("  Cohort Retention Analysis")
    print("=" * 80)

    # Show retention for a recent cohort
    print("\nRetention curve for most recent cohort:")
    latest_cohort = final_df.select(spark_max("cohort_month").alias("max_cohort")).first()["max_cohort"]

    final_df.filter(col("cohort_month") == latest_cohort) \
        .orderBy("months_since_first_purchase") \
        .select(
        "months_since_first_purchase",
        "cohort_size",
        "customers_active",
        "retention_rate",
        "cumulative_avg_revenue_per_customer"
    ).show(12, truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Cohort Analysis Summary")
    print("=" * 80)

    cohort_summary = final_df.groupBy("cohort_month").agg(
        spark_max("cohort_size").alias("cohort_size"),
        spark_max("cumulative_avg_revenue_per_customer").alias("max_ltv"),
        spark_max("cumulative_orders").alias("total_orders")
    ).orderBy("cohort_month")

    print("\nCohort Lifetime Value Summary:")
    cohort_summary.show(10, truncate=False)

    # Overall metrics
    total_cohorts = cohort_summary.count()
    avg_cohort_size = cohort_summary.select(avg("cohort_size")).first()[0]

    print(f"\nTotal cohorts analyzed: {total_cohorts}")
    print(f"Average cohort size: {avg_cohort_size:.1f} customers")

    print("\n" + "=" * 80)
    print("  SUCCESS! Cohort analysis complete")
    print("=" * 80)
    print("\nQuery examples:")
    print(
        "  spark.sql('SELECT * FROM gold.cohort_analysis WHERE months_since_first_purchase <= 6 ORDER BY cohort_month, months_since_first_purchase').show()")
    print(
        "  spark.sql('SELECT cohort_month, MAX(cumulative_avg_revenue_per_customer) as ltv FROM gold.cohort_analysis GROUP BY cohort_month ORDER BY cohort_month').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
