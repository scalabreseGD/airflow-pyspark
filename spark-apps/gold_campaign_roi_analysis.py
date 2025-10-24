"""
Gold Layer - Campaign ROI Analysis (PySpark)

Analyzes marketing campaign effectiveness and ROI metrics.

NOTE: This implementation simulates campaign marketing data based on transaction patterns.
In a production environment, you would join with a campaigns table containing:
- Campaign budgets, impressions, clicks from marketing platforms
- Campaign metadata (name, type, channel, dates)
- Actual marketing spend data

For this demo, we derive campaign metrics from coupon codes and transaction patterns.

Input: silver.transactions, silver.customer_360 (if exists, else transactions only)
Output: gold.campaign_roi_analysis (Parquet, partitioned by analysis_date)

Usage:
    ./submit.sh gold_campaign_roi_analysis.py
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, lit,
    current_timestamp, current_date, when, explode, size,
    min as spark_min, max as spark_max, datediff, rand, expr,
    dense_rank
)
from pyspark.sql.window import Window

print("=" * 80)
print("  Gold Layer - Campaign ROI Analysis")
print("=" * 80)

spark = SparkSession.builder \
    .appName("GoldCampaignROI") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    print(f"\nStarting analysis at: {datetime.now()}")

    # Read silver tables
    print("\n[1/6] Reading silver layer data...")
    transactions_df = spark.table("silver.transactions")

    print(f"  ✓ Transactions: {transactions_df.count():,} records")

    # Extract campaigns from coupon codes
    print("\n[2/6] Extracting campaign data from transactions...")

    # Explode coupon_codes array to get individual campaigns
    campaigns_with_transactions = transactions_df \
        .filter((col("coupon_codes").isNotNull()) & (size(col("coupon_codes")) > 0)) \
        .withColumn("coupon_code", explode(col("coupon_codes"))) \
        .withColumn("campaign_id", expr("CAST(regexp_extract(coupon_code, '[0-9]+', 0) AS BIGINT)")) \
        .filter(col("campaign_id").isNotNull())

    # Add control group (no campaign)
    no_campaign_txns = transactions_df \
        .filter((col("coupon_codes").isNull()) | (size(col("coupon_codes")) == 0)) \
        .withColumn("campaign_id", lit(0)) \
        .withColumn("coupon_code", lit("ORGANIC"))

    # Union both datasets
    all_campaign_txns = campaigns_with_transactions.unionByName(
        no_campaign_txns.select(campaigns_with_transactions.columns),
        allowMissingColumns=True
    )

    print(f"  ✓ Identified transactions with campaign attribution")

    # Calculate campaign-level metrics
    print("\n[3/6] Calculating campaign attribution metrics...")

    campaign_metrics = all_campaign_txns.groupBy("campaign_id", "coupon_code", "channel").agg(
        countDistinct("transaction_id").alias("attributed_transactions"),
        countDistinct("customer_id").alias("unique_customers_reached"),
        spark_sum("total_amount").alias("attributed_revenue"),
        spark_sum(col("total_amount") - col("discount_amount") - col("tax_amount")).alias("attributed_profit"),
        spark_sum(when(col("is_first_purchase"), col("total_amount")).otherwise(0)).alias("new_customer_revenue"),
        spark_sum(when(~col("is_first_purchase"), col("total_amount")).otherwise(0)).alias("existing_customer_revenue"),
        count(when(col("is_first_purchase"), 1)).alias("new_customer_acquisitions"),
        spark_min("transaction_date").alias("start_date"),
        spark_max("transaction_date").alias("end_date"),
        avg("total_amount").alias("avg_transaction_value")
    )

    # Calculate campaign duration
    campaign_metrics = campaign_metrics.withColumn(
        "campaign_duration_days",
        datediff(col("end_date"), col("start_date")) + 1
    )

    # Generate campaign metadata (simulated)
    print("\n[4/6] Generating campaign metadata and marketing metrics...")

    campaign_metrics = campaign_metrics \
        .withColumn("campaign_name",
                    when(col("campaign_id") == 0, "Organic Traffic")
                    .otherwise(expr("CONCAT('Campaign_', campaign_id, '_', coupon_code)"))) \
        .withColumn("campaign_type",
                    when(col("campaign_id") == 0, "organic")
                    .when(col("channel") == "online", "digital")
                    .when(col("channel") == "mobile", "mobile_app")
                    .otherwise("offline"))

    # Simulate marketing metrics based on conversion patterns
    # In production, these would come from marketing platforms (Google Ads, Facebook, etc.)
    campaign_metrics = campaign_metrics \
        .withColumn("total_conversions", col("attributed_transactions")) \
        .withColumn("total_clicks", (col("attributed_transactions") * (20 + rand() * 30)).cast("BIGINT")) \
        .withColumn("total_impressions", (col("total_clicks") * (50 + rand() * 100)).cast("BIGINT"))

    # Calculate engagement rates
    campaign_metrics = campaign_metrics \
        .withColumn("click_through_rate",
                    (col("total_clicks") / col("total_impressions") * 100).cast("DECIMAL(5,2)")) \
        .withColumn("conversion_rate",
                    (col("total_conversions") / col("total_clicks") * 100).cast("DECIMAL(5,2)"))

    # Simulate budget allocation
    # Budget is estimated as 15-25% of attributed revenue for paid campaigns
    campaign_metrics = campaign_metrics \
        .withColumn("total_budget",
                    when(col("campaign_id") == 0, lit(0))
                    .otherwise((col("attributed_revenue") * (0.15 + rand() * 0.10)).cast("DECIMAL(12,2)"))) \
        .withColumn("actual_spend",
                    when(col("campaign_id") == 0, lit(0))
                    .otherwise((col("total_budget") * (0.85 + rand() * 0.15)).cast("DECIMAL(12,2)")))

    # Calculate profitability metrics
    print("\n[5/6] Computing ROI and profitability metrics...")

    final_df = campaign_metrics \
        .withColumn("budget_utilization_rate",
                    when(col("total_budget") > 0,
                         (col("actual_spend") / col("total_budget") * 100)).otherwise(0)
                    .cast("DECIMAL(5,2)")) \
        .withColumn("cost_per_impression",
                    when(col("total_impressions") > 0,
                         col("actual_spend") / col("total_impressions")).otherwise(0)
                    .cast("DECIMAL(10,4)")) \
        .withColumn("cost_per_click",
                    when(col("total_clicks") > 0,
                         col("actual_spend") / col("total_clicks")).otherwise(0)
                    .cast("DECIMAL(10,2)")) \
        .withColumn("cost_per_acquisition",
                    when(col("new_customer_acquisitions") > 0,
                         col("actual_spend") / col("new_customer_acquisitions")).otherwise(0)
                    .cast("DECIMAL(10,2)")) \
        .withColumn("roi_percentage",
                    when(col("actual_spend") > 0,
                         ((col("attributed_profit") - col("actual_spend")) / col("actual_spend") * 100))
                    .otherwise(lit(None))
                    .cast("DECIMAL(8,2)")) \
        .withColumn("roas",
                    when(col("actual_spend") > 0,
                         col("attributed_revenue") / col("actual_spend")).otherwise(lit(None))
                    .cast("DECIMAL(8,2)")) \
        .withColumn("revenue_per_impression",
                    when(col("total_impressions") > 0,
                         col("attributed_revenue") / col("total_impressions")).otherwise(0)
                    .cast("DECIMAL(10,4)")) \
        .withColumn("profit_per_dollar_spent",
                    when(col("actual_spend") > 0,
                         col("attributed_profit") / col("actual_spend")).otherwise(lit(None))
                    .cast("DECIMAL(10,2)"))

    # Calculate incremental revenue (vs organic baseline)
    organic_avg_revenue = final_df.filter(col("campaign_id") == 0) \
        .select(avg("attributed_revenue")).first()

    organic_baseline = float(organic_avg_revenue[0]) if organic_avg_revenue and organic_avg_revenue[0] else 0.0

    final_df = final_df \
        .withColumn("incremental_revenue",
                    (col("attributed_revenue") - lit(organic_baseline)).cast("DECIMAL(12,2)")) \
        .withColumn("incremental_profit",
                    (col("attributed_profit") - lit(organic_baseline * 0.3)).cast("DECIMAL(12,2)"))

    # Calculate CLV for acquired customers (simplified: avg transaction value * 5)
    final_df = final_df \
        .withColumn("avg_clv_of_acquired_customers",
                    (col("avg_transaction_value") * 5).cast("DECIMAL(10,2)")) \
        .withColumn("estimated_lifetime_roi",
                    when(col("actual_spend") > 0,
                         ((col("avg_clv_of_acquired_customers") * col("new_customer_acquisitions")) / col(
                             "actual_spend") * 100))
                    .otherwise(lit(None))
                    .cast("DECIMAL(8,2)"))

    # Calculate channel-level comparison metrics
    channel_window = Window.partitionBy("channel").orderBy(col("attributed_revenue").desc())

    # Calculate channel average ROI
    channel_avg_roi = final_df.groupBy("channel").agg(
        avg("roi_percentage").alias("channel_avg_roi")
    )

    final_df = final_df.join(channel_avg_roi, on="channel", how="left") \
        .withColumn("roi_vs_channel_avg",
                    (col("roi_percentage") - col("channel_avg_roi")).cast("DECIMAL(8,2)")) \
        .withColumn("performance_rank_in_channel",
                    dense_rank().over(channel_window)) \
        .drop("channel_avg_roi")

    # Add analysis date and timestamp
    final_df = final_df \
        .withColumn("analysis_date", current_date()) \
        .withColumn("created_timestamp", current_timestamp())

    # Select final columns matching DDL
    final_df = final_df.select(
        "analysis_date",
        "campaign_id",
        "campaign_name",
        "campaign_type",
        "channel",
        "start_date",
        "end_date",
        "campaign_duration_days",
        "total_budget",
        "actual_spend",
        "budget_utilization_rate",
        "total_impressions",
        "total_clicks",
        "total_conversions",
        "unique_customers_reached",
        "click_through_rate",
        "conversion_rate",
        "attributed_revenue",
        "attributed_transactions",
        "new_customer_revenue",
        "existing_customer_revenue",
        "attributed_profit",
        "cost_per_impression",
        "cost_per_click",
        "cost_per_acquisition",
        "roi_percentage",
        "roas",
        "incremental_revenue",
        "incremental_profit",
        "avg_clv_of_acquired_customers",
        "estimated_lifetime_roi",
        "revenue_per_impression",
        "profit_per_dollar_spent",
        "roi_vs_channel_avg",
        "performance_rank_in_channel",
        "created_timestamp"
    )

    # Write to gold layer
    print("\n[6/6] Writing to gold.campaign_roi_analysis...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("analysis_date") \
        .format("parquet") \
        .saveAsTable("gold.campaign_roi_analysis")

    final_count = final_df.count()
    print(f"  ✓ Successfully wrote {final_count:,} campaign records")

    # Show top performing campaigns
    print("\n" + "=" * 80)
    print("  Top Performing Campaigns (by ROI)")
    print("=" * 80)
    final_df.filter(col("campaign_id") != 0) \
        .orderBy(col("roi_percentage").desc_nulls_last()) \
        .select(
        "campaign_name", "channel", "attributed_revenue",
        "actual_spend", "roi_percentage", "roas"
    ) \
        .show(10, truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Campaign ROI Summary")
    print("=" * 80)

    summary_stats = final_df.filter(col("campaign_id") != 0).select(
        count("*").alias("total_campaigns"),
        spark_sum("attributed_revenue").alias("total_revenue"),
        spark_sum("actual_spend").alias("total_spend"),
        avg("roi_percentage").alias("avg_roi"),
        avg("roas").alias("avg_roas")
    ).first()

    print(f"Total campaigns: {summary_stats['total_campaigns']:,}")
    print(f"Total attributed revenue: ${summary_stats['total_revenue']:,.2f}")
    print(f"Total marketing spend: ${summary_stats['total_spend']:,.2f}")
    print(f"Average ROI: {summary_stats['avg_roi']:.2f}%")
    print(f"Average ROAS: {summary_stats['avg_roas']:.2f}x")

    print("\n" + "=" * 80)
    print("  SUCCESS! Campaign ROI analysis complete")
    print("=" * 80)
    print("\nQuery examples:")
    print(
        "  spark.sql('SELECT * FROM gold.campaign_roi_analysis WHERE roi_percentage > 100 ORDER BY roi_percentage DESC').show()")
    print(
        "  spark.sql('SELECT channel, AVG(roas) as avg_roas FROM gold.campaign_roi_analysis GROUP BY channel ORDER BY avg_roas DESC').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
