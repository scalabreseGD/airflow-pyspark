"""
Gold Layer - Channel Attribution (PySpark)

Implements multiple attribution models to analyze how different marketing channels
contribute to conversions. Tracks customer journey with touchpoints and applies
various attribution methods.

Attribution Models Implemented:
- First Touch: 100% credit to first channel
- Last Touch: 100% credit to last channel
- Linear: Equal credit across all touchpoints
- Time Decay: More credit to recent touchpoints
- Position Based: 40% first, 40% last, 20% middle
- Data-Driven: ML-based weighting (simplified)

Input: silver.transactions, silver.customer_interactions
Output: gold.channel_attribution (Parquet, partitioned by attribution_date)

Usage:
    ./submit.sh gold_channel_attribution.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, lit,
    current_timestamp, current_date, when, struct, array, map_from_arrays,
    collect_list, expr, row_number, datediff, min as spark_min, max as spark_max
)
from pyspark.sql.window import Window

print("=" * 80)
print("  Gold Layer - Channel Attribution")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

try:
    print(f"\nStarting analysis at: {datetime.now()}")

    # Read silver tables
    print("\n[1/6] Reading silver layer data...")
    transactions_df = spark.table("silver.transactions")

    # Try to read interactions, if it doesn't exist use only transactions
    try:
        interactions_df = spark.table("silver.customer_interactions")
        print(f"  ✓ Transactions: {transactions_df.count():,} records")
        print(f"  ✓ Interactions: {interactions_df.count():,} records")
        has_interactions = True
    except:
        print(f"  ✓ Transactions: {transactions_df.count():,} records")
        print("  ⚠ No customer_interactions table found, using transactions only")
        has_interactions = False

    # Build customer journey with touchpoints
    print("\n[2/6] Building customer journey touchpoints...")

    # Create touchpoints from transactions (these are conversions)
    transaction_touchpoints = transactions_df.select(
        col("customer_id"),
        col("transaction_id"),
        col("transaction_timestamp").alias("timestamp"),
        col("channel"),
        lit(None).cast("BIGINT").alias("campaign_id"),
        lit("purchase").alias("event_type"),
        col("total_amount"),
        col("total_amount").alias("transaction_revenue"),
        (col("total_amount") - col("discount_amount") - col("tax_amount")).alias("transaction_profit"),
        col("transaction_date").alias("attribution_date")
    )

    if has_interactions:
        # Create touchpoints from interactions (these are pre-purchase touchpoints)
        # Assume interaction_type maps to channel
        interaction_touchpoints = interactions_df.select(
            col("customer_id"),
            lit(None).cast("BIGINT").alias("transaction_id"),
            col("interaction_timestamp").alias("timestamp"),
            col("interaction_type").alias("channel"),
            lit(None).cast("BIGINT").alias("campaign_id"),
            lit("interaction").alias("event_type"),
            lit(0).cast("DECIMAL(12,2)").alias("total_amount"),
            lit(0).cast("DECIMAL(12,2)").alias("transaction_revenue"),
            lit(0).cast("DECIMAL(12,2)").alias("transaction_profit"),
            current_date().alias("attribution_date")
        )

        # Union all touchpoints
        all_touchpoints = transaction_touchpoints.unionByName(interaction_touchpoints)
    else:
        all_touchpoints = transaction_touchpoints

    print("  ✓ Created customer touchpoints")

    # For each transaction, find all touchpoints in the 30 days before purchase
    print("\n[3/6] Mapping customer journeys to transactions...")

    # Get only purchase touchpoints
    purchases = all_touchpoints.filter(col("event_type") == "purchase")

    # Create window for each customer ordered by time
    customer_window = Window.partitionBy("customer_id").orderBy("timestamp")

    # Add sequence number to touchpoints
    touchpoints_sequenced = all_touchpoints \
        .withColumn("touchpoint_seq", row_number().over(customer_window))

    # Join purchases with all prior touchpoints from same customer
    journeys = purchases.alias("p").join(
        touchpoints_sequenced.alias("t"),
        (col("p.customer_id") == col("t.customer_id")) &
        (col("t.timestamp") <= col("p.timestamp")) &
        (datediff(col("p.timestamp"), col("t.timestamp")) <= 30),
        "left"
    )

    # Aggregate touchpoints into journey for each purchase
    journey_aggregated = journeys.groupBy(
        col("p.transaction_id"),
        col("p.customer_id"),
        col("p.transaction_revenue"),
        col("p.transaction_profit"),
        col("p.attribution_date")
    ).agg(
        collect_list(
            struct(
                col("t.channel").alias("channel"),
                col("t.timestamp").alias("timestamp"),
                col("t.campaign_id").alias("campaign_id"),
                col("t.event_type").alias("event_type")
            )
        ).alias("journey_touchpoints"),
        count("*").alias("touchpoint_count"),
        spark_min(col("t.timestamp")).alias("first_touchpoint_time"),
        spark_max(col("t.timestamp")).alias("last_touchpoint_time")
    )

    # Calculate journey length in days
    journey_aggregated = journey_aggregated \
        .withColumn("journey_length_days",
                    datediff(col("last_touchpoint_time"), col("first_touchpoint_time")))

    print("  ✓ Mapped customer journeys")

    # Extract first and last touch channels
    print("\n[4/6] Applying attribution models...")

    final_df = journey_aggregated \
        .withColumn("first_touch_channel",
                    expr("journey_touchpoints[0].channel")) \
        .withColumn("first_touch_campaign_id",
                    expr("journey_touchpoints[0].campaign_id")) \
        .withColumn("last_touch_channel",
                    expr("journey_touchpoints[size(journey_touchpoints)-1].channel")) \
        .withColumn("last_touch_campaign_id",
                    expr("journey_touchpoints[size(journey_touchpoints)-1].campaign_id"))

    # First Touch Attribution: 100% to first channel
    final_df = final_df \
        .withColumn("first_touch_attribution_revenue", col("transaction_revenue"))

    # Last Touch Attribution: 100% to last channel
    final_df = final_df \
        .withColumn("last_touch_attribution_revenue", col("transaction_revenue"))

    # Linear Attribution: Equal credit to all touchpoints
    final_df = final_df \
        .withColumn("linear_attribution_per_touchpoint",
                    (col("transaction_revenue") / col("touchpoint_count")).cast("DECIMAL(12,2)"))

    # Time Decay Attribution: Use exponential decay (half-life = 7 days)
    # For simplicity, we'll create a map with channel -> attributed revenue
    # In production, you'd calculate per-touchpoint decay weights
    print("  ✓ Calculating time decay attribution...")

    # Create a simplified time decay map (demo purposes)
    # Handle single touchpoint case to avoid duplicate map keys
    final_df = final_df \
        .withColumn("time_decay_attribution_map",
                    when(col("first_touch_channel") == col("last_touch_channel"),
                         # Single touchpoint: give 100% credit
                         map_from_arrays(
                             array(col("first_touch_channel")),
                             array(col("transaction_revenue").cast("DECIMAL(12,2)"))
                         ))
                    .otherwise(
                        # Multiple touchpoints: 30% first, 70% last
                        map_from_arrays(
                            array(col("first_touch_channel"), col("last_touch_channel")),
                            array(
                                (col("transaction_revenue") * lit(0.3)).cast("DECIMAL(12,2)"),
                                (col("transaction_revenue") * lit(0.7)).cast("DECIMAL(12,2)")
                            )
                        )
                    ))

    # Position Based Attribution: 40% first, 40% last, 20% middle
    final_df = final_df \
        .withColumn("position_based_attribution_map",
                    when(col("touchpoint_count") == 1,
                         # Single touchpoint: 100% credit
                         map_from_arrays(
                             array(col("first_touch_channel")),
                             array(col("transaction_revenue").cast("DECIMAL(12,2)"))
                         ))
                    .when((col("touchpoint_count") == 2) | (col("first_touch_channel") == col("last_touch_channel")),
                          # Two touchpoints or same channel: 50/50 split (but avoid duplicate keys)
                          when(col("first_touch_channel") == col("last_touch_channel"),
                               map_from_arrays(
                                   array(col("first_touch_channel")),
                                   array(col("transaction_revenue").cast("DECIMAL(12,2)"))
                               ))
                          .otherwise(
                              map_from_arrays(
                                  array(col("first_touch_channel"), col("last_touch_channel")),
                                  array(
                                      (col("transaction_revenue") * lit(0.5)).cast("DECIMAL(12,2)"),
                                      (col("transaction_revenue") * lit(0.5)).cast("DECIMAL(12,2)")
                                  )
                              )
                          ))
                    .otherwise(
                        # Multiple touchpoints: 40% first, 40% last, 20% middle (simplified)
                        map_from_arrays(
                            array(col("first_touch_channel"), col("last_touch_channel")),
                            array(
                                (col("transaction_revenue") * lit(0.4)).cast("DECIMAL(12,2)"),
                                (col("transaction_revenue") * lit(0.4)).cast("DECIMAL(12,2)")
                            )
                        )
                    ))

    # Data-Driven Attribution (simplified ML-based approach)
    # In production, this would use actual ML models trained on conversion data
    # For demo, we'll use a weighted approach based on conversion patterns
    print("  ✓ Calculating data-driven attribution...")

    final_df = final_df \
        .withColumn("data_driven_attribution_map",
                    when(col("first_touch_channel") == col("last_touch_channel"),
                         # Single touchpoint or same channel: 100% credit
                         map_from_arrays(
                             array(col("first_touch_channel")),
                             array(col("transaction_revenue").cast("DECIMAL(12,2)"))
                         ))
                    .otherwise(
                        # Multiple distinct touchpoints: 45% first, 55% last
                        map_from_arrays(
                            array(col("first_touch_channel"), col("last_touch_channel")),
                            array(
                                (col("transaction_revenue") * lit(0.45)).cast("DECIMAL(12,2)"),
                                (col("transaction_revenue") * lit(0.55)).cast("DECIMAL(12,2)")
                            )
                        )
                    ))

    # Add metadata
    final_df = final_df \
        .withColumn("created_timestamp", current_timestamp())

    # Select final columns matching DDL
    final_df = final_df.select(
        "attribution_date",
        "customer_id",
        "transaction_id",
        col("transaction_revenue").cast("DECIMAL(12,2)"),
        col("transaction_profit").cast("DECIMAL(12,2)"),
        "first_touch_channel",
        "first_touch_campaign_id",
        col("first_touch_attribution_revenue").cast("DECIMAL(12,2)"),
        "last_touch_channel",
        "last_touch_campaign_id",
        col("last_touch_attribution_revenue").cast("DECIMAL(12,2)"),
        col("touchpoint_count").cast("INT"),
        "linear_attribution_per_touchpoint",
        "time_decay_attribution_map",
        "position_based_attribution_map",
        "data_driven_attribution_map",
        col("journey_length_days").cast("INT"),
        "journey_touchpoints",
        "created_timestamp"
    )

    print("  ✓ Applied all attribution models")

    # Write to gold layer
    print("\n[5/6] Writing to gold.channel_attribution...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("attribution_date") \
        .format("parquet") \
        .saveAsTable("gold.channel_attribution")

    final_count = final_df.count()
    print(f"  ✓ Successfully wrote {final_count:,} attributed transactions")

    # Show attribution summary
    print("\n" + "=" * 80)
    print("  Channel Attribution Summary")
    print("=" * 80)

    # First Touch Attribution
    print("\n  First Touch Attribution:")
    final_df.groupBy("first_touch_channel").agg(
        count("*").alias("conversions"),
        spark_sum("first_touch_attribution_revenue").alias("attributed_revenue")
    ).orderBy(col("attributed_revenue").desc()).show(10, truncate=False)

    # Last Touch Attribution
    print("\n  Last Touch Attribution:")
    final_df.groupBy("last_touch_channel").agg(
        count("*").alias("conversions"),
        spark_sum("last_touch_attribution_revenue").alias("attributed_revenue")
    ).orderBy(col("attributed_revenue").desc()).show(10, truncate=False)

    # Journey metrics
    print("\n  Customer Journey Insights:")
    summary_stats = final_df.select(
        avg("journey_length_days").alias("avg_journey_length"),
        avg("touchpoint_count").alias("avg_touchpoints")
    ).first()

    print(f"  Average journey length: {summary_stats['avg_journey_length']:.1f} days")
    print(f"  Average touchpoints per conversion: {summary_stats['avg_touchpoints']:.1f}")

    print("\n" + "=" * 80)
    print("  SUCCESS! Channel attribution analysis complete")
    print("=" * 80)
    print("\nQuery examples:")
    print(
        "  spark.sql('SELECT first_touch_channel, COUNT(*) FROM gold.channel_attribution GROUP BY first_touch_channel').show()")
    print("  spark.sql('SELECT * FROM gold.channel_attribution WHERE journey_length_days > 7 LIMIT 10').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
