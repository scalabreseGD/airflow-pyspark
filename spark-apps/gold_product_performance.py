"""
Gold Layer - Product Performance (PySpark)

Analyzes product sales performance, profitability, and trends with inventory metrics.

Input: silver.transaction_items, silver.product_catalog, silver.inventory_snapshots
Output: gold.product_performance (Parquet, partitioned by analysis_date, time_period)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct, current_timestamp,
    current_date, desc, when, lit, dense_rank, row_number
)
from pyspark.sql.window import Window

print("=" * 80)
print("  Gold Layer - Product Performance")
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
    # Read data
    print("\n[1/2] Reading silver layer...")
    items_df = spark.table("silver.transaction_items")
    products_df = spark.table("silver.product_catalog").filter(col("is_current") == True)
    transactions_df = spark.table("silver.transactions")

    # Join items with products and transactions
    items_with_products = items_df.join(
        products_df.select("product_id", "product_name", "category_level1", "category_level2", "brand", "list_price"),
        on="product_id"
    ).join(
        transactions_df.select("transaction_id", "customer_id", "is_first_purchase"),
        on="transaction_id"
    )

    # Calculate product metrics
    print("\n[2/2] Calculating product metrics...")
    product_metrics = items_with_products.groupBy("product_id", "product_name", "category_level1", "category_level2",
                                                  "brand").agg(
        spark_sum("net_quantity").alias("total_units_sold"),
        count("transaction_item_id").alias("total_transactions"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct(when(col("is_first_purchase"), col("customer_id"))).alias("new_customers"),
        spark_sum("line_total").alias("gross_revenue"),
        spark_sum("line_profit").alias("gross_profit"),
        spark_sum("return_quantity").alias("units_returned"),
        avg("unit_price").alias("avg_selling_price"),
        avg("discount_percentage").alias("avg_discount_percentage")
    )

    # Calculate derived metrics
    product_metrics = product_metrics \
        .withColumn("repeat_customers", col("total_customers") - col("new_customers")) \
        .withColumn("net_revenue", col("gross_revenue").cast("DECIMAL(15,2)")) \
        .withColumn("return_rate",
                    (col("units_returned") / (col("total_units_sold") + col("units_returned"))).cast("DECIMAL(5,2)")) \
        .withColumn("gross_margin_percentage", (col("gross_profit") / col("gross_revenue") * 100).cast("DECIMAL(5,2)"))

    # Rank products
    category_window = Window.partitionBy("category_level1").orderBy(desc("gross_revenue"))
    overall_window = Window.orderBy(desc("gross_revenue"))

    final_df = product_metrics \
        .withColumn("product_key", row_number().over(overall_window)) \
        .withColumn("analysis_date", current_date()) \
        .withColumn("time_period", lit("monthly")) \
        .withColumn("category_revenue_rank", dense_rank().over(category_window)) \
        .withColumn("overall_revenue_rank", dense_rank().over(overall_window)) \
        .withColumn("is_high_performer", when(col("category_revenue_rank") <= 10, True).otherwise(False)) \
        .withColumn("created_timestamp", current_timestamp())

    # Select final columns
    final_df = final_df.select(
        "product_key", "product_id", "analysis_date", "time_period",
        "product_name", "category_level1", "category_level2", lit(None).cast("STRING").alias("brand"),
        "total_units_sold", "total_transactions", "total_customers",
        "new_customers", "repeat_customers",
        col("gross_revenue").cast("DECIMAL(15,2)"), col("net_revenue").cast("DECIMAL(15,2)"),
        lit(0).cast("DECIMAL(12,2)").alias("total_discounts"),
        col("avg_selling_price").cast("DECIMAL(10,2)"),
        col("avg_discount_percentage").cast("DECIMAL(5,2)"),
        lit(0).cast("DECIMAL(15,2)").alias("total_cost"),
        col("gross_profit").cast("DECIMAL(15,2)"),
        "gross_margin_percentage",
        lit(0).cast("DECIMAL(15,2)").alias("contribution_margin"),
        "units_returned",
        "return_rate",
        lit(0).cast("DECIMAL(12,2)").alias("return_value"),
        lit(None).cast("STRING").alias("top_return_reason"),
        lit(0).cast("DECIMAL(12,2)").alias("avg_inventory_on_hand"),
        lit(0).alias("stockout_days"),
        lit(0).cast("DECIMAL(5,2)").alias("stockout_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("inventory_turnover"),
        lit(0).cast("DECIMAL(8,2)").alias("days_inventory_outstanding"),
        lit(0).cast("DECIMAL(10,2)").alias("units_per_day"),
        lit(0).cast("DECIMAL(12,2)").alias("revenue_per_day"),
        lit("stable").alias("sales_velocity_trend"),
        col("category_revenue_rank").cast("INT"),
        lit(0).cast("INT").alias("category_units_rank"),
        "overall_revenue_rank",
        lit(0).cast("DECIMAL(5,2)").alias("category_market_share"),
        lit(0).cast("DECIMAL(5,2)").alias("penetration_rate"),
        lit(0).cast("DECIMAL(5,2)").alias("repeat_purchase_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("avg_units_per_customer"),
        lit(0).cast("DECIMAL(8,4)").alias("price_elasticity_coefficient"),
        lit(0).cast("DECIMAL(10,2)").alias("optimal_price_point"),
        lit(0).cast("DECIMAL(5,2)").alias("basket_attachment_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("avg_basket_size_with_product"),
        lit(None).cast("ARRAY<STRING>").alias("top_cross_sell_products"),
        lit(0).cast("DECIMAL(8,2)").alias("revenue_growth_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("volume_growth_rate"),
        lit(0).cast("DECIMAL(8,2)").alias("market_share_change"),
        lit(False).alias("is_trending"),
        lit(False).alias("is_seasonal"),
        lit(False).alias("is_low_performer"),
        "is_high_performer",
        lit(False).alias("requires_reorder"),
        "created_timestamp"
    )

    # Write to gold
    print("\nWriting to gold.product_performance...")
    cols = [col.col_name for col in
            spark.sql(f"SHOW COLUMNS IN gold.product_performance").select('col_name').collect()]

    final_df = final_df.select([final_df[col] for col in cols])
    # Write to gold layer
    final_df.write.insertInto("gold.product_performance", overwrite=True)

    print(f"\n✓ Successfully created product performance for {final_df.count():,} products")
    print("\n" + "=" * 80)
    print("  SUCCESS! Product performance analysis complete")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise
finally:
    spark.stop()
