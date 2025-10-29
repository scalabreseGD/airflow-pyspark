"""
Gold Layer - Basket Analysis (PySpark)

Market basket analysis to identify product associations and cross-sell opportunities.
Analyzes which products are frequently purchased together in the same transaction.

Metrics calculated:
- Product pairs and their co-occurrence frequency
- Association rules: support, confidence, lift, conviction
- Revenue impact analysis (basket value with/without products)
- Recommendation scores and strong association flags

Input: silver.transactions, silver.transaction_items, silver.product_catalog
Output: gold.basket_analysis (Parquet, partitioned by analysis_date)

Usage:
    ./submit.sh gold_basket_analysis.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, lit, avg, current_timestamp, current_date, desc, when
)

print("=" * 80)
print("  Gold Layer - Basket Analysis")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Neo4j
from neo4j_lineage import enable
enable(spark)

try:
    print(f"\nStarting analysis at: {datetime.now()}")

    # Read silver tables
    print("\n[1/6] Reading silver layer data...")
    transactions_df = spark.table("silver.transactions")
    items_df = spark.table("silver.transaction_items")
    products_df = spark.table("silver.product_catalog").filter(col("is_current") == True)

    print(f"  ✓ Transactions: {transactions_df.count():,} records")
    print(f"  ✓ Transaction items: {items_df.count():,} records")
    print(f"  ✓ Products: {products_df.count():,} records")

    # Join items with product info and transaction totals
    print("\n[2/6] Building product baskets with transaction values...")
    items_with_products = items_df.join(
        products_df.select("product_id", "product_name", "category_level1"),
        on="product_id",
        how="left"
    ).join(
        transactions_df.select("transaction_id", "total_amount"),
        on="transaction_id",
        how="left"
    )

    # Calculate total transactions for support calculation
    total_transactions = transactions_df.count()
    print(f"  ✓ Total transactions: {total_transactions:,}")

    # Find product pairs (self-join on transaction_id)
    print("\n[3/6] Generating product pairs...")

    pairs_df = items_with_products.alias("a").join(
        items_with_products.alias("b"),
        (col("a.transaction_id") == col("b.transaction_id")) &
        (col("a.product_id") < col("b.product_id")),  # Avoid duplicates and self-pairs
        "inner"
    ).select(
        col("a.transaction_id"),
        col("a.product_id").alias("product_id_a"),
        col("a.product_name").alias("product_name_a"),
        col("a.category_level1").alias("category_a"),
        col("b.product_id").alias("product_id_b"),
        col("b.product_name").alias("product_name_b"),
        col("b.category_level1").alias("category_b"),
        col("a.total_amount").alias("transaction_value")
    )

    print(f"  ✓ Generated product pairs")

    # Calculate pair statistics
    print("\n[4/6] Calculating association metrics...")

    # Count transactions with both products and average basket value
    pair_stats = pairs_df.groupBy(
        "product_id_a", "product_name_a", "category_a",
        "product_id_b", "product_name_b", "category_b"
    ).agg(
        countDistinct("transaction_id").alias("transactions_with_both"),
        avg("transaction_value").alias("avg_basket_value_with_both")
    )

    # Count transactions with product A
    product_a_stats = items_with_products.groupBy("product_id").agg(
        countDistinct("transaction_id").alias("transactions_with_a"),
        avg("total_amount").alias("avg_basket_value_with_a_only")
    ).withColumnRenamed("product_id", "prod_a_id")

    # Count transactions with product B
    product_b_stats = items_with_products.groupBy("product_id").agg(
        countDistinct("transaction_id").alias("transactions_with_b")
    ).withColumnRenamed("product_id", "prod_b_id")

    # Join all statistics
    associations_df = pair_stats \
        .join(product_a_stats, col("product_id_a") == col("prod_a_id"), "left") \
        .drop("prod_a_id") \
        .join(product_b_stats, col("product_id_b") == col("prod_b_id"), "left") \
        .drop("prod_b_id")

    # Calculate association rule metrics
    print("\n[5/6] Computing association rules and revenue impact...")

    final_df = associations_df \
        .withColumn("total_transactions", lit(total_transactions)) \
        .withColumn("support",
                    (col("transactions_with_both") / col("total_transactions")).cast("DECIMAL(8,6)")) \
        .withColumn("confidence_a_to_b",
                    (col("transactions_with_both") / col("transactions_with_a")).cast("DECIMAL(8,6)")) \
        .withColumn("confidence_b_to_a",
                    (col("transactions_with_both") / col("transactions_with_b")).cast("DECIMAL(8,6)")) \
        .withColumn("lift",
                    ((col("transactions_with_both") * col("total_transactions")) /
                     (col("transactions_with_a") * col("transactions_with_b"))).cast("DECIMAL(10,4)")) \
        .withColumn("conviction",
                    when(col("confidence_a_to_b") < 1.0,
                         (((1 - (col("transactions_with_b") / col("total_transactions"))) /
                           (1 - col("confidence_a_to_b")))).cast("DECIMAL(10,4)"))
                    .otherwise(lit(None).cast("DECIMAL(10,4)"))) \
        .withColumn("incremental_basket_value",
                    (col("avg_basket_value_with_both") - col("avg_basket_value_with_a_only")).cast("DECIMAL(10,2)")) \
        .withColumn("recommendation_score",
                    ((col("lift") * 20) +
                     (col("confidence_a_to_b") * 50) +
                     (col("support") * 1000) +
                     when(col("incremental_basket_value") > 0, col("incremental_basket_value") / 10).otherwise(0)
                     ).cast("DECIMAL(5,2)")) \
        .withColumn("is_strong_association",
                    (col("lift") > 1.5) &
                    (col("confidence_a_to_b") > 0.3) &
                    (col("transactions_with_both") >= 3)) \
        .withColumn("analysis_date", current_date()) \
        .withColumn("created_timestamp", current_timestamp())

    # Select final columns matching DDL
    final_df = final_df.select(
        "analysis_date",
        "product_id_a",
        "product_id_b",
        "product_name_a",
        "product_name_b",
        "category_a",
        "category_b",
        "transactions_with_a",
        "transactions_with_b",
        "transactions_with_both",
        "total_transactions",
        "support",
        "confidence_a_to_b",
        "confidence_b_to_a",
        "lift",
        "conviction",
        "avg_basket_value_with_both",
        "avg_basket_value_with_a_only",
        "incremental_basket_value",
        "recommendation_score",
        "is_strong_association",
        "created_timestamp"
    )

    print("  ✓ Calculated all association metrics and revenue impact")

    cols = [col.col_name for col in
            spark.sql(f"SHOW COLUMNS IN gold.basket_analysis").select('col_name').collect()]

    final_df = final_df.select([final_df[col] for col in cols])
    # Write to gold layer
    print("\n[6/6] Writing to gold.basket_analysis...")
    final_df.write.insertInto("gold.basket_analysis", overwrite=True)

    final_count = final_df.count()
    print(f"  ✓ Successfully wrote {final_count:,} product associations")

    # Show top associations
    print("\n" + "=" * 80)
    print("  Top Product Associations (by recommendation score)")
    print("=" * 80)
    final_df.filter(col("is_strong_association") == True) \
        .orderBy(desc("recommendation_score")) \
        .select(
        "product_name_a", "product_name_b",
        "lift", "confidence_a_to_b", "recommendation_score",
        "incremental_basket_value"
    ) \
        .show(10, truncate=False)

    # Summary stats
    print("\n" + "=" * 80)
    print("  Basket Analysis Summary")
    print("=" * 80)
    print(f"Total transactions analyzed: {total_transactions:,}")
    print(f"Total product associations: {final_count:,}")

    strong_associations = final_df.filter(col("is_strong_association") == True).count()
    print(f"Strong associations: {strong_associations:,}")

    avg_lift = final_df.select(avg("lift")).first()[0]
    print(f"Average lift: {avg_lift:.2f}")

    print("\n" + "=" * 80)
    print("  SUCCESS! Basket analysis complete")
    print("=" * 80)
    print("\nQuery examples:")
    print(
        "  spark.sql('SELECT * FROM gold.basket_analysis WHERE is_strong_association = TRUE ORDER BY recommendation_score DESC LIMIT 10').show(truncate=False)")
    print(
        "  spark.sql('SELECT * FROM gold.basket_analysis WHERE lift > 2 AND transactions_with_both >= 5 ORDER BY lift DESC').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
