"""
Bronze to Silver - Transform Product Catalog

This script reads raw product catalog data from bronze.product_catalog_raw,
applies data quality transformations, type casting, and derives additional
product metrics to create the silver.product_catalog table with SCD Type 2 support.

Transformations applied:
- Type casting (string -> proper types: BIGINT, DATE, DECIMAL, BOOLEAN, ARRAY, etc.)
- Extract numeric IDs from string prefixes
- Parse JSON tags array
- Build full category path
- Calculate margin percentage
- Determine price tier
- Calculate product age and lifecycle stage
- Add SCD Type 2 fields
- Add metadata timestamps

Input: bronze.product_catalog_raw
Output: silver.product_catalog (Parquet, partitioned by category_level1, is_current)

Usage:
    ./submit.sh bronze_to_silver_product_catalog.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, current_timestamp, lit, expr,
    coalesce, datediff, concat_ws, from_json
)
from pyspark.sql.types import ArrayType, StringType

print("=" * 80)
print("  Bronze to Silver - Product Catalog Transformation")
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
    print(f"\nStarting transformation at: {datetime.now()}")

    # Read from bronze
    print("\n[1/6] Reading from bronze.product_catalog_raw...")
    bronze_df = spark.table("bronze.product_catalog_raw")

    initial_count = bronze_df.count()
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Data transformations
    print("\n[2/6] Applying transformations...")

    # Extract numeric IDs from string prefixes
    silver_df = bronze_df \
        .withColumn("product_id", expr("CAST(regexp_extract(product_id, '[0-9]+', 0) AS BIGINT)")) \
        .withColumn("supplier_id", expr("CAST(regexp_extract(supplier_id, '[0-9]+', 0) AS BIGINT)"))

    print("  ✓ ID extraction and casting complete")

    # Build full category path
    silver_df = silver_df \
        .withColumn("full_category_path",
                    concat_ws(" > ",
                              coalesce(col("category_level1"), lit("")),
                              coalesce(col("category_level2"), lit("")),
                              coalesce(col("category_level3"), lit(""))))

    print("  ✓ Built category hierarchy path")

    # Cast numeric fields
    silver_df = silver_df \
        .withColumn("unit_cost", col("unit_cost").cast("DECIMAL(10,2)")) \
        .withColumn("list_price", col("list_price").cast("DECIMAL(10,2)")) \
        .withColumn("lead_time_days", coalesce(col("lead_time_days").cast("INT"), lit(0))) \
        .withColumn("weight_kg", coalesce(col("weight_kg").cast("DECIMAL(8,3)"), lit(0.0)))

    print("  ✓ Numeric field casting complete")

    # Calculate margin percentage: ((list_price - unit_cost) / list_price) * 100
    silver_df = silver_df \
        .withColumn("margin_percentage",
                    when(col("list_price") > 0,
                         ((col("list_price") - col("unit_cost")) / col("list_price") * 100))
                    .otherwise(lit(0.0))
                    .cast("DECIMAL(5,2)"))

    print("  ✓ Calculated margin percentage")

    # Determine price tier based on list_price
    silver_df = silver_df \
        .withColumn("price_tier",
                    when(col("list_price") < 50, "budget")
                    .when((col("list_price") >= 50) & (col("list_price") < 200), "mid-range")
                    .when((col("list_price") >= 200) & (col("list_price") < 500), "premium")
                    .otherwise("luxury"))

    print("  ✓ Determined price tiers")

    # Cast date fields
    silver_df = silver_df \
        .withColumn("launch_date", to_date(col("launch_date"), "yyyy-MM-dd")) \
        .withColumn("discontinuation_date",
                    when(col("discontinuation_date").isNotNull() & (col("discontinuation_date") != ""),
                         to_date(col("discontinuation_date"), "yyyy-MM-dd"))
                    .otherwise(lit(None)))

    print("  ✓ Date casting complete")

    print("\n[3/6] Product lifecycle calculations...")

    # Calculate product_age_days
    silver_df = silver_df \
        .withColumn("product_age_days",
                    coalesce(
                        datediff(current_timestamp().cast("date"), col("launch_date")),
                        lit(0)
                    ))

    print("  ✓ Calculated product age")

    # Determine is_active (not discontinued)
    silver_df = silver_df \
        .withColumn("is_active",
                    when(col("discontinuation_date").isNull(), True)
                    .otherwise(False))

    print("  ✓ Determined active status")

    # Determine lifecycle_stage based on product_age_days and is_active
    silver_df = silver_df \
        .withColumn("lifecycle_stage",
                    when(~col("is_active"), "discontinued")
                    .when(col("product_age_days") < 90, "introduction")  # < 3 months
                    .when(col("product_age_days") < 365, "growth")  # < 1 year
                    .when(col("product_age_days") < 1095, "maturity")  # < 3 years
                    .otherwise("decline"))

    print("  ✓ Determined lifecycle stage")

    print("\n[4/6] Processing tags...")

    # Parse JSON tags array if it exists
    # Assuming tags is stored as JSON array string like '["tag1", "tag2"]' or empty/null
    silver_df = silver_df \
        .withColumn("tags",
                    when((col("tags").isNotNull()) & (col("tags") != "") & (col("tags") != "[]"),
                         from_json(col("tags"), ArrayType(StringType())))
                    .otherwise(lit(None).cast(ArrayType(StringType()))))

    print("  ✓ Parsed tags array")

    print("\n[5/6] Adding SCD Type 2 fields...")

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
        "product_id",
        "product_name",
        "category_level1",
        "category_level2",
        "category_level3",
        "full_category_path",
        "brand",
        "manufacturer",
        "unit_cost",
        "list_price",
        "margin_percentage",
        "price_tier",
        "supplier_id",
        "lead_time_days",
        "weight_kg",
        "launch_date",
        "discontinuation_date",
        "is_active",
        "product_age_days",
        "lifecycle_stage",
        "tags",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "source_system",
        "created_timestamp",
        "updated_timestamp"
    )

    print("\n[6/6] Data quality checks...")
    final_count = final_df.count()
    null_product_ids = final_df.filter(col("product_id").isNull()).count()
    active_products = final_df.filter(col("is_active") == True).count()
    discontinued_products = final_df.filter(col("is_active") == False).count()

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null product_id: {null_product_ids}")
    print(f"  ✓ Active products: {active_products}")
    print(f"  ✓ Discontinued products: {discontinued_products}")

    if null_product_ids > 0:
        print("  ⚠️  Warning: Found null values in product_id")

    # Calculate data quality percentage
    valid_records = final_count - null_product_ids
    quality_pct = (valid_records / final_count * 100) if final_count > 0 else 0

    # Write to silver layer
    print("\n[7/7] Writing to silver.product_catalog...")
    print("  Partitioning by: category_level1, is_current")

    cols = [col.col_name for col in
            spark.sql(f"SHOW COLUMNS IN silver.product_catalog").select('col_name').collect()]

    final_df = final_df.select([final_df[col] for col in cols])
    final_df.write.insertInto("silver.product_catalog", overwrite=True)

    print(f"  ✓ Successfully wrote {final_count:,} records to silver.product_catalog")

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.product_catalog")
    print("=" * 80)
    spark.table("silver.product_catalog").select(
        "product_id", "product_name", "category_level1", "brand",
        "list_price", "margin_percentage", "price_tier",
        "is_active", "lifecycle_stage"
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
    metrics_df = spark.table("silver.product_catalog").selectExpr(
        "COUNT(*) as total_products",
        "SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_products",
        "AVG(list_price) as avg_list_price",
        "AVG(margin_percentage) as avg_margin_pct",
        "AVG(product_age_days) as avg_age_days"
    )
    metrics_df.show(truncate=False)

    # Lifecycle distribution
    print("\nProduct lifecycle distribution:")
    spark.table("silver.product_catalog") \
        .groupBy("lifecycle_stage") \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    # Category distribution
    print("\nTop categories:")
    spark.table("silver.product_catalog") \
        .groupBy("category_level1") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(10)

    # Price tier distribution
    print("\nPrice tier distribution:")
    spark.table("silver.product_catalog") \
        .groupBy("price_tier") \
        .count() \
        .orderBy(col("count").desc()) \
        .show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Product catalog transformed to silver layer")
    print("=" * 80)
    print("\nQuery examples:")
    print("  spark.sql('SELECT * FROM silver.product_catalog WHERE is_active = true LIMIT 10').show()")
    print(
        "  spark.sql('SELECT * FROM silver.product_catalog WHERE lifecycle_stage = \"introduction\" LIMIT 10').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
