"""
Bronze to Silver - Transform Inventory Snapshots (Pure Spark SQL)

This script reads raw inventory snapshot data from bronze.inventory_snapshots_raw,
applies data quality transformations using pure Spark SQL, and creates the
silver.inventory_snapshots table.

Transformations applied using SQL:
- Type casting (string -> proper types: BIGINT, TIMESTAMP, DATE, INT)
- Extract numeric IDs from string prefixes
- Calculate quantity_available from quantity_on_hand - quantity_reserved
- Calculate days_of_supply based on reorder_point
- Determine stock_status (in_stock, low_stock, out_of_stock, overstock)
- Calculate is_stockout flag
- Calculate stockout_duration_days
- Add metadata timestamps

Input: bronze.inventory_snapshots_raw
Output: silver.inventory_snapshots (Parquet, partitioned by snapshot_date, warehouse_id)

Usage:
    ./submit.sh bronze_to_silver_inventory_snapshots.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Bronze to Silver - Inventory Snapshots Transformation (Spark SQL)")
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

    # Read from bronze to get initial count
    print("\n[1/4] Reading from bronze.inventory_snapshots_raw...")
    initial_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.inventory_snapshots_raw").collect()[0]['cnt']
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Define the SQL transformation
    print("\n[2/4] Executing Spark SQL transformation...")

    transformation_sql = """
    INSERT OVERWRITE TABLE silver.inventory_snapshots
    PARTITION (snapshot_date, warehouse_id)
    SELECT
        -- Extract numeric IDs from string prefixes
        CAST(regexp_extract(snapshot_id, '[0-9]+', 0) AS BIGINT) AS snapshot_id,

        -- Cast timestamp fields
        CAST(snapshot_timestamp AS TIMESTAMP) AS snapshot_timestamp,

        -- Extract product and warehouse IDs
        CAST(regexp_extract(product_id, '[0-9]+', 0) AS BIGINT) AS product_id,
        CAST(regexp_extract(variant_id, '[0-9]+', 0) AS BIGINT) AS variant_id,

        -- Cast quantity fields
        CAST(quantity_on_hand AS INT) AS quantity_on_hand,
        CAST(quantity_reserved AS INT) AS quantity_reserved,

        -- Calculate quantity_available = on_hand - reserved
        CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0) AS quantity_available,

        CAST(reorder_point AS INT) AS reorder_point,
        CAST(reorder_quantity AS INT) AS reorder_quantity,

        -- Calculate days_of_supply
        -- Assuming average daily demand is reorder_point / 30 days (simplified)
        -- days_of_supply = available / (reorder_point / 30)
        CASE
            WHEN CAST(reorder_point AS INT) > 0 THEN
                CAST(
                    (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) /
                    (CAST(reorder_point AS INT) / 30.0)
                AS INT)
            ELSE 999
        END AS days_of_supply,

        -- Determine stock_status
        CASE
            WHEN (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) <= 0
                THEN 'out_of_stock'
            WHEN (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) < CAST(reorder_point AS INT)
                THEN 'low_stock'
            WHEN (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) > (CAST(reorder_point AS INT) * 3)
                THEN 'overstock'
            ELSE 'in_stock'
        END AS stock_status,

        -- Determine is_stockout
        CASE
            WHEN (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) <= 0
                THEN TRUE
            ELSE FALSE
        END AS is_stockout,

        -- Calculate stockout_duration_days
        -- For now, set to 0 if in stock, else estimated based on days_of_supply
        CASE
            WHEN (CAST(quantity_on_hand AS INT) - COALESCE(CAST(quantity_reserved AS INT), 0)) <= 0
                THEN 1
            ELSE 0
        END AS stockout_duration_days,

        -- Metadata
        _source_system AS source_system,
        CURRENT_TIMESTAMP() AS created_timestamp,

        -- Partition columns (must be last)
        CAST(snapshot_timestamp AS DATE) AS snapshot_date,
        CAST(regexp_extract(warehouse_id, '[0-9]+', 0) AS BIGINT) AS warehouse_id

    FROM bronze.inventory_snapshots_raw
    """

    print("  ✓ Executing SQL transformation...")
    spark.sql(transformation_sql)
    print("  ✓ SQL transformation complete")

    # Data quality checks
    print("\n[3/4] Data quality checks...")
    final_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.inventory_snapshots").collect()[0]['cnt']

    quality_metrics = spark.sql("""
                                SELECT COUNT(*)                                                    as total_records,
                                       SUM(CASE WHEN snapshot_id IS NULL THEN 1 ELSE 0 END)        as null_snapshot_ids,
                                       SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)         as null_product_ids,
                                       SUM(CASE WHEN warehouse_id IS NULL THEN 1 ELSE 0 END)       as null_warehouse_ids,
                                       SUM(CASE WHEN is_stockout THEN 1 ELSE 0 END)                as stockout_count,
                                       SUM(CASE WHEN stock_status = 'low_stock' THEN 1 ELSE 0 END) as low_stock_count,
                                       SUM(CASE WHEN stock_status = 'in_stock' THEN 1 ELSE 0 END)  as in_stock_count,
                                       SUM(CASE WHEN stock_status = 'overstock' THEN 1 ELSE 0 END) as overstock_count
                                FROM silver.inventory_snapshots
                                """).collect()[0]

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null snapshot_id: {quality_metrics['null_snapshot_ids']}")
    print(f"  ✓ Records with null product_id: {quality_metrics['null_product_ids']}")
    print(f"  ✓ Records with null warehouse_id: {quality_metrics['null_warehouse_ids']}")
    print(f"  ✓ Stockout records: {quality_metrics['stockout_count']}")
    print(f"  ✓ Low stock records: {quality_metrics['low_stock_count']}")
    print(f"  ✓ In stock records: {quality_metrics['in_stock_count']}")
    print(f"  ✓ Overstock records: {quality_metrics['overstock_count']}")

    if quality_metrics['null_snapshot_ids'] > 0 or quality_metrics['null_product_ids'] > 0 or quality_metrics[
        'null_warehouse_ids'] > 0:
        print("  ⚠️  Warning: Found null values in key fields")

    # Calculate data quality percentage
    null_keys = max(quality_metrics['null_snapshot_ids'], quality_metrics['null_product_ids'],
                    quality_metrics['null_warehouse_ids'])
    valid_records = final_count - null_keys
    quality_pct = (valid_records / final_count * 100) if final_count > 0 else 0

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.inventory_snapshots")
    print("=" * 80)
    spark.sql("""
              SELECT snapshot_id,
                     snapshot_date,
                     product_id,
                     warehouse_id,
                     quantity_on_hand,
                     quantity_reserved,
                     quantity_available,
                     days_of_supply,
                     stock_status,
                     is_stockout
              FROM silver.inventory_snapshots LIMIT 5
              """).show(truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Transformation Summary")
    print("=" * 80)
    print(f"Input records (bronze):  {initial_count:,}")
    print(f"Output records (silver): {final_count:,}")
    print(f"Data quality: {quality_pct:.2f}% valid records")

    # Business metrics summary
    print("\n[4/4] Business Metrics Summary:")
    spark.sql("""
              SELECT COUNT(*)                     as total_snapshots,
                     COUNT(DISTINCT product_id)   as unique_products,
                     COUNT(DISTINCT warehouse_id) as unique_warehouses,
                     SUM(quantity_on_hand)        as total_qty_on_hand,
                     SUM(quantity_reserved)       as total_qty_reserved,
                     SUM(quantity_available)      as total_qty_available,
                     AVG(days_of_supply)          as avg_days_of_supply
              FROM silver.inventory_snapshots
              """).show(truncate=False)

    # Stock status distribution
    print("\nStock status distribution:")
    spark.sql("""
              SELECT stock_status,
                     COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
              FROM silver.inventory_snapshots
              GROUP BY stock_status
              ORDER BY count DESC
              """).show()

    # Warehouse inventory summary
    print("\nWarehouse inventory summary:")
    spark.sql("""
              SELECT warehouse_id,
                     COUNT(*)                                     as product_count,
                     SUM(quantity_available)                      as total_available,
                     SUM(CASE WHEN is_stockout THEN 1 ELSE 0 END) as stockout_count,
                     ROUND(AVG(days_of_supply), 1)                as avg_days_supply
              FROM silver.inventory_snapshots
              GROUP BY warehouse_id
              ORDER BY warehouse_id
              """).show()

    # Low stock and stockout products
    print("\nProducts needing attention (low stock or stockout):")
    spark.sql("""
              SELECT product_id,
                     warehouse_id,
                     quantity_available,
                     reorder_point,
                     stock_status,
                     is_stockout
              FROM silver.inventory_snapshots
              WHERE stock_status IN ('low_stock', 'out_of_stock')
              ORDER BY quantity_available LIMIT 10
              """).show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Inventory snapshots transformed to silver layer using Spark SQL")
    print("=" * 80)
    print("\nQuery examples:")
    print("  spark.sql('SELECT * FROM silver.inventory_snapshots WHERE is_stockout = true LIMIT 10').show()")
    print("  spark.sql('SELECT * FROM silver.inventory_snapshots WHERE stock_status = \"low_stock\" LIMIT 10').show()")
    print("  spark.sql('SELECT warehouse_id, COUNT(*) FROM silver.inventory_snapshots GROUP BY warehouse_id').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
