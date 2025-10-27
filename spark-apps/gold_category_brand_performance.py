"""
Gold Layer - Category & Brand Performance (Spark SQL)

Analyzes category and brand sales performance across multiple time periods.
Includes growth metrics, market share, customer penetration, and rankings.

Input: silver.transaction_items, silver.product_catalog, silver.transactions
Output: gold.category_brand_performance (Parquet, partitioned by analysis_date, time_period)

Usage:
    ./submit.sh gold_category_brand_performance.py
"""

import argparse

from pyspark.sql import SparkSession

print("=" * 80)
print("  Gold Layer - Category & Brand Performance (Spark SQL)")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

try:

    print("\n[1/3] Reading silver layer...")
    initial_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.transaction_items").collect()[0]['cnt']
    print(f"  ✓ Read {initial_count:,} transaction items")

    print("\n[2/3] Executing SQL transformation for monthly analysis...")

    transformation_sql = """
    INSERT OVERWRITE TABLE gold.category_brand_performance
    PARTITION (analysis_date, time_period)

    WITH period_totals AS (
        SELECT
            DATE_TRUNC('MONTH', transaction_date) as period_date,
            COUNT(DISTINCT customer_id) as total_period_customers
        FROM silver.transactions
        GROUP BY DATE_TRUNC('MONTH', transaction_date)
    ),
    base_metrics AS (
        SELECT
            DATE_TRUNC('MONTH', t.transaction_date) as period_date,
            'monthly' as time_period,
            p.category_level1,
            p.category_level2,
            p.category_level3,
            p.brand,

            -- Sales Metrics
            SUM(i.line_total) as total_revenue,
            SUM(i.net_quantity) as total_units_sold,
            COUNT(DISTINCT i.transaction_id) as total_transactions,
            COUNT(DISTINCT t.customer_id) as unique_customers,

            -- Average Metrics
            AVG(t.total_amount) as avg_transaction_value,
            AVG(i.net_quantity) as avg_units_per_transaction,
            AVG(i.unit_price) as avg_price_point,

            -- Profitability
            SUM(i.line_profit) as total_profit,
            AVG(i.line_margin_percentage) as profit_margin_percentage,

            -- Discount Metrics
            AVG(i.discount_percentage) as avg_discount_percentage,
            SUM(CASE WHEN i.discount_percentage > 0 THEN i.line_total ELSE 0 END) / NULLIF(SUM(i.line_total), 0) * 100 as promotional_sales_percentage,

            -- Customer Metrics
            COUNT(DISTINCT CASE WHEN t.is_first_purchase THEN t.customer_id END) as new_customers,
            COUNT(DISTINCT CASE WHEN NOT t.is_first_purchase THEN t.customer_id END) as repeat_customers

        FROM silver.transaction_items i
        INNER JOIN silver.product_catalog p ON i.product_id = p.product_id AND p.is_current = true
        INNER JOIN silver.transactions t ON i.transaction_id = t.transaction_id
        GROUP BY
            DATE_TRUNC('MONTH', t.transaction_date),
            p.category_level1,
            p.category_level2,
            p.category_level3,
            p.brand
    ),
    base_with_period_totals AS (
        SELECT bm.*, pt.total_period_customers
        FROM base_metrics bm
        LEFT JOIN period_totals pt ON bm.period_date = pt.period_date
    ),

    -- Calculate market share within category
    category_totals AS (
        SELECT
            period_date,
            time_period,
            category_level1,
            SUM(total_revenue) as category_total_revenue
        FROM base_with_period_totals
        GROUP BY period_date, time_period, category_level1
    ),
    brand_totals AS (
        SELECT
            period_date,
            category_level1,
            brand,
            SUM(total_revenue) as brand_total_revenue
        FROM base_with_period_totals
        GROUP BY period_date, category_level1, brand
    ),

    -- Add market share
    metrics_with_share AS (
        SELECT
            bm.*,
            ct.category_total_revenue,
            bm.total_revenue / NULLIF(ct.category_total_revenue, 0) * 100 as category_revenue_percentage,
            bm.total_revenue / NULLIF(bt.brand_total_revenue, 0) * 100 as brand_revenue_percentage
        FROM base_with_period_totals bm
        LEFT JOIN category_totals ct
            ON bm.period_date = ct.period_date
            AND bm.time_period = ct.time_period
            AND bm.category_level1 = ct.category_level1
        LEFT JOIN brand_totals bt
            ON bm.period_date = bt.period_date
            AND bm.category_level1 = bt.category_level1
            AND bm.brand = bt.brand
    ),

    -- Calculate growth metrics using LAG
    metrics_with_growth AS (
        SELECT
            *,
            -- Week over Week (approximate from monthly)
            (total_revenue - LAG(total_revenue, 1) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date)) /
                NULLIF(LAG(total_revenue, 1) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date), 0) * 100 as revenue_growth_wow,

            -- Month over Month
            (total_revenue - LAG(total_revenue, 1) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date)) /
                NULLIF(LAG(total_revenue, 1) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date), 0) * 100 as revenue_growth_mom,

            -- Year over Year
            (total_revenue - LAG(total_revenue, 12) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date)) /
                NULLIF(LAG(total_revenue, 12) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date), 0) * 100 as revenue_growth_yoy,

            -- Units Year over Year
            (total_units_sold - LAG(total_units_sold, 12) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date)) /
                NULLIF(LAG(total_units_sold, 12) OVER (PARTITION BY category_level1, category_level2, brand ORDER BY period_date), 0) * 100 as units_growth_yoy
        FROM metrics_with_share
    ),

    -- Calculate rankings
    final_metrics AS (
        SELECT
            CURRENT_DATE() as analysis_date,
            time_period,
            category_level1,
            category_level2,
            category_level3,
            brand,

            -- Sales Metrics
            CAST(total_revenue AS DECIMAL(15,2)) as total_revenue,
            total_units_sold,
            total_transactions,
            unique_customers,

            -- Average Metrics
            CAST(avg_transaction_value AS DECIMAL(10,2)) as avg_transaction_value,
            CAST(avg_units_per_transaction AS DECIMAL(8,2)) as avg_units_per_transaction,
            CAST(avg_price_point AS DECIMAL(10,2)) as avg_price_point,

            -- Profitability
            CAST(total_profit AS DECIMAL(15,2)) as total_profit,
            CAST(profit_margin_percentage AS DECIMAL(5,2)) as profit_margin_percentage,

            -- Market Share
            CAST(category_revenue_percentage AS DECIMAL(5,2)) as category_revenue_percentage,
            CAST(brand_revenue_percentage AS DECIMAL(5,2)) as brand_revenue_percentage,

            -- Growth Metrics
            CAST(COALESCE(revenue_growth_wow, 0) AS DECIMAL(8,2)) as revenue_growth_wow,
            CAST(COALESCE(revenue_growth_mom, 0) AS DECIMAL(8,2)) as revenue_growth_mom,
            CAST(COALESCE(revenue_growth_yoy, 0) AS DECIMAL(8,2)) as revenue_growth_yoy,
            CAST(COALESCE(units_growth_yoy, 0) AS DECIMAL(8,2)) as units_growth_yoy,

            -- Customer Penetration
            CAST(unique_customers / NULLIF(total_period_customers, 0) * 100 AS DECIMAL(5,2)) as customer_penetration_rate,
            CAST(new_customers / NULLIF(unique_customers, 0) * 100 AS DECIMAL(5,2)) as new_customer_percentage,
            CAST(repeat_customers / NULLIF(unique_customers, 0) * 100 AS DECIMAL(5,2)) as repeat_customer_rate,

            -- Price & Promotion
            CAST(avg_discount_percentage AS DECIMAL(5,2)) as avg_discount_percentage,
            CAST(promotional_sales_percentage AS DECIMAL(5,2)) as promotional_sales_percentage,
            CAST(avg_price_point / NULLIF(
                AVG(avg_price_point) OVER (PARTITION BY category_level1), 0
            ) * 100 AS DECIMAL(8,2)) as price_index,

            -- Rankings
            DENSE_RANK() OVER (PARTITION BY category_level1, period_date ORDER BY total_revenue DESC) as revenue_rank_in_category,
            DENSE_RANK() OVER (PARTITION BY category_level1, period_date ORDER BY total_units_sold DESC) as units_rank_in_category,
            DENSE_RANK() OVER (PARTITION BY category_level1, period_date ORDER BY revenue_growth_yoy DESC) as growth_rank_in_category,

            -- Metadata
            CURRENT_TIMESTAMP() as created_timestamp

        FROM metrics_with_growth
        WHERE period_date IS NOT NULL
    )

    SELECT
        category_level1,
        category_level2,
        category_level3,
        brand,
        total_revenue,
        total_units_sold,
        total_transactions,
        unique_customers,
        avg_transaction_value,
        avg_units_per_transaction,
        avg_price_point,
        total_profit,
        profit_margin_percentage,
        category_revenue_percentage,
        brand_revenue_percentage,
        revenue_growth_wow,
        revenue_growth_mom,
        revenue_growth_yoy,
        units_growth_yoy,
        customer_penetration_rate,
        new_customer_percentage,
        repeat_customer_rate,
        avg_discount_percentage,
        promotional_sales_percentage,
        price_index,
        revenue_rank_in_category,
        units_rank_in_category,
        growth_rank_in_category,
        created_timestamp,
        analysis_date,
        time_period
    FROM final_metrics
    """

    spark.sql(transformation_sql)
    print("  ✓ SQL transformation complete")

    print("\n[3/3] Data quality checks...")
    final_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.category_brand_performance").collect()[0]['cnt']
    print(f"  ✓ Created {final_count:,} category-brand-period records")

    print("\n" + "=" * 80)
    print("  Top Categories by Revenue (Current Period)")
    print("=" * 80)
    spark.sql("""
              SELECT category_level1,
                     brand,
                     SUM(total_revenue)            as revenue,
                     AVG(revenue_growth_yoy)       as avg_yoy_growth,
                     MAX(revenue_rank_in_category) as best_rank
              FROM gold.category_brand_performance
              WHERE time_period = 'monthly'
              GROUP BY category_level1, brand
              ORDER BY revenue DESC LIMIT 10
              """).show(truncate=False)

    print("\n" + "=" * 80)
    print("  Category Growth Leaders")
    print("=" * 80)
    spark.sql("""
              SELECT category_level1,
                     brand,
                     total_revenue,
                     revenue_growth_yoy,
                     growth_rank_in_category
              FROM gold.category_brand_performance
              WHERE time_period = 'monthly'
                AND revenue_growth_yoy > 0
              ORDER BY revenue_growth_yoy DESC LIMIT 10
              """).show(truncate=False)

    print("\n" + "=" * 80)
    print("  SUCCESS! Category & brand performance complete")
    print("=" * 80)
    print("\nQuery examples:")
    print(
        "  spark.sql('SELECT * FROM gold.category_brand_performance WHERE category_level1 = \"Electronics\" ORDER BY total_revenue DESC').show()")
    print(
        "  spark.sql('SELECT category_level1, AVG(revenue_growth_yoy) FROM gold.category_brand_performance GROUP BY category_level1').show()")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise
finally:
    spark.stop()
