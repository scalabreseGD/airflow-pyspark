# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - unified_sales_daily
# MAGIC
# MAGIC Daily KPIs by source / channel / country over `silver.unified_sales` (already EUR-normalized).
# MAGIC
# MAGIC **Output:** `unity_catalog.gold.unified_sales_daily`  ·  Depends on `silver_unified_sales`.

# COMMAND ----------

CATALOG = "unity_catalog"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.unified_sales_daily AS
    SELECT
        business_date,
        source_domain,
        source_system,
        channel_group,
        coalesce(country_code, 'UNK')            AS country_code,
        count(DISTINCT order_id)                 AS orders,
        count(DISTINCT customer_id)              AS distinct_customers,
        count(DISTINCT sku)                      AS distinct_skus,
        sum(quantity)                            AS units,
        sum(revenue_eur)                         AS revenue_eur,
        sum(cost_eur)                            AS cost_eur,
        sum(margin_eur)                          AS margin_eur,
        sum(discount_eur)                        AS discount_eur,
        round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 2)      AS gross_margin_pct,
        round(sum(revenue_eur) / nullif(count(DISTINCT order_id), 0), 2)   AS avg_order_value_eur
    FROM {CATALOG}.silver.unified_sales
    GROUP BY business_date, source_domain, source_system, channel_group, coalesce(country_code, 'UNK')
    """
)
print("unified_sales_daily rows:", spark.table(f"{CATALOG}.gold.unified_sales_daily").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain,
               round(sum(revenue_eur), 0) AS revenue_eur,
               round(sum(margin_eur), 0)  AS margin_eur,
               sum(orders)                AS orders,
               round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 1) AS margin_pct
        FROM {CATALOG}.gold.unified_sales_daily
        GROUP BY source_domain
        ORDER BY revenue_eur DESC
        """
    )
)
