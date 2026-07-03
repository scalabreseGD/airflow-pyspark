# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - unified_product_performance
# MAGIC
# MAGIC Product / brand / category performance across both domains over `silver.unified_sales`.
# MAGIC
# MAGIC **Output:** `unity_catalog.gold.unified_product_performance`  ·  Depends on `silver_unified_sales`.

# COMMAND ----------

CATALOG = "unity_catalog"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.unified_product_performance AS
    SELECT
        source_domain,
        coalesce(brand, 'Unknown')       AS brand,
        coalesce(category, 'Unknown')    AS category,
        sku,
        max(product_name)                AS product_name,
        count(DISTINCT order_id)         AS orders,
        sum(quantity)                    AS units,
        sum(CASE WHEN is_return THEN quantity ELSE 0 END)  AS return_units,
        sum(revenue_eur)                 AS revenue_eur,
        sum(cost_eur)                    AS cost_eur,
        sum(margin_eur)                  AS margin_eur,
        round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 2)  AS gross_margin_pct,
        round(sum(revenue_eur) / nullif(sum(quantity), 0), 2)         AS avg_selling_price_eur
    FROM {CATALOG}.silver.unified_sales
    GROUP BY source_domain, coalesce(brand, 'Unknown'), coalesce(category, 'Unknown'), sku
    """
)
print("unified_product_performance rows:", spark.table(f"{CATALOG}.gold.unified_product_performance").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain, brand, category, sku, product_name,
               round(revenue_eur) AS revenue_eur, round(margin_eur) AS margin_eur, units
        FROM {CATALOG}.gold.unified_product_performance
        ORDER BY revenue_eur DESC
        LIMIT 20
        """
    )
)
