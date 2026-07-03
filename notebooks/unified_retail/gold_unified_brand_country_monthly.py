# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - unified_brand_country_monthly
# MAGIC
# MAGIC Monthly brand x country x channel trend across both domains over `silver.unified_sales`.
# MAGIC
# MAGIC **Output:** `unity_catalog.gold.unified_brand_country_monthly`  ·  Depends on `silver_unified_sales`.

# COMMAND ----------

CATALOG = "unity_catalog"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.unified_brand_country_monthly AS
    SELECT
        date_format(business_date, 'yyyy-MM')    AS year_month,
        source_domain,
        coalesce(brand, 'Unknown')               AS brand,
        coalesce(country_code, 'UNK')            AS country_code,
        channel_group,
        count(DISTINCT order_id)                 AS orders,
        sum(quantity)                            AS units,
        sum(revenue_eur)                         AS revenue_eur,
        sum(margin_eur)                          AS margin_eur,
        round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 2)  AS gross_margin_pct
    FROM {CATALOG}.silver.unified_sales
    GROUP BY date_format(business_date, 'yyyy-MM'), source_domain,
             coalesce(brand, 'Unknown'), coalesce(country_code, 'UNK'), channel_group
    """
)
print("unified_brand_country_monthly rows:", spark.table(f"{CATALOG}.gold.unified_brand_country_monthly").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT year_month, source_domain,
               round(sum(revenue_eur)) AS revenue_eur,
               round(sum(margin_eur))  AS margin_eur
        FROM {CATALOG}.gold.unified_brand_country_monthly
        GROUP BY year_month, source_domain
        ORDER BY year_month, source_domain
        """
    )
)
