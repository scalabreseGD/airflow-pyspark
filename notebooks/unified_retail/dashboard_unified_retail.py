# Databricks notebook source
# MAGIC %md
# MAGIC # Unified Retail + Cloudberry - Executive Dashboard
# MAGIC
# MAGIC Visual analytics over the unified gold marts. Each cell renders one tile with `display()`
# MAGIC so you can pin it to a Databricks dashboard (**cell menu -> Add to dashboard**), or use these
# MAGIC queries directly as datasets in an AI/BI dashboard.
# MAGIC
# MAGIC Depends on `gold_unified_analytics`.

# COMMAND ----------

CATALOG = "unity_catalog"

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI headline - revenue, margin, orders by domain

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            source_domain,
            round(sum(revenue_eur))                                          AS revenue_eur,
            round(sum(margin_eur))                                           AS margin_eur,
            round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 1)    AS gross_margin_pct,
            sum(orders)                                                      AS orders,
            round(sum(revenue_eur) / nullif(sum(orders), 0), 2)             AS avg_order_value_eur,
            round(sum(units))                                                AS units
        FROM {CATALOG}.gold.unified_sales_daily
        GROUP BY source_domain
        ORDER BY revenue_eur DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly revenue trend by domain
# MAGIC Line chart: X = `month`, Y = `revenue_eur`, series grouping = `source_domain`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT date_format(business_date, 'yyyy-MM') AS month,
               source_domain,
               round(sum(revenue_eur))              AS revenue_eur,
               round(sum(margin_eur))               AS margin_eur
        FROM {CATALOG}.gold.unified_sales_daily
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Channel mix
# MAGIC Bar chart: X = `channel_group`, Y = `revenue_eur`, series grouping = `source_domain`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT channel_group,
               source_domain,
               round(sum(revenue_eur)) AS revenue_eur,
               sum(orders)             AS orders
        FROM {CATALOG}.gold.unified_sales_daily
        GROUP BY channel_group, source_domain
        ORDER BY revenue_eur DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 15 brands by revenue (both domains)
# MAGIC Bar chart: X = `brand`, Y = `revenue_eur`, series grouping = `source_domain`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH b AS (
            SELECT brand, source_domain,
                   sum(revenue_eur)       AS revenue_eur,
                   avg(gross_margin_pct)  AS gross_margin_pct
            FROM {CATALOG}.gold.unified_product_performance
            GROUP BY brand, source_domain
        ),
        ranked AS (
            SELECT *,
                   dense_rank() OVER (ORDER BY sum(revenue_eur) OVER (PARTITION BY brand) DESC) AS brand_rank
            FROM b
        )
        SELECT brand, source_domain,
               round(revenue_eur)          AS revenue_eur,
               round(gross_margin_pct, 1)  AS gross_margin_pct
        FROM ranked
        WHERE brand_rank <= 15
        ORDER BY revenue_eur DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by country
# MAGIC Map or bar chart: key = `country_code`, value = `revenue_eur`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT coalesce(country_code, 'UNK') AS country_code,
               round(sum(revenue_eur))       AS revenue_eur,
               sum(orders)                   AS orders,
               round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 1) AS gross_margin_pct
        FROM {CATALOG}.gold.unified_sales_daily
        GROUP BY coalesce(country_code, 'UNK')
        ORDER BY revenue_eur DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gross margin % by category
# MAGIC Bar chart: X = `category`, Y = `gross_margin_pct`, series grouping = `source_domain`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT category,
               source_domain,
               round(sum(revenue_eur))                                       AS revenue_eur,
               round(sum(margin_eur) / nullif(sum(revenue_eur), 0) * 100, 1) AS gross_margin_pct
        FROM {CATALOG}.gold.unified_product_performance
        GROUP BY category, source_domain
        HAVING sum(revenue_eur) > 0
        ORDER BY revenue_eur DESC
        LIMIT 20
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 20 products (table)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain, brand, category, sku, product_name,
               round(sum(revenue_eur)) AS revenue_eur,
               round(sum(margin_eur))  AS margin_eur,
               sum(units)              AS units,
               round(avg(gross_margin_pct), 1) AS gross_margin_pct
        FROM {CATALOG}.gold.unified_product_performance
        GROUP BY source_domain, brand, category, sku, product_name
        ORDER BY revenue_eur DESC
        LIMIT 20
        """
    )
)
