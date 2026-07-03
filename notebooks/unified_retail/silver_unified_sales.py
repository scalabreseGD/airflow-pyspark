# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - unified_sales
# MAGIC
# MAGIC Line-item grain fact that merges retail (`silver.transactions` + `silver.transaction_items`)
# MAGIC and Cloudberry (`bronze.cloudberry_sales_order` + `bronze.cloudberry_sales_order_line`),
# MAGIC enriched with each domain's product master and normalized to EUR.
# MAGIC
# MAGIC **Output:** `unity_catalog.silver.unified_sales` (partitioned by `source_domain`)

# COMMAND ----------

# MAGIC %run ./_fx_helpers

# COMMAND ----------

from pyspark.sql import functions as F

# --- Retail side ------------------------------------------------------------
r_tx = spark.table(f"{CATALOG}.silver.transactions")
r_ti = spark.table(f"{CATALOG}.silver.transaction_items")
r_pc = spark.table(f"{CATALOG}.silver.product_catalog").filter(F.col("is_current") == True)

retail = (
    r_ti.alias("l")
    .join(r_tx.alias("h"), "transaction_id", "inner")
    .join(r_pc.alias("p"), F.col("l.product_id") == F.col("p.product_id"), "left")
    .select(
        F.lit("retail").alias("source_domain"),
        F.lit("retail").alias("source_system"),
        F.col("l.transaction_id").cast("string").alias("order_id"),
        F.col("l.transaction_item_id").cast("string").alias("line_no"),
        F.col("h.transaction_timestamp").alias("order_ts"),
        F.col("h.transaction_date").cast("date").alias("business_date"),
        F.col("h.channel").alias("channel_raw"),
        channel_group(F.col("h.channel")).alias("channel_group"),
        F.col("h.customer_id").cast("string").alias("customer_id"),
        F.col("h.store_id").cast("string").alias("store_id"),
        F.lit("UNK").alias("country_code"),
        F.col("l.product_id").cast("string").alias("sku"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.brand").alias("brand"),
        F.col("p.category_level1").alias("category"),
        F.col("l.net_quantity").cast("decimal(18,3)").alias("quantity"),
        F.col("h.currency").alias("currency_code"),
        F.col("l.line_total").cast("decimal(18,2)").alias("revenue_native"),
        F.col("h.currency").alias("revenue_ccy"),
        F.col("l.line_cost").cast("decimal(18,2)").alias("cost_native"),
        F.col("h.currency").alias("cost_ccy"),
        F.col("l.line_discount").cast("decimal(18,2)").alias("discount_native"),
        (F.coalesce(F.col("l.return_quantity"), F.lit(0)) > 0).alias("is_return"),
    )
)

# --- Cloudberry side --------------------------------------------------------
c_so = spark.table(f"{CATALOG}.bronze.cloudberry_sales_order")
c_sol = spark.table(f"{CATALOG}.bronze.cloudberry_sales_order_line")
c_dp = spark.table(f"{CATALOG}.bronze.cloudberry_dim_product").filter(F.col("is_current") == True)

cloudberry = (
    c_sol.alias("l")
    .join(c_so.alias("h"), "sales_order_id", "inner")
    .join(c_dp.alias("p"), F.col("l.sku") == F.col("p.sku"), "left")
    .select(
        F.lit("cloudberry").alias("source_domain"),
        F.coalesce(F.col("h.source_system"), F.lit("cloudberry")).alias("source_system"),
        F.col("l.sales_order_id").cast("string").alias("order_id"),
        F.col("l.line_no").cast("string").alias("line_no"),
        F.col("h.order_ts").alias("order_ts"),
        F.col("h.business_date").cast("date").alias("business_date"),
        F.col("h.channel").alias("channel_raw"),
        channel_group(F.col("h.channel")).alias("channel_group"),
        F.col("h.customer_id").cast("string").alias("customer_id"),
        F.col("h.store_code").cast("string").alias("store_id"),
        F.col("h.country_code").alias("country_code"),
        F.col("l.sku").alias("sku"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.brand_name").alias("brand"),
        F.col("p.category_name").alias("category"),
        F.col("l.quantity").cast("decimal(18,3)").alias("quantity"),
        F.col("h.currency_code").alias("currency_code"),
        F.col("l.line_total").cast("decimal(18,2)").alias("revenue_native"),
        F.col("h.currency_code").alias("revenue_ccy"),
        (F.col("l.quantity") * F.coalesce(F.col("p.cost_price"), F.lit(0))).cast("decimal(18,2)").alias("cost_native"),
        F.coalesce(F.col("p.currency_code"), F.lit("EUR")).alias("cost_ccy"),
        F.col("l.discount_amount").cast("decimal(18,2)").alias("discount_native"),
        F.lit(False).alias("is_return"),
    )
)

unified = retail.unionByName(cloudberry)

# EUR normalization (revenue and cost may be in different currencies per row)
unified = to_eur(unified, "revenue_native", "revenue_ccy", "business_date", "revenue_eur")
unified = to_eur(unified, "cost_native", "cost_ccy", "business_date", "cost_eur")
unified = to_eur(unified, "discount_native", "revenue_ccy", "business_date", "discount_eur")
unified = unified.withColumn(
    "margin_eur", (F.col("revenue_eur") - F.col("cost_eur")).cast("decimal(18,2)")
)

(
    unified.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("source_domain")
    .saveAsTable(f"{CATALOG}.silver.unified_sales")
)
print("unified_sales rows:", spark.table(f"{CATALOG}.silver.unified_sales").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain,
               count(*)                    AS lines,
               count(DISTINCT order_id)    AS orders,
               round(sum(revenue_eur), 0)  AS revenue_eur,
               round(sum(margin_eur), 0)   AS margin_eur,
               min(business_date)          AS from_date,
               max(business_date)          AS to_date
        FROM {CATALOG}.silver.unified_sales
        GROUP BY source_domain
        ORDER BY revenue_eur DESC
        """
    )
)
