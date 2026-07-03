# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - unified_product
# MAGIC
# MAGIC Product master merging retail `silver.product_catalog` (current rows) and Cloudberry
# MAGIC `bronze.cloudberry_dim_product` (current rows). List price normalized to EUR using each
# MAGIC currency's average FX rate (product master carries no date).
# MAGIC
# MAGIC **Output:** `unity_catalog.silver.unified_product`

# COMMAND ----------

# MAGIC %run ./_fx_helpers

# COMMAND ----------

from pyspark.sql import functions as F

r_pc = spark.table(f"{CATALOG}.silver.product_catalog").filter(F.col("is_current") == True)
c_dp = spark.table(f"{CATALOG}.bronze.cloudberry_dim_product").filter(F.col("is_current") == True)

r_prod = r_pc.select(
    F.lit("retail").alias("source_domain"),
    F.col("product_id").cast("string").alias("product_key"),
    F.col("product_id").cast("string").alias("sku"),
    F.col("product_name"),
    F.col("brand"),
    F.col("category_level1").alias("category"),
    F.col("list_price").cast("decimal(18,2)").alias("list_price_native"),
    F.lit("USD").alias("price_ccy"),
    F.col("unit_cost").cast("decimal(18,2)").alias("unit_cost_native"),
    F.col("is_active"),
)

c_prod = c_dp.select(
    F.lit("cloudberry").alias("source_domain"),
    F.col("sku").alias("product_key"),
    F.col("sku"),
    F.col("product_name"),
    F.col("brand_name").alias("brand"),
    F.col("category_name").alias("category"),
    F.col("list_price").cast("decimal(18,2)").alias("list_price_native"),
    F.coalesce(F.col("currency_code"), F.lit("EUR")).alias("price_ccy"),
    F.col("cost_price").cast("decimal(18,2)").alias("unit_cost_native"),
    F.col("is_active"),
)

products = r_prod.unionByName(c_prod)

# Product master has no date, so convert list price using each currency's average rate.
products = (
    products.withColumn("_ccy", F.upper("price_ccy"))
    .join(fx_avg, F.col("_ccy") == fx_avg.ccy, "left")
    .drop("ccy")
    .withColumn(
        "list_price_eur",
        F.when(F.col("_ccy") == "EUR", F.col("list_price_native"))
        .otherwise((F.col("list_price_native") * F.coalesce("to_eur_avg", F.lit(1.0))))
        .cast("decimal(18,2)"),
    )
    .drop("_ccy", "to_eur_avg")
)

(
    products.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.unified_product")
)
print("unified_product rows:", spark.table(f"{CATALOG}.silver.unified_product").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain, count(*) AS products,
               count(DISTINCT brand) AS brands,
               round(avg(list_price_eur), 2) AS avg_list_price_eur
        FROM {CATALOG}.silver.unified_product
        GROUP BY source_domain
        """
    )
)
