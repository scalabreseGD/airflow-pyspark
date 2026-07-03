# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - unified_store
# MAGIC
# MAGIC Store master. Cloudberry has a full store dimension; the retail sample has only `store_id`s,
# MAGIC so those are added as minimal entries so cross-source store analytics still resolve.
# MAGIC
# MAGIC **Output:** `unity_catalog.silver.unified_store`

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "unity_catalog"

# COMMAND ----------

c_store = (
    spark.table(f"{CATALOG}.bronze.cloudberry_store")
    .filter(F.col("is_current") == True)
    .select(
        F.lit("cloudberry").alias("source_domain"),
        F.col("store_code").cast("string").alias("store_key"),
        F.col("store_name"),
        F.col("country_code"),
        F.col("region"),
        F.col("city"),
        F.col("store_format"),
        F.col("is_active"),
    )
)

r_store = (
    spark.table(f"{CATALOG}.silver.transactions")
    .select(F.col("store_id").cast("string").alias("store_key"))
    .distinct()
    .select(
        F.lit("retail").alias("source_domain"),
        F.col("store_key"),
        F.concat(F.lit("Retail store "), F.col("store_key")).alias("store_name"),
        F.lit("UNK").alias("country_code"),
        F.lit(None).cast("string").alias("region"),
        F.lit(None).cast("string").alias("city"),
        F.lit(None).cast("string").alias("store_format"),
        F.lit(True).alias("is_active"),
    )
)

stores = c_store.unionByName(r_store)
(
    stores.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.unified_store")
)
print("unified_store rows:", spark.table(f"{CATALOG}.silver.unified_store").count())

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_domain, count(*) AS stores, count(DISTINCT country_code) AS countries
        FROM {CATALOG}.silver.unified_store
        GROUP BY source_domain
        """
    )
)
