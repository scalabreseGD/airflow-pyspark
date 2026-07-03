# Databricks notebook source
# MAGIC %md
# MAGIC # _fx_helpers - shared helpers for the unified silver notebooks
# MAGIC
# MAGIC Loaded by the silver build notebooks via `%run ./_fx_helpers`. Defines:
# MAGIC - `CATALOG`
# MAGIC - `fx_daily`, `fx_avg` - EUR conversion lookups from `bronze.cloudberry_fx_rate`
# MAGIC - `to_eur(df, amount_col, ccy_col, date_col, out_col)` - date-aware currency->EUR
# MAGIC - `channel_group(col)` - canonical channel mapping

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "unity_catalog"

# COMMAND ----------

# `cloudberry_fx_rate` stores `EUR -> currency_to` at `rate_date`. To convert an amount in
# `currency` to EUR we multiply by `1 / rate`. EUR passes through as-is. Join on the exact
# business date; fall back to each currency's average rate when a day is missing.
fx = spark.table(f"{CATALOG}.bronze.cloudberry_fx_rate")

fx_daily = (
    fx.select(
        F.upper("currency_to").alias("ccy"),
        F.to_date("rate_date").alias("d"),
        (F.lit(1.0) / F.col("rate")).cast("double").alias("to_eur"),
    )
    .groupBy("ccy", "d")
    .agg(F.avg("to_eur").alias("to_eur"))
)

fx_avg = fx_daily.groupBy("ccy").agg(F.avg("to_eur").alias("to_eur_avg"))


def to_eur(df, amount_col, ccy_col, date_col, out_col):
    """Add `out_col` = amount converted to EUR, robust to missing rates / currencies."""
    d = (
        df.withColumn("_ccy", F.upper(F.col(ccy_col)))
        .withColumn("_d", F.to_date(F.col(date_col)))
        .join(fx_daily, (F.col("_ccy") == fx_daily.ccy) & (F.col("_d") == fx_daily.d), "left")
        .drop("ccy", "d")
        .join(fx_avg, F.col("_ccy") == fx_avg.ccy, "left")
        .drop("ccy")
    )
    mult = (
        F.when(F.col("_ccy") == "EUR", F.lit(1.0))
        .otherwise(F.coalesce(F.col("to_eur"), F.col("to_eur_avg"), F.lit(1.0)))
    )
    return d.withColumn(out_col, (F.col(amount_col) * mult).cast("decimal(18,2)")).drop(
        "_ccy", "_d", "to_eur", "to_eur_avg"
    )


def channel_group(col):
    c = F.lower(col)
    return (
        F.when(c.isin("store", "in-store", "instore", "retail"), F.lit("store"))
        .when(c.isin("web", "online", "ecom", "e-commerce"), F.lit("web"))
        .when(c == "mobile", F.lit("mobile"))
        .when(c == "app", F.lit("app"))
        .when(c == "marketplace", F.lit("marketplace"))
        .otherwise(F.lit("other"))
    )
