# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Limpieza de Datos Comercio Exterior
# MAGIC Normaliza datos de UN Comtrade para Peru.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Parametros del ambiente
dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# Read from Bronze (CosmosDB)
cosmos_endpoint = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-endpoint")
cosmos_key = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-primary-key")

config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "bronze-db",
    "spark.cosmos.container": "comtrade_peru",
    "spark.cosmos.read.inferSchema.enabled": "true",
}

df_raw = spark.read.format("cosmos.oltp").options(**config).load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpiar y estandarizar datos de comercio

# COMMAND ----------

df_trade_clean = (df_raw
    .select(
        F.col("partner_code").cast("int"),
        F.col("partner"),
        F.col("flow_code"),
        F.col("flow_desc"),
        F.col("year").cast("int"),
        F.col("cmd_code"),
        F.col("cmd_desc"),
        F.col("trade_value").cast("double"),
        F.col("net_weight").cast("double"),
        F.lit("UNComtrade").alias("source"),
    )
    .filter(F.col("trade_value").isNotNull())
    .filter(F.col("trade_value") > 0)
    .filter(F.col("year").isNotNull())
    .dropDuplicates(["partner_code", "flow_code", "year", "cmd_code"])
    .withColumn("trade_value_millions", F.round(F.col("trade_value") / 1000000, 2))
    .withColumn("processed_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir a Silver (Delta Lake)

# COMMAND ----------

silver_table = f"{catalog_name}.silver.trade_data"

# Handle empty DataFrame from CosmosDB
trade_count = df_trade_clean.count()
print(f"Trade records to write: {trade_count}")

if trade_count == 0:
    print("WARNING: No trade data to write. Bronze may not have ingested data yet.")
else:
    # Table pre-created in setup with LOCATION pointing to ADLS
    (df_trade_clean.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .insertInto(silver_table, overwrite=True))
    print(f"Silver write completed: {trade_count} records to {silver_table}")

    # Summary
    display(
        spark.table(silver_table)
        .groupBy("flow_desc", "partner")
        .agg(
            F.count("*").alias("records"),
            F.sum("trade_value_millions").alias("total_mill_usd"),
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
        )
        .orderBy("flow_desc", "partner")
    )
