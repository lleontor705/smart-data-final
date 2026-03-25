# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Limpieza de Indicadores Economicos
# MAGIC Unifica datos de BCRP y World Bank, limpia y estandariza.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Parametros del ambiente (pasados por el workflow o widgets)
dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# Read from Bronze (CosmosDB)
cosmos_endpoint = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-endpoint")
cosmos_key = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-primary-key")

def read_cosmos_collection(container):
    config = {
        "spark.cosmos.accountEndpoint": cosmos_endpoint,
        "spark.cosmos.accountKey": cosmos_key,
        "spark.cosmos.database": "bronze-db",
        "spark.cosmos.container": container,
        "spark.cosmos.read.inferSchema.enabled": "true",
        "spark.cosmos.read.inferSchema.forceNullableProperties": "true",
    }
    df = spark.read.format("cosmos.oltp").options(**config).load()
    row_count = df.count()
    print(f"Container '{container}': {row_count} rows, columns={df.columns}")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Limpiar datos BCRP

# COMMAND ----------

df_bcrp = read_cosmos_collection("bcrp_indicators")
print("BCRP schema:")
df_bcrp.printSchema()
bcrp_cols = df_bcrp.columns
print(f"BCRP columns: {bcrp_cols}")

# Handle empty container - inferSchema returns 0 columns when no data
bcrp_has_data = len(bcrp_cols) > 0 and df_bcrp.count() > 0

if bcrp_has_data:
    # Map columns dynamically with fallbacks
    indicator_id_col = next((c for c in ["series_id", "id"] if c in bcrp_cols), None)
    indicator_name_col = next((c for c in ["series_name", "name", "source"] if c in bcrp_cols), None)
    category_col = next((c for c in ["category", "_partition_key"] if c in bcrp_cols), None)
    period_col = next((c for c in ["period", "date"] if c in bcrp_cols), None)
    value_col = next((c for c in ["value", "amount"] if c in bcrp_cols), None)

    missing = []
    if not indicator_id_col: missing.append("indicator_id")
    if not indicator_name_col: missing.append("indicator_name")
    if not category_col: missing.append("category")
    if not period_col: missing.append("period")
    if not value_col: missing.append("value")

    if missing:
        raise RuntimeError(
            f"Cannot find columns {missing} in BCRP data. Available: {bcrp_cols}"
        )

    print(f"BCRP column mapping: id={indicator_id_col}, name={indicator_name_col}, "
          f"cat={category_col}, period={period_col}, val={value_col}")

    df_bcrp_clean = (df_bcrp
        .select(
            F.col(indicator_id_col).alias("indicator_id"),
            F.col(indicator_name_col).alias("indicator_name"),
            F.col(category_col).alias("category"),
            F.lit("BCRP").alias("source"),
            F.col(period_col).alias("period"),
            F.col(value_col).alias("value"),
        )
        .withColumn("value_numeric",
            F.regexp_replace(F.col("value"), "[^0-9.-]", "").cast("double"))
        .withColumn("year",
            F.regexp_extract(F.col("period"), r"(\d{4})", 1).cast("int"))
        .withColumn("month",
            F.when(F.col("period").contains("Ene"), 1)
             .when(F.col("period").contains("Feb"), 2)
             .when(F.col("period").contains("Mar"), 3)
             .when(F.col("period").contains("Abr"), 4)
             .when(F.col("period").contains("May"), 5)
             .when(F.col("period").contains("Jun"), 6)
             .when(F.col("period").contains("Jul"), 7)
             .when(F.col("period").contains("Ago"), 8)
             .when(F.col("period").contains("Sep"), 9)
             .when(F.col("period").contains("Oct"), 10)
             .when(F.col("period").contains("Nov"), 11)
             .when(F.col("period").contains("Dic"), 12)
             .otherwise(1))
        .withColumn("date",
            F.to_date(F.concat_ws("-", F.col("year"), F.lpad(F.col("month"), 2, "0"), F.lit("01"))))
        .filter(F.col("value_numeric").isNotNull())
        .filter(F.col("date").isNotNull())
        .dropDuplicates(["indicator_id", "date"])
    )
else:
    print("WARNING: BCRP container is empty - bronze may not have ingested data yet")
    bcrp_schema = StructType([
        StructField("indicator_id", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source", StringType(), True),
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("value_numeric", DoubleType(), True),
    ])
    df_bcrp_clean = spark.createDataFrame([], bcrp_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpiar datos World Bank

# COMMAND ----------

df_wb = read_cosmos_collection("worldbank_indicators")
print("WorldBank schema:")
df_wb.printSchema()
wb_cols = df_wb.columns
print(f"WorldBank columns: {wb_cols}")

wb_has_data = len(wb_cols) > 0 and df_wb.count() > 0

if wb_has_data:
    wb_indicator_id_col = next((c for c in ["indicator_id", "id"] if c in wb_cols), None)
    wb_indicator_name_col = next((c for c in ["indicator_name", "name", "source"] if c in wb_cols), None)
    wb_category_col = next((c for c in ["category", "_partition_key"] if c in wb_cols), None)
    wb_year_col = next((c for c in ["year", "period", "date"] if c in wb_cols), None)
    wb_value_col = next((c for c in ["value", "amount"] if c in wb_cols), None)

    missing = []
    if not wb_indicator_id_col: missing.append("indicator_id")
    if not wb_indicator_name_col: missing.append("indicator_name")
    if not wb_category_col: missing.append("category")
    if not wb_year_col: missing.append("year")
    if not wb_value_col: missing.append("value")

    if missing:
        raise RuntimeError(
            f"Cannot find columns {missing} in WorldBank data. Available: {wb_cols}"
        )

    print(f"WorldBank column mapping: id={wb_indicator_id_col}, name={wb_indicator_name_col}, "
          f"cat={wb_category_col}, year={wb_year_col}, val={wb_value_col}")

    df_wb_clean = (df_wb
        .select(
            F.col(wb_indicator_id_col).alias("indicator_id"),
            F.col(wb_indicator_name_col).alias("indicator_name"),
            F.col(wb_category_col).alias("category"),
            F.lit("WorldBank").alias("source"),
            F.col(wb_year_col).alias("period"),
            F.col(wb_value_col).cast("double").alias("value_numeric"),
        )
        .withColumn("year", F.col("period").cast("int"))
        .withColumn("month", F.lit(1))
        .withColumn("date",
            F.to_date(F.concat_ws("-", F.col("year"), F.lit("01"), F.lit("01"))))
        .filter(F.col("value_numeric").isNotNull())
        .filter(F.col("year").isNotNull())
        .dropDuplicates(["indicator_id", "date"])
    )
else:
    print("WARNING: WorldBank container is empty - bronze may not have ingested data yet")
    wb_schema = StructType([
        StructField("indicator_id", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source", StringType(), True),
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("value_numeric", DoubleType(), True),
    ])
    df_wb_clean = spark.createDataFrame([], wb_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unificar y escribir a Silver (Delta Lake)

# COMMAND ----------

# Unify schema
common_columns = [
    "indicator_id", "indicator_name", "category", "source",
    "date", "year", "month", "value_numeric",
]

df_silver = (df_bcrp_clean.select(*common_columns)
    .unionByName(df_wb_clean.select(*common_columns))
    .withColumn("processed_timestamp", F.current_timestamp())
)

total_rows = df_silver.count()
print(f"Silver unified DataFrame: {total_rows} rows")

# COMMAND ----------

# Write to Silver layer - Unity Catalog external Delta table
silver_table = f"{catalog_name}.silver.economic_indicators"

if total_rows == 0:
    print("WARNING: No data to write to silver layer. Both bronze sources are empty.")
    print("This is expected on first run if bronze ingestion is still in progress.")
else:
    # Table pre-created in setup with LOCATION pointing to ADLS
    (df_silver.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .insertInto(silver_table, overwrite=True))
    print(f"Silver write completed: {total_rows} records to {silver_table}")

    # Summary
    display(
        spark.table(silver_table)
        .groupBy("source", "category")
        .agg(
            F.count("*").alias("records"),
            F.min("date").alias("min_date"),
            F.max("date").alias("max_date"),
        )
        .orderBy("source", "category")
    )
