# Databricks notebook source
# MAGIC %md
# MAGIC # Reversion - Eliminar tablas y estructuras Unity Catalog
# MAGIC Ejecutar para limpiar completamente el ambiente.
# MAGIC **PRECAUCION**: Esta operacion es irreversible.

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"

print(f"REVERSION para: {catalog_name}")
print(f"ATENCION: Se eliminaran TODAS las tablas y schemas del catalogo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drop tablas Gold

# COMMAND ----------

gold_tables = ["fact_trade_yearly", "fact_economic_indicators",
               "dim_trade_partner", "dim_indicator", "dim_date"]

for table in gold_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.gold.{table}")
        print(f"  Dropped: {catalog_name}.gold.{table}")
    except Exception as e:
        print(f"  Skip: {table} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop tablas Silver

# COMMAND ----------

silver_tables = ["economic_indicators", "trade_data"]

for table in silver_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.silver.{table}")
        print(f"  Dropped: {catalog_name}.silver.{table}")
    except Exception as e:
        print(f"  Skip: {table} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drop schemas y catalogo

# COMMAND ----------

for schema in ["gold", "silver", "bronze"]:
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema} CASCADE")
        print(f"  Dropped schema: {catalog_name}.{schema}")
    except Exception as e:
        print(f"  Skip: {schema} - {e}")

try:
    spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
    print(f"  Dropped catalog: {catalog_name}")
except Exception as e:
    print(f"  Skip catalog: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drop external locations

# COMMAND ----------

for container in ["gold", "silver", "bronze"]:
    loc_name = f"exlt_{container}_{environment}"
    try:
        spark.sql(f"DROP EXTERNAL LOCATION IF EXISTS `{loc_name}`")
        print(f"  Dropped external location: {loc_name}")
    except Exception as e:
        print(f"  Skip: {loc_name} - {e}")

print(f"\nReversion completada para {catalog_name}.")
