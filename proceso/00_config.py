# Databricks notebook source
# MAGIC %md
# MAGIC # Configuracion comun para todos los notebooks
# MAGIC Configura acceso a Storage Account y variables compartidas.

# COMMAND ----------

from pyspark.sql import SparkSession

# Storage Account config from secret scope
storage_account_name = dbutils.secrets.get(scope="keyvault-scope", key="storage-account-name")
storage_account_key = dbutils.secrets.get(scope="keyvault-scope", key="storage-account-key")

# Configure Spark to access ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Base path for Data Lake
DATALAKE_BASE = f"abfss://{{container}}@{storage_account_name}.dfs.core.windows.net"

def get_datalake_path(container, path):
    """Get full ABFSS path for Data Lake access."""
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"

print(f"Storage configured: {storage_account_name}")
