# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingesta BCRP (Banco Central de Reserva del Peru)
# MAGIC Ingesta de indicadores economicos desde la API publica del BCRP.
# MAGIC Datos crudos almacenados en CosmosDB sin transformacion.

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# Storage Account config
storage_account_name = dbutils.secrets.get(scope="keyvault-scope", key="storage-account-name")
storage_account_key = dbutils.secrets.get(scope="keyvault-scope", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

def get_datalake_path(container, path):
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"

# COMMAND ----------

# Configuracion
BCRP_BASE_URL = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api"

# Monthly series (PM/MM suffix) use YYYY-M format
# Daily series (PD suffix) use YYYY-M-D format
MONTHLY_START = "2020-1"
MONTHLY_END = datetime.now().strftime("%Y-%m")
DAILY_START = "2020-1-1"
DAILY_END = datetime.now().strftime("%Y-%m-%d")

# Series activas y verificadas (se evitan series descontinuadas)
SERIES = {
    "PN38705PM": {"name": "PBI (var % interanual)", "category": "produccion", "freq": "monthly"},
    "PN01270PM": {"name": "IPC Lima Metropolitana (var % mensual)", "category": "precios", "freq": "monthly"},
    "PN00015MM": {"name": "Tasa de Referencia BCRP", "category": "tasas", "freq": "monthly"},
    "PN01207PM": {"name": "Balanza Comercial (mill US$)", "category": "comercio_exterior", "freq": "monthly"},
    "PN00271PM": {"name": "Reservas Internacionales (mill US$)", "category": "reservas", "freq": "monthly"},
    "PD04638PD": {"name": "Tipo de Cambio Venta", "category": "tipo_cambio", "freq": "daily"},
    "PD04639PD": {"name": "Tipo de Cambio Compra", "category": "tipo_cambio", "freq": "daily"},
}

# COMMAND ----------

# Cosmos DB config from Key Vault
cosmos_endpoint = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-endpoint")
cosmos_key = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-primary-key")

cosmos_config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "bronze-db",
    "spark.cosmos.container": "bcrp_indicators",
    "spark.cosmos.write.strategy": "ItemOverwrite",
}

# COMMAND ----------

def fetch_bcrp_series(series_id, start_period, end_period):
    """Fetch data from BCRP API for a given series."""
    url = f"{BCRP_BASE_URL}/{series_id}/json/{start_period}/{end_period}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

# Ingest all series
all_records = []
ingestion_timestamp = datetime.utcnow().isoformat()

for series_id, metadata in SERIES.items():
    freq = metadata.get("freq", "monthly")
    start_p = DAILY_START if freq == "daily" else MONTHLY_START
    end_p = DAILY_END if freq == "daily" else MONTHLY_END
    print(f"Fetching: {metadata['name']} ({series_id}) [{freq}: {start_p} -> {end_p}]")
    try:
        data = fetch_bcrp_series(series_id, start_p, end_p)

        if "periods" in data:
            for period in data["periods"]:
                for value_entry in period.get("values", []):
                    record = {
                        "id": f"{series_id}_{period['name']}",
                        "series_id": series_id,
                        "series_name": metadata["name"],
                        "category": metadata["category"],
                        "period": period["name"],
                        "value": value_entry.get("value"),
                        "source": "BCRP",
                        "ingestion_timestamp": ingestion_timestamp,
                        "raw_response": json.dumps(period),
                    }
                    all_records.append(record)

        print(f"  -> {len([r for r in all_records if r['series_id'] == series_id])} records")
    except Exception as e:
        print(f"  ERROR fetching {series_id}: {e}")

# Filter out records with null/empty values
all_records = [r for r in all_records if r.get("value") and r["value"].strip() not in ("", "n.d.", "N.D.")]
print(f"\nTotal records (after filtering): {len(all_records)}")

# COMMAND ----------

# Create DataFrame and write to CosmosDB
if all_records:
    df_bronze = spark.createDataFrame(all_records)

    df_bronze = df_bronze.withColumn("_partition_key", F.col("category"))

    (df_bronze.write
        .format("cosmos.oltp")
        .options(**cosmos_config)
        .mode("append")
        .save())

    print(f"Written {df_bronze.count()} records to CosmosDB bronze layer")

    # Backup to Data Lake (non-blocking if external storage not available)
    try:
        output_path = get_datalake_path("bronze", f"bcrp/{datetime.now().strftime('%Y/%m/%d')}")
        df_bronze.write.mode("overwrite").parquet(output_path)
        print(f"Backup saved to: {output_path}")
    except Exception as e:
        print(f"Data Lake backup skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validacion
# MAGIC Verificar que los datos se ingresaron correctamente.

# COMMAND ----------

try:
    df_check = (spark.read
        .format("cosmos.oltp")
        .options(**cosmos_config)
        .load())

    display(
        df_check.groupBy("category", "series_name")
        .agg(F.count("*").alias("records"), F.max("period").alias("latest_period"))
        .orderBy("category")
    )
except Exception as e:
    print(f"Validation read skipped (non-blocking): {e}")
