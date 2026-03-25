# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingesta World Bank Peru
# MAGIC Indicadores de desarrollo del Banco Mundial para Peru.

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

WORLDBANK_BASE = "https://api.worldbank.org/v2"
COUNTRY = "PE"

INDICATORS = {
    "SP.POP.TOTL": {"name": "Poblacion Total", "category": "demografia"},
    "NY.GDP.MKTP.CD": {"name": "PBI (US$ corrientes)", "category": "economia"},
    "NY.GDP.PCAP.CD": {"name": "PBI per capita (US$)", "category": "economia"},
    "FP.CPI.TOTL.ZG": {"name": "Inflacion (% anual)", "category": "precios"},
    "SI.POV.NAHC": {"name": "Tasa de Pobreza Nacional (%)", "category": "social"},
    "SE.ADT.LITR.ZS": {"name": "Tasa de Alfabetizacion (%)", "category": "educacion"},
    "SH.XPD.CHEX.PC.CD": {"name": "Gasto Salud per capita (US$)", "category": "salud"},
    "SL.UEM.TOTL.ZS": {"name": "Tasa de Desempleo (%)", "category": "empleo"},
    "BX.KLT.DINV.CD.WD": {"name": "Inversion Extranjera Directa (US$)", "category": "inversion"},
    "NE.EXP.GNFS.ZS": {"name": "Exportaciones (% PBI)", "category": "comercio"},
}

# COMMAND ----------

cosmos_endpoint = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-endpoint")
cosmos_key = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-primary-key")

cosmos_config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "bronze-db",
    "spark.cosmos.container": "worldbank_indicators",
    "spark.cosmos.write.strategy": "ItemOverwrite",
}

# COMMAND ----------

def fetch_worldbank_indicator(indicator_id, country="PE"):
    """Fetch indicator data from World Bank API."""
    url = f"{WORLDBANK_BASE}/country/{country}/indicator/{indicator_id}"
    params = {"format": "json", "per_page": 100, "date": "2000:2025"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    result = response.json()
    # World Bank API returns [metadata, data]
    if len(result) > 1:
        return result[1]
    return []

# COMMAND ----------

all_records = []
ingestion_timestamp = datetime.utcnow().isoformat()

for indicator_id, metadata in INDICATORS.items():
    print(f"Fetching: {metadata['name']} ({indicator_id})")
    try:
        data = fetch_worldbank_indicator(indicator_id)
        if data:
            for entry in data:
                if entry.get("value") is not None:
                    record = {
                        "id": f"{indicator_id}_{entry['date']}",
                        "indicator_id": indicator_id,
                        "indicator_name": metadata["name"],
                        "category": metadata["category"],
                        "country": entry.get("country", {}).get("value", "Peru"),
                        "country_code": COUNTRY,
                        "year": str(entry["date"]),
                        "value": str(entry["value"]) if entry.get("value") is not None else None,
                        "unit": entry.get("unit", ""),
                        "source": "WorldBank",
                        "ingestion_timestamp": ingestion_timestamp,
                        "raw_response": json.dumps(entry),
                    }
                    all_records.append(record)

        print(f"  -> {len([r for r in all_records if r['indicator_id'] == indicator_id])} records")
    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\nTotal records: {len(all_records)}")

# COMMAND ----------

all_records = [r for r in all_records if r.get("value") is not None]
print(f"Records after filtering nulls: {len(all_records)}")

if all_records:
    df_bronze = spark.createDataFrame(all_records)

    (df_bronze.write
        .format("cosmos.oltp")
        .options(**cosmos_config)
        .mode("append")
        .save())

    print(f"Written {len(all_records)} records to CosmosDB")

    # Backup to Data Lake (non-blocking if external storage not available)
    try:
        output_path = get_datalake_path("bronze", f"worldbank/{datetime.now().strftime('%Y/%m/%d')}")
        df_bronze.write.mode("overwrite").parquet(output_path)
        print(f"Backup saved to: {output_path}")
    except Exception as e:
        print(f"Data Lake backup skipped: {e}")
