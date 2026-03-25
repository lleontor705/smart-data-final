# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingesta UN Comtrade Peru
# MAGIC Exportaciones e importaciones de Peru por socio comercial.
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
COMTRADE_BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
REPORTER_CODE = 604  # Peru

PARTNERS = {
    156: "China",
    842: "Estados Unidos",
    76: "Brasil",
    152: "Chile",
    170: "Colombia",
}

FLOW_CODES = {"M": "Importaciones", "X": "Exportaciones"}
YEARS = list(range(2015, datetime.now().year + 1))

# COMMAND ----------

cosmos_endpoint = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-endpoint")
cosmos_key = dbutils.secrets.get(scope="keyvault-scope", key="cosmos-primary-key")

cosmos_config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "bronze-db",
    "spark.cosmos.container": "comtrade_peru",
    "spark.cosmos.write.strategy": "ItemOverwrite",
}

# COMMAND ----------

def fetch_comtrade(reporter, partner, flow, year):
    """Fetch trade data from UN Comtrade API."""
    url = COMTRADE_BASE
    params = {
        "reporterCode": reporter,
        "partnerCode": partner,
        "flowCode": flow,
        "period": year,
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    result = response.json()
    return result.get("data", [])

# COMMAND ----------

all_records = []
ingestion_timestamp = datetime.utcnow().isoformat()

for year in YEARS:
    for flow_code, flow_name in FLOW_CODES.items():
        for partner_code, partner_name in PARTNERS.items():
            print(f"Fetching: {flow_name} - {partner_name} ({year})")
            try:
                data = fetch_comtrade(REPORTER_CODE, partner_code, flow_code, year)
                for entry in data:
                    record = {
                        "id": f"{REPORTER_CODE}_{partner_code}_{flow_code}_{year}_{entry.get('cmdCode', 'TOTAL')}",
                        "reporter_code": str(REPORTER_CODE),
                        "reporter": "Peru",
                        "partner_code": str(partner_code),
                        "partner": partner_name,
                        "flow_code": str(flow_code),
                        "flow_desc": flow_name,
                        "year": str(year),
                        "cmd_code": str(entry.get("cmdCode", "TOTAL")),
                        "cmd_desc": str(entry.get("cmdDescE", "Total")),
                        "trade_value": float(entry.get("primaryValue", 0) or 0),
                        "net_weight": float(entry.get("netWgt", 0) or 0),
                        "source": "UNComtrade",
                        "ingestion_timestamp": ingestion_timestamp,
                        "raw_response": json.dumps(entry),
                    }
                    all_records.append(record)
                print(f"  -> {len(data)} records")
            except Exception as e:
                print(f"  ERROR: {e}")

print(f"\nTotal records: {len(all_records)}")

# COMMAND ----------

if all_records:
    df_bronze = spark.createDataFrame(all_records)

    df_bronze = df_bronze.withColumn("_partition_key", F.col("year").cast("string"))

    (df_bronze.write
        .format("cosmos.oltp")
        .options(**cosmos_config)
        .mode("append")
        .save())

    print(f"Written {len(all_records)} records to CosmosDB")

    # Backup to Data Lake (non-blocking if external storage not available)
    try:
        output_path = get_datalake_path("bronze", f"comtrade/{datetime.now().strftime('%Y/%m/%d')}")
        df_bronze.write.mode("overwrite").parquet(output_path)
        print(f"Backup saved to: {output_path}")
    except Exception as e:
        print(f"Data Lake backup skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validacion

# COMMAND ----------

try:
    df_check = (spark.read
        .format("cosmos.oltp")
        .options(**cosmos_config)
        .load())

    display(
        df_check.groupBy("flow_desc", "partner")
        .agg(
            F.count("*").alias("records"),
            F.sum("trade_value").alias("total_value"),
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
        )
        .orderBy("flow_desc", "partner")
    )
except Exception as e:
    print(f"Validation read skipped (non-blocking): {e}")
