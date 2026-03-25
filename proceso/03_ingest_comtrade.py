# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingesta UN Comtrade Peru
# MAGIC Exportaciones e importaciones de Peru por socio comercial.
# MAGIC Datos crudos almacenados en CosmosDB sin transformacion.

# COMMAND ----------

import requests
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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
YEARS = list(range(2018, 2024))  # 2018-2023 (datos consolidados disponibles)

# Batch all partner codes for single API call (comma-separated)
ALL_PARTNER_CODES = ",".join(str(c) for c in PARTNERS.keys())

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

import time as _time

def fetch_comtrade(reporter, partners_csv, flow, year, max_retries=3):
    """Fetch trade data from UN Comtrade API with retry on 429."""
    url = COMTRADE_BASE
    params = {
        "reporterCode": reporter,
        "partnerCode": partners_csv,
        "flowCode": flow,
        "period": year,
    }
    for attempt in range(max_retries):
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 429:
            wait = 2 ** attempt + 1  # 2s, 3s, 5s
            _time.sleep(wait)
            continue
        response.raise_for_status()
        result = response.json()
        return result.get("data", [])
    response.raise_for_status()
    return []

def fetch_task(flow_code, flow_name, year):
    """Single fetch task for ThreadPoolExecutor - batches all partners."""
    try:
        data = fetch_comtrade(REPORTER_CODE, ALL_PARTNER_CODES, flow_code, year)
        records = []
        for entry in data:
            partner_code = str(entry.get("partnerCode", ""))
            partner_name = PARTNERS.get(int(partner_code), partner_code) if partner_code.isdigit() else partner_code
            records.append({
                "id": f"{REPORTER_CODE}_{partner_code}_{flow_code}_{year}_{entry.get('cmdCode', 'TOTAL')}",
                "reporter_code": str(REPORTER_CODE),
                "reporter": "Peru",
                "partner_code": partner_code,
                "partner": str(partner_name),
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
            })
        return flow_name, year, records, None
    except Exception as e:
        return flow_name, year, [], str(e)

# COMMAND ----------

ingestion_timestamp = datetime.utcnow().isoformat()
all_records = []

# Parallel fetch: 12 calls (6 years x 2 flows) instead of 120 sequential
# max_workers=2 to respect Comtrade API rate limits (~1 req/sec)
tasks = []
with ThreadPoolExecutor(max_workers=2) as executor:
    for year in YEARS:
        for flow_code, flow_name in FLOW_CODES.items():
            tasks.append(executor.submit(fetch_task, flow_code, flow_name, year))
            _time.sleep(1.5)  # stagger submissions to avoid burst

    for future in as_completed(tasks):
        flow_name, year, records, error = future.result()
        if error:
            print(f"  ERROR {flow_name} {year}: {error}")
        else:
            all_records.extend(records)
            print(f"  {flow_name} {year}: {len(records)} records")

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
