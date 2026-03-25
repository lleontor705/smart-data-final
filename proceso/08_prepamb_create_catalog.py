# Databricks notebook source
# MAGIC %md
# MAGIC # Preparacion de Ambiente - Unity Catalog
# MAGIC Crea el catalogo, schemas y tablas externas para la arquitectura medallion.
# MAGIC Basado en los patrones de cicd-proyectoFinal y CICDSmartDataDatabricks.

# COMMAND ----------

# Parametros via widgets (pasados por el workflow)
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("storage_account_name", "")

environment = dbutils.widgets.get("environment")

# Si no viene por widget, leer de Key Vault
if not dbutils.widgets.get("storage_account_name"):
    storage_account_name = dbutils.secrets.get(scope="keyvault-scope", key="storage-account-name")
else:
    storage_account_name = dbutils.widgets.get("storage_account_name")

catalog_name = f"catalog_smartdata_{environment}"
credential_name = f"credential-smartdata-{environment}"

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Storage Account: {storage_account_name}")
print(f"Credential: {credential_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear Catalogo y Schemas

# COMMAND ----------

sql_statements = [
    f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT 'Catalogo medallion para Smart Data - {environment}'",
    f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze COMMENT 'Capa Bronze - datos crudos desde APIs'",
    f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver COMMENT 'Capa Silver - datos limpios y estandarizados'",
    f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold COMMENT 'Capa Gold - tablas de hechos y dimensiones'",
]

for stmt in sql_statements:
    print(f"Ejecutando: {stmt.strip()}")
    spark.sql(stmt)

print("\nCatalogo y schemas creados correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear External Locations (si no existen)

# COMMAND ----------

# External locations se crean via API antes del workflow
# Aqui solo verificamos que existan
try:
    for container in ["bronze", "silver", "gold"]:
        loc_name = f"exlt_{container}_{environment}"
        result = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{loc_name}`")
        print(f"  External location '{loc_name}': OK")
except Exception as e:
    print(f"External locations check: {e}")
    print("External locations should be created via API before running the pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear tablas Silver (external Delta tables)

# COMMAND ----------

# Silver: economic_indicators
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.economic_indicators (
    indicator_id STRING,
    indicator_name STRING,
    category STRING,
    source STRING,
    date DATE,
    year INT,
    month INT,
    value_numeric DOUBLE,
    processed_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/{catalog_name}/economic_indicators'
""")
print(f"Table {catalog_name}.silver.economic_indicators created/verified")

# Silver: trade_data
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.trade_data (
    partner_code INT,
    partner STRING,
    flow_code STRING,
    flow_desc STRING,
    year INT,
    cmd_code STRING,
    cmd_desc STRING,
    trade_value DOUBLE,
    net_weight DOUBLE,
    source STRING,
    trade_value_millions DOUBLE,
    processed_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/{catalog_name}/trade_data'
""")
print(f"Table {catalog_name}.silver.trade_data created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear tablas Gold (external Delta tables)

# COMMAND ----------

# Gold: dim_date
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold.dim_date (
    full_date DATE,
    date_key INT,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    week_of_year INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/{catalog_name}/dim_date'
""")
print(f"Table {catalog_name}.gold.dim_date created/verified")

# Gold: dim_indicator
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold.dim_indicator (
    indicator_key STRING,
    indicator_name STRING,
    category STRING,
    source STRING,
    unit STRING
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/{catalog_name}/dim_indicator'
""")
print(f"Table {catalog_name}.gold.dim_indicator created/verified")

# Gold: dim_trade_partner
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold.dim_trade_partner (
    partner_key INT,
    partner_name STRING,
    region STRING
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/{catalog_name}/dim_trade_partner'
""")
print(f"Table {catalog_name}.gold.dim_trade_partner created/verified")

# Gold: fact_economic_indicators
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold.fact_economic_indicators (
    date_key INT,
    indicator_key STRING,
    source STRING,
    category STRING,
    value_numeric DOUBLE,
    year INT,
    month INT,
    prev_year_value DOUBLE,
    yoy_change DOUBLE,
    load_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/{catalog_name}/fact_economic_indicators'
""")
print(f"Table {catalog_name}.gold.fact_economic_indicators created/verified")

# Gold: fact_trade_yearly
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold.fact_trade_yearly (
    date_key INT,
    partner_key INT,
    flow_code STRING,
    flow_desc STRING,
    cmd_code STRING,
    cmd_desc STRING,
    trade_value DOUBLE,
    trade_value_millions DOUBLE,
    net_weight DOUBLE,
    year INT,
    prev_year_value DOUBLE,
    yoy_change_pct DOUBLE,
    load_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/{catalog_name}/fact_trade_yearly'
""")
print(f"Table {catalog_name}.gold.fact_trade_yearly created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar estructura

# COMMAND ----------

schemas = ["bronze", "silver", "gold"]
for schema in schemas:
    tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}")
    count = tables.count()
    print(f"  {catalog_name}.{schema}: {count} tablas")
    if count > 0:
        display(tables)

print(f"\nCatalogo {catalog_name} listo para uso.")
