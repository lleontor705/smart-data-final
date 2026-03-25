# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Tablas de Hechos
# MAGIC Genera las fact tables desde Silver (Unity Catalog).
# MAGIC Escribe a Unity Catalog (gold schema) y a Azure SQL para Power BI.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Parametros del ambiente
dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"
print(f"Using catalog: {catalog_name}")

# SQL Server connection - parse ADO.NET string from Key Vault into JDBC
ado_conn = dbutils.secrets.get(scope="keyvault-scope", key="sql-connection-string")
conn_parts = dict(p.split("=", 1) for p in ado_conn.split(";") if "=" in p)
sql_host = conn_parts.get("Server", "").replace("tcp:", "").replace(",", ":")
sql_db = conn_parts.get("Database", "")
sql_user = conn_parts.get("User ID", "")
sql_password = dbutils.secrets.get(scope="keyvault-scope", key="sql-admin-password")

jdbc_url = f"jdbc:sqlserver://{sql_host};database={sql_db};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
jdbc_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user": sql_user,
    "password": sql_password,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_economic_indicators

# COMMAND ----------

# Read from Silver (Unity Catalog)
df_econ = spark.table(f"{catalog_name}.silver.economic_indicators")

df_fact_econ = (df_econ
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .select(
        "date_key",
        F.col("indicator_id").alias("indicator_key"),
        "source",
        "category",
        "value_numeric",
        "year",
        "month",
    )
    # Calculate YoY change
    .withColumn("prev_year_value",
        F.lag("value_numeric", 12).over(
            Window.partitionBy("indicator_key").orderBy("date_key")))
    .withColumn("yoy_change",
        F.when(F.col("prev_year_value").isNotNull() & (F.col("prev_year_value") != 0),
               ((F.col("value_numeric") - F.col("prev_year_value")) / F.abs(F.col("prev_year_value")) * 100))
         .otherwise(None))
    .withColumn("load_timestamp", F.current_timestamp())
)

# Write to Unity Catalog gold schema
fact_econ_table = f"{catalog_name}.gold.fact_economic_indicators"
(df_fact_econ.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .insertInto(fact_econ_table, overwrite=True))

# Write to SQL Server for Power BI
df_fact_econ.write.jdbc(url=jdbc_url, table="fact_economic_indicators", mode="overwrite", properties=jdbc_properties)
print(f"fact_economic_indicators: {df_fact_econ.count()} rows -> {fact_econ_table} + SQL Server")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_trade_yearly

# COMMAND ----------

df_trade = spark.table(f"{catalog_name}.silver.trade_data")

df_fact_trade = (df_trade
    .withColumn("date_key",
        F.concat(F.col("year"), F.lit("0101")).cast("int"))
    .select(
        "date_key",
        F.col("partner_code").alias("partner_key"),
        "flow_code",
        "flow_desc",
        "cmd_code",
        "cmd_desc",
        "trade_value",
        "trade_value_millions",
        "net_weight",
        "year",
    )
    # YoY trade change per partner and flow
    .withColumn("prev_year_value",
        F.lag("trade_value").over(
            Window.partitionBy("partner_key", "flow_code", "cmd_code").orderBy("year")))
    .withColumn("yoy_change_pct",
        F.when(F.col("prev_year_value").isNotNull() & (F.col("prev_year_value") != 0),
               F.round(((F.col("trade_value") - F.col("prev_year_value")) / F.col("prev_year_value") * 100), 2))
         .otherwise(None))
    .withColumn("load_timestamp", F.current_timestamp())
)

# Write to Unity Catalog gold schema
fact_trade_table = f"{catalog_name}.gold.fact_trade_yearly"
(df_fact_trade.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .insertInto(fact_trade_table, overwrite=True))

# Write to SQL Server for Power BI
df_fact_trade.write.jdbc(url=jdbc_url, table="fact_trade_yearly", mode="overwrite", properties=jdbc_properties)
print(f"fact_trade_yearly: {df_fact_trade.count()} rows -> {fact_trade_table} + SQL Server")
