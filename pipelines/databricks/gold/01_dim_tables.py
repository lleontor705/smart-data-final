# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Tablas de Dimension
# MAGIC Crea las dimensiones del modelo estrella.
# MAGIC Escribe a Unity Catalog (gold schema) y a Azure SQL para Power BI.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# COMMAND ----------

# Parametros del ambiente
dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"
print(f"Using catalog: {catalog_name}")

# SQL Server connection from Key Vault
jdbc_url = dbutils.secrets.get(scope="keyvault-scope", key="sql-connection-string")
jdbc_properties = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date - Dimension Temporal

# COMMAND ----------

# Generate date dimension (2015-2026)
start = datetime(2015, 1, 1)
end = datetime(2026, 12, 31)
dates = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]

df_dates = spark.createDataFrame(dates, ["full_date"])

df_dim_date = (df_dates
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name",
        F.when(F.month("full_date") == 1, "Enero")
         .when(F.month("full_date") == 2, "Febrero")
         .when(F.month("full_date") == 3, "Marzo")
         .when(F.month("full_date") == 4, "Abril")
         .when(F.month("full_date") == 5, "Mayo")
         .when(F.month("full_date") == 6, "Junio")
         .when(F.month("full_date") == 7, "Julio")
         .when(F.month("full_date") == 8, "Agosto")
         .when(F.month("full_date") == 9, "Septiembre")
         .when(F.month("full_date") == 10, "Octubre")
         .when(F.month("full_date") == 11, "Noviembre")
         .when(F.month("full_date") == 12, "Diciembre"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name",
        F.when(F.dayofweek("full_date") == 1, "Domingo")
         .when(F.dayofweek("full_date") == 2, "Lunes")
         .when(F.dayofweek("full_date") == 3, "Martes")
         .when(F.dayofweek("full_date") == 4, "Miercoles")
         .when(F.dayofweek("full_date") == 5, "Jueves")
         .when(F.dayofweek("full_date") == 6, "Viernes")
         .when(F.dayofweek("full_date") == 7, "Sabado"))
    .withColumn("is_weekend",
        F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False))
)

# Write to Unity Catalog gold schema
dim_date_table = f"{catalog_name}.gold.dim_date"
(df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .insertInto(dim_date_table, overwrite=True))

# Write to SQL Server for Power BI
df_dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=jdbc_properties)
print(f"dim_date: {df_dim_date.count()} rows -> {dim_date_table} + SQL Server")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_indicator - Dimension Indicadores Economicos

# COMMAND ----------

indicators = [
    ("PN01288PM", "PBI Global (var % 12 meses)", "produccion", "BCRP", "%"),
    ("PN01270PM", "IPC Lima (var % mensual)", "precios", "BCRP", "%"),
    ("PD04638PD", "Tipo de Cambio Venta", "tipo_cambio", "BCRP", "S/ por US$"),
    ("PD04639PD", "Tipo de Cambio Compra", "tipo_cambio", "BCRP", "S/ por US$"),
    ("PN00015MM", "Tasa de Referencia BCRP", "tasas", "BCRP", "%"),
    ("PN01207PM", "Balanza Comercial", "comercio_exterior", "BCRP", "Millones US$"),
    ("SP.POP.TOTL", "Poblacion Total", "demografia", "WorldBank", "Personas"),
    ("NY.GDP.MKTP.CD", "PBI (US$ corrientes)", "economia", "WorldBank", "US$"),
    ("NY.GDP.PCAP.CD", "PBI per capita", "economia", "WorldBank", "US$"),
    ("FP.CPI.TOTL.ZG", "Inflacion (% anual)", "precios", "WorldBank", "%"),
    ("SL.UEM.TOTL.ZS", "Tasa de Desempleo", "empleo", "WorldBank", "%"),
]

df_dim_indicator = spark.createDataFrame(
    indicators,
    ["indicator_key", "indicator_name", "category", "source", "unit"]
)

dim_indicator_table = f"{catalog_name}.gold.dim_indicator"
(df_dim_indicator.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .insertInto(dim_indicator_table, overwrite=True))

df_dim_indicator.write.jdbc(url=jdbc_url, table="dim_indicator", mode="overwrite", properties=jdbc_properties)
print(f"dim_indicator: {df_dim_indicator.count()} rows -> {dim_indicator_table} + SQL Server")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_trade_partner - Dimension Socios Comerciales

# COMMAND ----------

partners = [
    (156, "China", "Asia"),
    (842, "Estados Unidos", "America del Norte"),
    (76, "Brasil", "America del Sur"),
    (152, "Chile", "America del Sur"),
    (170, "Colombia", "America del Sur"),
]

df_dim_partner = spark.createDataFrame(
    partners,
    ["partner_key", "partner_name", "region"]
)

dim_partner_table = f"{catalog_name}.gold.dim_trade_partner"
(df_dim_partner.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .insertInto(dim_partner_table, overwrite=True))

df_dim_partner.write.jdbc(url=jdbc_url, table="dim_trade_partner", mode="overwrite", properties=jdbc_properties)
print(f"dim_trade_partner: {df_dim_partner.count()} rows -> {dim_partner_table} + SQL Server")
