# Databricks notebook source
# MAGIC %md
# MAGIC # Seguridad - Grants Unity Catalog
# MAGIC Configura permisos de acceso a los datos por grupo de usuarios.
# MAGIC
# MAGIC | Grupo | Descripcion | Permisos |
# MAGIC |-------|------------|----------|
# MAGIC | datareaders | Analistas de datos | SELECT en todas las capas |
# MAGIC | dataengineers | Ingenieros de datos | SELECT + MODIFY en todas las capas |
# MAGIC | dataadmins | Administradores | ALL PRIVILEGES |

# COMMAND ----------

# Parametros del ambiente
dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")
catalog_name = f"catalog_smartdata_{environment}"
print(f"Aplicando grants en: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Grants a nivel de Catalogo

# COMMAND ----------

catalog_grants = [
    # datareaders - solo lectura
    f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `datareaders`",
    # dataengineers - lectura y escritura
    f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `dataengineers`",
    # dataadmins - acceso total
    f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `dataadmins`",
]

for stmt in catalog_grants:
    print(f"  {stmt}")
    spark.sql(stmt)

print("Grants de catalogo aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Grants a nivel de Schema

# COMMAND ----------

schemas = ["bronze", "silver", "gold"]

for schema in schemas:
    schema_grants = [
        # datareaders - USE SCHEMA + SELECT
        f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema} TO `datareaders`",
        f"GRANT SELECT ON SCHEMA {catalog_name}.{schema} TO `datareaders`",
        # dataengineers - USE SCHEMA + SELECT + MODIFY
        f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema} TO `dataengineers`",
        f"GRANT SELECT ON SCHEMA {catalog_name}.{schema} TO `dataengineers`",
        f"GRANT MODIFY ON SCHEMA {catalog_name}.{schema} TO `dataengineers`",
        f"GRANT CREATE TABLE ON SCHEMA {catalog_name}.{schema} TO `dataengineers`",
    ]
    print(f"\n--- Schema: {catalog_name}.{schema} ---")
    for stmt in schema_grants:
        print(f"  {stmt}")
        spark.sql(stmt)

print("\nGrants de schemas aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grants en External Locations

# COMMAND ----------

for container in ["bronze", "silver", "gold"]:
    loc_name = f"exlt_{container}_{environment}"
    try:
        ext_loc_grants = [
            # datareaders - solo lectura de archivos
            f"GRANT READ FILES ON EXTERNAL LOCATION `{loc_name}` TO `datareaders`",
            # dataengineers - lectura y escritura
            f"GRANT READ FILES ON EXTERNAL LOCATION `{loc_name}` TO `dataengineers`",
            f"GRANT WRITE FILES ON EXTERNAL LOCATION `{loc_name}` TO `dataengineers`",
            # dataadmins - todo
            f"GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `{loc_name}` TO `dataadmins`",
        ]
        print(f"\n--- External Location: {loc_name} ---")
        for stmt in ext_loc_grants:
            print(f"  {stmt}")
            spark.sql(stmt)
    except Exception as e:
        print(f"  External location {loc_name} grants skipped: {e}")

print("\nGrants de external locations aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grants especificos en tablas Gold (para Power BI / Analistas)

# COMMAND ----------

gold_tables = [
    "dim_date",
    "dim_indicator",
    "dim_trade_partner",
    "fact_economic_indicators",
    "fact_trade_yearly",
]

print(f"\n--- Tablas Gold (acceso analistas) ---")
for table in gold_tables:
    full_table = f"{catalog_name}.gold.{table}"
    try:
        spark.sql(f"GRANT SELECT ON TABLE {full_table} TO `datareaders`")
        print(f"  SELECT granted on {full_table} -> datareaders")
    except Exception as e:
        print(f"  Grant on {full_table} skipped (table may not exist yet): {e}")

print("\nGrants de tablas Gold aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Grants aplicados

# COMMAND ----------

print("=== Grants en Catalogo ===")
display(spark.sql(f"SHOW GRANTS ON CATALOG {catalog_name}"))

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    print(f"\n=== Grants en Schema {schema} ===")
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog_name}.{schema}"))

# COMMAND ----------

# Verificar external locations
for container in ["bronze", "silver", "gold"]:
    loc_name = f"exlt_{container}_{environment}"
    try:
        print(f"\n=== Grants en External Location {loc_name} ===")
        display(spark.sql(f"SHOW GRANTS ON EXTERNAL LOCATION `{loc_name}`"))
    except Exception as e:
        print(f"  {loc_name}: {e}")

# COMMAND ----------

print(f"\nSeguridad configurada correctamente para {catalog_name}.")
print(f"Grupos: datareaders, dataengineers, dataadmins")
