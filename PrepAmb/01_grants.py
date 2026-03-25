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
# MAGIC ## 0. Verificar/Crear grupos en workspace

# COMMAND ----------

import requests as _requests

workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

groups_to_create = ["datareaders", "dataengineers", "dataadmins"]
existing_groups = []

for group_name in groups_to_create:
    try:
        # Check if group exists via SCIM API
        resp = _requests.get(
            f"https://{workspace_url}/api/2.0/preview/scim/v2/Groups",
            headers={"Authorization": f"Bearer {token}"},
            params={"filter": f'displayName eq "{group_name}"'},
            timeout=10,
        )
        data = resp.json()
        if data.get("totalResults", 0) > 0:
            print(f"  Group '{group_name}' exists (id={data['Resources'][0]['id']})")
            existing_groups.append(group_name)
        else:
            # Create group
            resp = _requests.post(
                f"https://{workspace_url}/api/2.0/preview/scim/v2/Groups",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"displayName": group_name, "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"]},
                timeout=10,
            )
            if resp.status_code in (200, 201):
                print(f"  Group '{group_name}' created")
                existing_groups.append(group_name)
            else:
                print(f"  Group '{group_name}' creation failed: {resp.text[:100]}")
    except Exception as e:
        print(f"  Group '{group_name}' check failed: {e}")

print(f"\nAvailable groups for grants: {existing_groups}")

# COMMAND ----------

def safe_grant(stmt):
    """Execute a GRANT statement, skipping if group/principal not found."""
    try:
        spark.sql(stmt)
        return True
    except Exception as e:
        err = str(e)
        if "PRINCIPAL_DOES_NOT_EXIST" in err or "does not exist" in err.lower():
            print(f"  SKIP (principal not found): {stmt[:80]}")
        else:
            print(f"  ERROR: {stmt[:80]} -> {err[:100]}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Grants a nivel de Catalogo

# COMMAND ----------

success = 0
total = 0
for group in existing_groups:
    stmts = [f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{group}`"]
    if group == "dataadmins":
        stmts.append(f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `{group}`")
    for stmt in stmts:
        total += 1
        if safe_grant(stmt):
            print(f"  OK: {stmt}")
            success += 1

print(f"\nCatalog grants: {success}/{total} applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Grants a nivel de Schema

# COMMAND ----------

schemas = ["bronze", "silver", "gold"]

for schema in schemas:
    print(f"\n--- Schema: {catalog_name}.{schema} ---")
    for group in existing_groups:
        grants = [
            f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema} TO `{group}`",
            f"GRANT SELECT ON SCHEMA {catalog_name}.{schema} TO `{group}`",
        ]
        if group in ("dataengineers", "dataadmins"):
            grants.extend([
                f"GRANT MODIFY ON SCHEMA {catalog_name}.{schema} TO `{group}`",
                f"GRANT CREATE TABLE ON SCHEMA {catalog_name}.{schema} TO `{group}`",
            ])
        for stmt in grants:
            if safe_grant(stmt):
                print(f"  OK: {stmt}")

print("\nSchema grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grants en External Locations

# COMMAND ----------

for container in ["bronze", "silver", "gold"]:
    loc_name = f"exlt_{container}_{environment}"
    print(f"\n--- External Location: {loc_name} ---")
    for group in existing_groups:
        grants = [f"GRANT READ FILES ON EXTERNAL LOCATION `{loc_name}` TO `{group}`"]
        if group in ("dataengineers", "dataadmins"):
            grants.append(f"GRANT WRITE FILES ON EXTERNAL LOCATION `{loc_name}` TO `{group}`")
        if group == "dataadmins":
            grants.append(f"GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `{loc_name}` TO `{group}`")
        for stmt in grants:
            if safe_grant(stmt):
                print(f"  OK: {stmt}")

print("\nExternal location grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grants especificos en tablas Gold (para Power BI / Analistas)

# COMMAND ----------

gold_tables = [
    "dim_date", "dim_indicator", "dim_trade_partner",
    "fact_economic_indicators", "fact_trade_yearly",
]

print(f"\n--- Tablas Gold (acceso analistas) ---")
for table in gold_tables:
    full_table = f"{catalog_name}.gold.{table}"
    for group in existing_groups:
        safe_grant(f"GRANT SELECT ON TABLE {full_table} TO `{group}`")
        print(f"  SELECT on {full_table} -> {group}")

print("\nGold table grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Grants aplicados

# COMMAND ----------

print("=== Grants en Catalogo ===")
try:
    display(spark.sql(f"SHOW GRANTS ON CATALOG {catalog_name}"))
except Exception as e:
    print(f"  Cannot show catalog grants: {e}")

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    print(f"\n=== Grants en Schema {schema} ===")
    try:
        display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog_name}.{schema}"))
    except Exception as e:
        print(f"  Cannot show schema grants: {e}")

# COMMAND ----------

print(f"\nSeguridad configurada para {catalog_name}.")
print(f"Grupos: {', '.join(existing_groups)}")
