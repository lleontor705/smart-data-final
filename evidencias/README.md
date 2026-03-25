# Evidencias de Ejecucion

Colocar aqui capturas de pantalla (.png) que evidencien:

## 1. Ejecucion de Workflow correcta en Databricks
- Screenshot del workflow DAG con todos los tasks en verde
- Detalle de cada task (setup_catalog, bronze_*, silver_*, gold_*, security_grants)

## 2. Ejecucion de GitHub Actions correcta
- Screenshot del workflow "Deploy Databricks Notebooks" exitoso
- Screenshot del workflow "Deploy Infrastructure" exitoso
- Historial de runs mostrando CI/CD funcionando

## 3. Servicios aprovisionados en Azure
- Screenshot del Resource Group con todos los recursos
- Azure Databricks workspace (Premium tier)
- Azure CosmosDB (bronze-db con 3 containers)
- Azure SQL Database (gold layer)
- Azure Key Vault (secretos)
- Azure Data Lake Storage Gen2 (bronze/silver/gold containers)
- Unity Catalog (catalog, schemas, external locations)

## 4. Seguridad
- Screenshot de grupos en Microsoft Entra ID
- Screenshot de Databricks Identity (grupos sincronizados)
- Screenshot de SHOW GRANTS en Unity Catalog
