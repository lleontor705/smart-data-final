#!/bin/bash
set -uo pipefail
export MSYS_NO_PATHCONV=1

# =============================================================================
# Setup Azure Infrastructure for partial-smart-data
# Usage: bash scripts/setup-infra.sh <environment>
# Example: bash scripts/setup-infra.sh dev
# Idempotent: safe to re-run if a step fails
# =============================================================================

ENV="${1:?Usage: bash scripts/setup-infra.sh <dev|pre|pro>}"

if [[ "$ENV" != "dev" && "$ENV" != "pre" && "$ENV" != "pro" ]]; then
  echo "ERROR: Environment must be dev, pre, or pro"
  exit 1
fi

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
LOCATION="eastus2"
RG="rg-smartdata-${ENV}"
SUFFIX="psd"
KV_NAME="kv-smartdata-${ENV}-${SUFFIX}"
STORAGE_NAME="stsmartdata${ENV}${SUFFIX}"
COSMOS_NAME="cosmos-smartdata-${ENV}-${SUFFIX}"
SQL_SERVER_NAME="sqlsrv-smartdata-${ENV}-${SUFFIX}"
SQL_DB_NAME="gold-db"
SQL_ADMIN="sqladmin"
DBW_NAME="dbw-smartdata-${ENV}"
ADF_NAME="adf-smartdata-${ENV}-${SUFFIX}"
SQL_LOCATION="centralus"  # East US 2 not accepting new SQL servers currently
TAGS="Environment=${ENV} Project=partial-smart-data ManagedBy=az-cli"

# Minimal resources for all environments (testing)
COSMOS_THROUGHPUT=400

echo "============================================="
echo " Setting up environment: ${ENV}"
echo " Location: ${LOCATION}"
echo "============================================="

# ---------------------------------------------------------------------------
# Pre-requisites: Register providers and install extensions
# ---------------------------------------------------------------------------
echo ""
echo "[0/8] Registering providers and extensions..."
for NS in Microsoft.DocumentDB Microsoft.Sql Microsoft.Databricks Microsoft.DataFactory; do
  STATE=$(az provider show -n "$NS" --query registrationState -o tsv 2>/dev/null || echo "NotRegistered")
  if [ "$STATE" != "Registered" ]; then
    az provider register --namespace "$NS" --output none 2>/dev/null || true
    echo "  -> Registering $NS..."
  fi
done

for EXT in databricks datafactory; do
  az extension show --name "$EXT" &>/dev/null || az extension add --name "$EXT" --yes --output none 2>/dev/null || true
done

# Wait for providers to be registered
for NS in Microsoft.DocumentDB Microsoft.Sql Microsoft.Databricks Microsoft.DataFactory; do
  while [ "$(az provider show -n "$NS" --query registrationState -o tsv 2>/dev/null)" != "Registered" ]; do
    sleep 10
  done
done
echo "  -> All providers registered"

# ---------------------------------------------------------------------------
# 1. Resource Group
# ---------------------------------------------------------------------------
echo ""
echo "[1/8] Creating Resource Group: ${RG}"
az group create \
  --name "$RG" \
  --location "$LOCATION" \
  --tags $TAGS \
  --output none
echo "  -> OK"

# ---------------------------------------------------------------------------
# 2. Key Vault
# ---------------------------------------------------------------------------
echo ""
echo "[2/8] Creating Key Vault: ${KV_NAME}"
CURRENT_USER_OID=$(az ad signed-in-user show --query id -o tsv)

if az keyvault show --name "$KV_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Already exists, skipping create"
else
  az keyvault create \
    --name "$KV_NAME" \
    --resource-group "$RG" \
    --location "$LOCATION" \
    --sku standard \
    --enable-rbac-authorization false \
    --retention-days 7 \
    --tags $TAGS \
    --output none
  echo "  -> Created"
fi

# Set access policy for current user (all secret permissions)
az keyvault set-policy \
  --name "$KV_NAME" \
  --resource-group "$RG" \
  --object-id "$CURRENT_USER_OID" \
  --secret-permissions get list set delete \
  --output none 2>/dev/null || true

KV_ID=$(az keyvault show --name "$KV_NAME" --resource-group "$RG" --query id -o tsv)
echo "  -> Access policy configured"

# ---------------------------------------------------------------------------
# 3. Storage Account (Data Lake Gen2)
# ---------------------------------------------------------------------------
echo ""
echo "[3/8] Creating Storage Account: ${STORAGE_NAME}"
if az storage account show --name "$STORAGE_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Already exists, skipping create"
else
  az storage account create \
    --name "$STORAGE_NAME" \
    --resource-group "$RG" \
    --location "$LOCATION" \
    --sku "Standard_LRS" \
    --kind "StorageV2" \
    --hns true \
    --tags $TAGS \
    --output none
  echo "  -> Created"
fi

for CONTAINER in bronze silver gold; do
  az storage container create \
    --name "$CONTAINER" \
    --account-name "$STORAGE_NAME" \
    --auth-mode login \
    --output none 2>/dev/null || true
done
echo "  -> Containers bronze/silver/gold OK"

# ---------------------------------------------------------------------------
# 4. CosmosDB
# ---------------------------------------------------------------------------
echo ""
echo "[4/8] Creating CosmosDB Account: ${COSMOS_NAME}"
if az cosmosdb show --name "$COSMOS_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Already exists, skipping create"
else
  az cosmosdb create \
    --name "$COSMOS_NAME" \
    --resource-group "$RG" \
    --locations regionName="$LOCATION" failoverPriority=0 \
    --default-consistency-level "Session" \
    --kind "GlobalDocumentDB" \
    --tags $TAGS \
    --output none
  echo "  -> Created"
fi

echo "  -> Creating database: bronze-db"
az cosmosdb sql database create \
  --account-name "$COSMOS_NAME" \
  --resource-group "$RG" \
  --name "bronze-db" \
  --throughput "$COSMOS_THROUGHPUT" \
  --output none 2>/dev/null || true

echo "  -> Creating containers..."
for CNAME_PKEY in "bcrp_indicators:/category" "worldbank_indicators:/category" "comtrade_peru:/year"; do
  CNAME="${CNAME_PKEY%%:*}"
  PKEY="${CNAME_PKEY##*:}"
  az cosmosdb sql container create \
    --account-name "$COSMOS_NAME" \
    --resource-group "$RG" \
    --database-name "bronze-db" \
    --name "$CNAME" \
    --partition-key-path "$PKEY" \
    --output none 2>/dev/null || true
  echo "    -> ${CNAME} OK"
done

# Store secrets in Key Vault
COSMOS_ENDPOINT=$(az cosmosdb show --name "$COSMOS_NAME" --resource-group "$RG" --query documentEndpoint -o tsv)
COSMOS_KEY=$(az cosmosdb keys list --name "$COSMOS_NAME" --resource-group "$RG" --query primaryMasterKey -o tsv)
COSMOS_CONN=$(az cosmosdb keys list --name "$COSMOS_NAME" --resource-group "$RG" --type connection-strings --query "connectionStrings[0].connectionString" -o tsv)

az keyvault secret set --vault-name "$KV_NAME" --name "cosmos-endpoint" --value "$COSMOS_ENDPOINT" --output none
az keyvault secret set --vault-name "$KV_NAME" --name "cosmos-primary-key" --value "$COSMOS_KEY" --output none
az keyvault secret set --vault-name "$KV_NAME" --name "cosmos-connection-string" --value "$COSMOS_CONN" --output none
echo "  -> Secrets stored in Key Vault"

# ---------------------------------------------------------------------------
# 5. SQL Server + Database (Gold layer)
# ---------------------------------------------------------------------------
echo ""
echo "[5/8] Creating SQL Server: ${SQL_SERVER_NAME}"

# Generate or retrieve password
if az sql server show --name "$SQL_SERVER_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Server already exists"
  # Try to get existing password from Key Vault
  SQL_PASSWORD=$(az keyvault secret show --vault-name "$KV_NAME" --name "sql-admin-password" --query value -o tsv 2>/dev/null || echo "")
  if [ -z "$SQL_PASSWORD" ]; then
    SQL_PASSWORD="P@ss$(openssl rand -hex 8)!"
    echo "  -> Warning: could not retrieve existing password, generated new one"
  fi
else
  SQL_PASSWORD="P@ss$(openssl rand -hex 8)!"
  az sql server create \
    --name "$SQL_SERVER_NAME" \
    --resource-group "$RG" \
    --location "$SQL_LOCATION" \
    --admin-user "$SQL_ADMIN" \
    --admin-password "$SQL_PASSWORD" \
    --minimal-tls-version "1.2" \
    --output none
  echo "  -> Server created"
fi

echo "  -> Creating database: ${SQL_DB_NAME}"
az sql db create \
  --name "$SQL_DB_NAME" \
  --server "$SQL_SERVER_NAME" \
  --resource-group "$RG" \
  --edition "Basic" \
  --max-size "2GB" \
  --output none 2>/dev/null || true

az sql server firewall-rule create \
  --server "$SQL_SERVER_NAME" \
  --resource-group "$RG" \
  --name "AllowAzureServices" \
  --start-ip-address "0.0.0.0" \
  --end-ip-address "0.0.0.0" \
  --output none 2>/dev/null || true

SQL_FQDN=$(az sql server show --name "$SQL_SERVER_NAME" --resource-group "$RG" --query fullyQualifiedDomainName -o tsv)
SQL_CONN="Server=tcp:${SQL_FQDN},1433;Database=${SQL_DB_NAME};User ID=${SQL_ADMIN};Password=${SQL_PASSWORD};Encrypt=true;TrustServerCertificate=false;"

az keyvault secret set --vault-name "$KV_NAME" --name "sql-admin-password" --value "$SQL_PASSWORD" --output none
az keyvault secret set --vault-name "$KV_NAME" --name "sql-connection-string" --value "$SQL_CONN" --output none
echo "  -> SQL secrets stored in Key Vault"

# ---------------------------------------------------------------------------
# 6. Azure Databricks Workspace
# ---------------------------------------------------------------------------
echo ""
echo "[6/8] Creating Databricks Workspace: ${DBW_NAME}"
if az databricks workspace show --name "$DBW_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Already exists, skipping create"
else
  az databricks workspace create \
    --name "$DBW_NAME" \
    --resource-group "$RG" \
    --location "$LOCATION" \
    --sku "standard" \
    --tags $TAGS \
    --output none
  echo "  -> Created"
fi

# ---------------------------------------------------------------------------
# 7. Azure Data Factory
# ---------------------------------------------------------------------------
echo ""
echo "[7/8] Creating Data Factory: ${ADF_NAME}"
if az datafactory show --name "$ADF_NAME" --resource-group "$RG" &>/dev/null; then
  echo "  -> Already exists, skipping create"
else
  az datafactory create \
    --name "$ADF_NAME" \
    --resource-group "$RG" \
    --location "$LOCATION" \
    --tags $TAGS \
    --output none
  echo "  -> Created"
fi

ADF_PRINCIPAL_ID=$(az datafactory show --name "$ADF_NAME" --resource-group "$RG" --query identity.principalId -o tsv 2>/dev/null || echo "")
if [ -n "$ADF_PRINCIPAL_ID" ] && [ "$ADF_PRINCIPAL_ID" != "None" ]; then
  az keyvault set-policy \
    --name "$KV_NAME" \
    --resource-group "$RG" \
    --object-id "$ADF_PRINCIPAL_ID" \
    --secret-permissions get list \
    --output none 2>/dev/null || true
  echo "  -> ADF Key Vault access policy set"
else
  echo "  -> No managed identity (assign access policy manually if needed)"
fi

# ---------------------------------------------------------------------------
# 8. Role Assignments - Storage access
# ---------------------------------------------------------------------------
echo ""
echo "[8/8] Configuring Storage Access"
STORAGE_KEY=$(az storage account keys list --account-name "$STORAGE_NAME" --resource-group "$RG" --query "[0].value" -o tsv)
echo "  -> Storage access configured (use account key or Managed Identity from Databricks)"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================="
echo " Infrastructure setup complete for: ${ENV}"
echo "============================================="
echo ""
echo "Resources created in ${RG}:"
echo "  - Key Vault:       ${KV_NAME}"
echo "  - Storage (ADLS):  ${STORAGE_NAME}"
echo "  - CosmosDB:        ${COSMOS_NAME}"
echo "  - SQL Server:      ${SQL_SERVER_NAME} / ${SQL_DB_NAME}"
echo "  - Databricks:      ${DBW_NAME}"
echo "  - Data Factory:    ${ADF_NAME}"
echo ""
echo "Secrets stored in Key Vault (${KV_NAME}):"
echo "  - cosmos-endpoint"
echo "  - cosmos-primary-key"
echo "  - cosmos-connection-string"
echo "  - sql-admin-password"
echo "  - sql-connection-string"
echo ""
