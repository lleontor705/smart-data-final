resource "azurerm_cosmosdb_account" "main" {
  name                = "cosmos-smartdata-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  consistency_policy {
    consistency_level = var.consistency_level
  }

  geo_location {
    location          = var.location
    failover_priority = 0
  }

  tags = var.tags
}

resource "azurerm_cosmosdb_sql_database" "bronze" {
  name                = "bronze-db"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  throughput          = var.throughput
}

# Collections for each data source
resource "azurerm_cosmosdb_sql_container" "bcrp" {
  name                = "bcrp_indicators"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  database_name       = azurerm_cosmosdb_sql_database.bronze.name
  partition_key_paths = ["/category"]

  indexing_policy {
    indexing_mode = "consistent"
    included_path { path = "/*" }
  }
}

resource "azurerm_cosmosdb_sql_container" "worldbank" {
  name                = "worldbank_indicators"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  database_name       = azurerm_cosmosdb_sql_database.bronze.name
  partition_key_paths = ["/category"]

  indexing_policy {
    indexing_mode = "consistent"
    included_path { path = "/*" }
  }
}

resource "azurerm_cosmosdb_sql_container" "comtrade" {
  name                = "comtrade_peru"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  database_name       = azurerm_cosmosdb_sql_database.bronze.name
  partition_key_paths = ["/year"]

  indexing_policy {
    indexing_mode = "consistent"
    included_path { path = "/*" }
  }
}

# Store connection string in Key Vault
resource "azurerm_key_vault_secret" "cosmos_connection" {
  name         = "cosmos-connection-string"
  value        = azurerm_cosmosdb_account.main.primary_sql_connection_string
  key_vault_id = var.keyvault_id
}

output "endpoint" {
  value = azurerm_cosmosdb_account.main.endpoint
}

output "account_name" {
  value = azurerm_cosmosdb_account.main.name
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "environment" { type = string }
variable "throughput" { type = number }
variable "consistency_level" { type = string }
variable "keyvault_id" { type = string }
variable "tags" { type = map(string) }
