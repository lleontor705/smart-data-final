# =============================================================================
# Partial Smart Data - Main Terraform Configuration
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }

  backend "azurerm" {
    # Configured per environment via -backend-config
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

# -----------------------------------------------------------------------------
# Resource Group
# -----------------------------------------------------------------------------
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# -----------------------------------------------------------------------------
# Modules
# -----------------------------------------------------------------------------

module "keyvault" {
  source              = "./modules/keyvault"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  environment         = var.environment
  tags                = var.tags
}

module "storage" {
  source                   = "./modules/storage"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  environment              = var.environment
  storage_replication_type = var.storage_replication_type
  tags                     = var.tags
}

module "cosmosdb" {
  source              = "./modules/cosmosdb"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  environment         = var.environment
  throughput          = var.cosmos_throughput
  consistency_level   = var.cosmos_consistency_level
  keyvault_id         = module.keyvault.keyvault_id
  tags                = var.tags
}

module "sql_server" {
  source              = "./modules/sql-server"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  environment         = var.environment
  sql_sku             = var.sql_sku
  sql_max_size_gb     = var.sql_max_size_gb
  keyvault_id         = module.keyvault.keyvault_id
  tags                = var.tags
}

module "databricks" {
  source              = "./modules/databricks"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  environment         = var.environment
  databricks_sku      = var.databricks_sku
  node_type           = var.databricks_node_type
  min_workers         = var.databricks_min_workers
  max_workers         = var.databricks_max_workers
  storage_account_id  = module.storage.storage_account_id
  keyvault_id         = module.keyvault.keyvault_id
  tags                = var.tags
}

module "data_factory" {
  source              = "./modules/data-factory"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  environment         = var.environment
  keyvault_id         = module.keyvault.keyvault_id
  databricks_workspace_url = module.databricks.workspace_url
  tags                = var.tags
}
