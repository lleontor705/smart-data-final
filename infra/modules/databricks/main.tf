resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-smartdata-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.databricks_sku

  custom_parameters {
    no_public_ip = var.environment == "pro" ? true : false
  }

  tags = var.tags
}

# Configure Databricks provider with workspace
provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
}

# Cluster for ETL processing
resource "databricks_cluster" "etl" {
  cluster_name            = "etl-${var.environment}"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = var.node_type
  autotermination_minutes = var.environment == "pro" ? 60 : 30

  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }

  library {
    pypi {
      package = "azure-cosmos==4.5.1"
    }
  }

  library {
    pypi {
      package = "pyodbc==5.0.1"
    }
  }
}

# Secret scope linked to Key Vault
resource "databricks_secret_scope" "keyvault" {
  name = "keyvault-scope"

  keyvault_metadata {
    resource_id = var.keyvault_id
    dns_name    = "https://kv-smartdata-${var.environment}.vault.azure.net/"
  }
}

output "workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "environment" { type = string }
variable "databricks_sku" { type = string }
variable "node_type" { type = string }
variable "min_workers" { type = number }
variable "max_workers" { type = number }
variable "storage_account_id" { type = string }
variable "keyvault_id" { type = string }
variable "tags" { type = map(string) }
