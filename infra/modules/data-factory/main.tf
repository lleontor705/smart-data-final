resource "azurerm_data_factory" "main" {
  name                = "adf-smartdata-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Linked service to Databricks
resource "azurerm_data_factory_linked_service_azure_databricks" "etl" {
  name            = "ls-databricks-etl"
  data_factory_id = azurerm_data_factory.main.id
  adb_domain      = "https://${var.databricks_workspace_url}"

  existing_cluster_id = "etl-${var.environment}"

  key_vault_password {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.main.name
    secret_name         = "databricks-token"
  }
}

# Linked service to Key Vault
resource "azurerm_data_factory_linked_service_key_vault" "main" {
  name            = "ls-keyvault"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = var.keyvault_id
}

# Grant ADF access to Key Vault
resource "azurerm_role_assignment" "adf_keyvault" {
  scope                = var.keyvault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

output "name" {
  value = azurerm_data_factory.main.name
}

output "id" {
  value = azurerm_data_factory.main.id
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "environment" { type = string }
variable "keyvault_id" { type = string }
variable "databricks_workspace_url" { type = string }
variable "tags" { type = map(string) }
