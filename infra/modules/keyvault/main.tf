data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                        = "kv-smartdata-${var.environment}"
  location                    = var.location
  resource_group_name         = var.resource_group_name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  purge_protection_enabled    = var.environment == "pro" ? true : false
  enable_rbac_authorization   = true
  tags                        = var.tags
}

output "keyvault_id" {
  value = azurerm_key_vault.main.id
}

output "keyvault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "environment" { type = string }
variable "tags" { type = map(string) }
