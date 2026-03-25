# =============================================================================
# Outputs
# =============================================================================

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "keyvault_uri" {
  value = module.keyvault.keyvault_uri
}

output "storage_account_name" {
  value = module.storage.storage_account_name
}

output "cosmos_endpoint" {
  value = module.cosmosdb.endpoint
}

output "sql_server_fqdn" {
  value = module.sql_server.server_fqdn
}

output "databricks_workspace_url" {
  value = module.databricks.workspace_url
}

output "data_factory_name" {
  value = module.data_factory.name
}
