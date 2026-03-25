# Environment: Development
environment         = "dev"
location            = "eastus2"
resource_group_name = "rg-smartdata-dev"

# Databricks
databricks_sku             = "premium"
databricks_node_type       = "Standard_DS3_v2"
databricks_min_workers     = 1
databricks_max_workers     = 2

# CosmosDB
cosmos_throughput           = 400
cosmos_consistency_level    = "Session"

# SQL Server
sql_sku                    = "Basic"
sql_max_size_gb            = 2

# Storage
storage_replication_type   = "LRS"

# Tags
tags = {
  Environment = "dev"
  Project     = "partial-smart-data"
  ManagedBy   = "terraform"
}
