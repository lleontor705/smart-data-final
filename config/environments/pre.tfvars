# Environment: Pre-production
environment         = "pre"
location            = "eastus2"
resource_group_name = "rg-smartdata-pre"

# Databricks
databricks_sku             = "premium"
databricks_node_type       = "Standard_DS3_v2"
databricks_min_workers     = 1
databricks_max_workers     = 4

# CosmosDB
cosmos_throughput           = 800
cosmos_consistency_level    = "Session"

# SQL Server
sql_sku                    = "S0"
sql_max_size_gb            = 10

# Storage
storage_replication_type   = "GRS"

# Tags
tags = {
  Environment = "pre"
  Project     = "partial-smart-data"
  ManagedBy   = "terraform"
}
