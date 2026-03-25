# Environment: Production
environment         = "pro"
location            = "eastus2"
resource_group_name = "rg-smartdata-pro"

# Databricks
databricks_sku             = "premium"
databricks_node_type       = "Standard_DS4_v2"
databricks_min_workers     = 2
databricks_max_workers     = 8

# CosmosDB
cosmos_throughput           = 2000
cosmos_consistency_level    = "BoundedStaleness"

# SQL Server
sql_sku                    = "S1"
sql_max_size_gb            = 50

# Storage
storage_replication_type   = "GRS"

# Tags
tags = {
  Environment = "pro"
  Project     = "partial-smart-data"
  ManagedBy   = "terraform"
}
