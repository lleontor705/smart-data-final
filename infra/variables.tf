# =============================================================================
# Variables
# =============================================================================

variable "environment" {
  description = "Environment name (dev, pre, pro)"
  type        = string
  validation {
    condition     = contains(["dev", "pre", "pro"], var.environment)
    error_message = "Environment must be dev, pre, or pro."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# Databricks
variable "databricks_sku" {
  description = "Databricks workspace SKU (standard or premium)"
  type        = string
  default     = "standard"
}

variable "databricks_node_type" {
  description = "Databricks cluster node type"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "databricks_min_workers" {
  description = "Minimum number of workers"
  type        = number
  default     = 1
}

variable "databricks_max_workers" {
  description = "Maximum number of workers"
  type        = number
  default     = 2
}

# CosmosDB
variable "cosmos_throughput" {
  description = "CosmosDB provisioned throughput (RU/s)"
  type        = number
  default     = 400
}

variable "cosmos_consistency_level" {
  description = "CosmosDB consistency level"
  type        = string
  default     = "Session"
}

# SQL Server
variable "sql_sku" {
  description = "Azure SQL Database SKU"
  type        = string
  default     = "Basic"
}

variable "sql_max_size_gb" {
  description = "Azure SQL Database max size in GB"
  type        = number
  default     = 2
}

# Storage
variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
}
