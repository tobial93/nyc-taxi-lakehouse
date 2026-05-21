variable "environment" {
  description = "Deployment environment: dev or prd"
  type        = string
  validation {
    condition     = contains(["dev", "prd"], var.environment)
    error_message = "environment must be dev or prd"
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope"
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name (globally unique, 3-24 chars, lowercase alphanumeric)"
  type        = string
}

variable "databricks_sku" {
  description = "Databricks workspace SKU: standard, premium, or trial"
  type        = string
  default     = "premium"
}

variable "cluster_node_type" {
  description = "VM size for the Databricks transform cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "spark_version" {
  description = "Databricks runtime version"
  type        = string
  default     = "14.3.x-scala2.12"
}
