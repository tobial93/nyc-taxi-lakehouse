output "resource_group" {
  value = azurerm_resource_group.main.name
}

output "storage_account" {
  value = azurerm_storage_account.datalake.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "databricks_job_id_bronze" {
  description = "Set as Airflow Variable: DATABRICKS_JOB_ID_BRONZE"
  value       = databricks_job.bronze.id
}

output "databricks_job_id_silver" {
  description = "Set as Airflow Variable: DATABRICKS_JOB_ID_SILVER"
  value       = databricks_job.silver.id
}

output "databricks_job_id_gold" {
  description = "Set as Airflow Variable: DATABRICKS_JOB_ID_GOLD"
  value       = databricks_job.gold.id
}
