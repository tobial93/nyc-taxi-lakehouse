data "azuread_client_config" "current" {}

# Service principal used by Databricks notebooks to access ADLS Gen2
resource "azuread_application" "databricks_sp" {
  display_name = "sp-nyc-taxi-lakehouse${local.suffix}"
}

resource "azuread_service_principal" "databricks_sp" {
  client_id = azuread_application.databricks_sp.client_id
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id
}

# Grant the SP read/write access to the data lake storage
resource "azurerm_role_assignment" "sp_storage_contributor" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks_sp.object_id
}

# Store SP credentials in a Databricks secret scope so notebooks never see plaintext keys
resource "databricks_secret_scope" "adls" {
  name = "adls"
}

resource "databricks_secret" "tenant_id" {
  key          = "tenant-id"
  string_value = data.azuread_client_config.current.tenant_id
  scope        = databricks_secret_scope.adls.name
}

resource "databricks_secret" "client_id" {
  key          = "client-id"
  string_value = azuread_application.databricks_sp.client_id
  scope        = databricks_secret_scope.adls.name
}

resource "databricks_secret" "client_secret" {
  key          = "client-secret"
  string_value = azuread_service_principal_password.databricks_sp.value
  scope        = databricks_secret_scope.adls.name
}
