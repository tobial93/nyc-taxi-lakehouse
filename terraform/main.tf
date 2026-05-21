terraform {
  required_version = ">= 1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }

  backend "azurerm" {
    # Configured via CLI flags:
    # -backend-config="resource_group_name=<rg>"
    # -backend-config="storage_account_name=<sa>"
    # -backend-config="container_name=tfstate"
    # -backend-config="key=nyc-taxi-lakehouse/${var.environment}.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "azuread" {}

provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
}

locals {
  suffix = var.environment == "prd" ? "" : "-${var.environment}"
  tags = {
    project     = "nyc-taxi-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "main" {
  name     = "rg-nyc-taxi-lakehouse${local.suffix}"
  location = var.location
  tags     = local.tags
}
