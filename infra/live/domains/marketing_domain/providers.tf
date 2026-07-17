terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.20"
    }
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  azure_workspace_resource_id = "/subscriptions/${var.azure_subscription_id}/resourceGroups/${var.databricks_rg_name}/providers/Microsoft.Databricks/workspaces/${var.databricks_workspace_name}"
}