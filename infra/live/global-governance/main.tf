# 1. Grupo de Recursos Central de Governança
resource "azurerm_resource_group" "gov_rg" {
  name     = "rg-datamesh-governance-prod"
  location = "eastus2"
}

# 2. Storage Account para o Root do Unity Catalog
resource "azurerm_storage_account" "uc_storage" {
  name                     = "stucrootdatameshprod" # Se este nome já foi criado, mude levemente (ex: stucrootmeshbr)
  resource_group_name      = azurerm_resource_group.gov_rg.name
  location                 = azurerm_resource_group.gov_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Obrigatório para ADLS Gen2 (Hierarchical Namespace)
}

# Container raiz
resource "azurerm_storage_data_lake_gen2_filesystem" "uc_container" {
  name               = "unity-catalog-root"
  storage_account_id = azurerm_storage_account.uc_storage.id
}

# 3. Access Connector (Managed Identity do Databricks)
resource "azurerm_databricks_access_connector" "uc_connector" {
  name                = "ext-access-connector-uc"
  resource_group_name = azurerm_resource_group.gov_rg.name
  location            = azurerm_resource_group.gov_rg.location
  identity {
    type = "SystemAssigned"
  }
}

# Concedendo permissão de leitura/escrita no Storage para a Managed Identity
resource "azurerm_role_assignment" "uc_data_contributor" {
  scope                = azurerm_storage_account.uc_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc_connector.identity[0].principal_id
}

# Pegamos o primeiro ID retornado na lista oficial de IDs válidos
locals {
    metastore_id = "0e36e8d6-6803-497f-9102-f2af71bec95e"
}

# Cria a credencial interna para o Spark ler o disco físico
resource "databricks_storage_credential" "external_creds" {
  name = "uc_storage_credential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc_connector.id
  }
}