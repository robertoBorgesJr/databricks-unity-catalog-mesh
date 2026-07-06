# 1. Busca dinamicamente o Access Connector existente na Azure (Forma Nativa e Correta)
data "azurerm_databricks_access_connector" "uc_connector" {
  name                = "ext-access-connector-uc"
  resource_group_name = "rg-datamesh-governance-prod"
}

# 2. Criando o Storage Account específico para o Domínio de Vendas (Isolamento de dados do Mesh)
resource "azurerm_storage_account" "sales_storage" {
  name                     = "stsalesdatameshprod" # Se der erro de nome já existente na Azure, mude levemente este nome
  resource_group_name      = "rg-datamesh-governance-prod"
  location                 = "eastus2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Obrigatório para Data Lake Gen2 (Hierarchical Namespace)
}

resource "azurerm_storage_data_lake_gen2_filesystem" "sales_container" {
  name               = "sales-domain-data"
  storage_account_id = azurerm_storage_account.sales_storage.id
}

# 3. Concedendo permissão ao Access Connector usando o ID descoberto dinamicamente (Sem duplicação)
resource "azurerm_role_assignment" "sales_data_contributor" {
  scope                = azurerm_storage_account.sales_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  
  # Captura o Principal ID diretamente de forma limpa e nativa
  principal_id         = data.azurerm_databricks_access_connector.uc_connector.identity[0].principal_id
}

# 4. Criando a External Location no Databricks apontando para o Storage de Vendas
resource "databricks_external_location" "sales_external" {
  name            = "sales_storage_location"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.sales_container.name}@${azurerm_storage_account.sales_storage.name}.dfs.core.windows.net/"
  credential_name = "uc_storage_credential" # Mapeado na nossa governança central
}

# 5. O Catálogo de Vendas (Apontando para a sua External Location dedicada)
resource "databricks_catalog" "sales_catalog" {
  name           = "vendas_prod"
  storage_root   = databricks_external_location.sales_external.url
  comment        = "Catalogo central do Dominio de Vendas - Data Mesh"
  force_destroy  = true
}

# 6. Criação das Camadas do Lakehouse (Schemas) dentro do Catálogo de Vendas
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.sales_catalog.name
  name         = "bronze"
  comment      = "Dados brutos do dominio de vendas"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.sales_catalog.name
  name         = "silver"
  comment      = "Dados limpos e dedupados de vendas"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.sales_catalog.name
  name         = "gold"
  comment      = "Modelos de negocio e agregacoes prontas para consumo"
}