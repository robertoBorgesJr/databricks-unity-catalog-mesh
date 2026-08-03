# Busca a versão estável mais recente do Spark/Runtime
# (count evita recriar/duplicar quando manage_cluster = false)
data "databricks_spark_version" "latest_lts" {
  count             = var.manage_cluster ? 1 : 0
  long_term_support = true
}

# Busca o menor tipo de máquina para desenvolvimento econômico
data "databricks_node_type" "smallest" {
  count      = var.manage_cluster ? 1 : 0
  local_disk = true
  category   = "General Purpose"
}

# 1. Criação do Cluster Compartilhado do Domínio (Ideal para DBeaver e Dev Geral)
# Criado apenas quando manage_cluster = true (ex: na instância "prod" do módulo).
# O mesmo cluster é reaproveitado para consultar o catálogo "dev", já que
# compute é independente de catálogo no Unity Catalog.
resource "databricks_cluster" "domain_interactive_cluster" {
  count                    = var.manage_cluster ? 1 : 0
  cluster_name            = "srv-${var.databricks_domain_name}-spark-shared-dev"
  spark_version           = data.databricks_spark_version.latest_lts[0].id
  node_type_id            = data.databricks_node_type.smallest[0].id
  autotermination_minutes = 20 # Desliga em 20 min ocioso (FinOps)

  # Habilita o isolamento de usuários necessário para o Unity Catalog e DBeaver
  data_security_mode = "USER_ISOLATION"

  # 1 Worker fixo exigido pelo modo Shared em contas pagas Azure
  num_workers = 1

  custom_tags = {
    "Environment" = "Development"
    "Domain"      = var.databricks_domain_name
  }
}

# 2. Busca dinamicamente o Access Connector existente na Azure
data "azurerm_databricks_access_connector" "uc_connector" {
  name                = "ext-access-connector-uc"
  resource_group_name = "rg-datamesh-governance-prod"
}

# 3. Criando o Storage Account específico para o Domínio e Ambiente, de forma dinâmica
# Cada ambiente (prod/dev) recebe seu próprio storage account -- isolamento
# físico dos dados, evitando qualquer risco de um pipeline de teste em dev
# tocar acidentalmente no storage de produção.
resource "azurerm_storage_account" "domain_storage" {
  name                     = "st${var.databricks_domain_name}datamesh${var.environment}"
  resource_group_name      = "rg-datamesh-governance-prod"
  location                 = "eastus2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true 
}

resource "azurerm_storage_data_lake_gen2_filesystem" "domain_container" {
  name               = "${var.databricks_domain_name}-domain-data"
  storage_account_id = azurerm_storage_account.domain_storage.id
}

# 4. Concedendo permissão ao Access Connector de forma dinâmica
resource "azurerm_role_assignment" "domain_data_contributor" {
  scope                = azurerm_storage_account.domain_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_databricks_access_connector.uc_connector.identity[0].principal_id
}

# 5. Criando a External Location no Databricks
# Para o ambiente "prod", mantém o nome ORIGINAL (sem sufixo de ambiente) --
# preserva o recurso já existente, evitando destroy/recreate. Para "dev" (e
# quaisquer ambientes futuros), usa o nome com sufixo, já que é um recurso novo.
resource "databricks_external_location" "domain_external" {
  name            = var.environment == "prod" ? "${var.databricks_domain_name}_storage_location" : "${var.databricks_domain_name}_${var.environment}_storage_location"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.domain_container.name}@${azurerm_storage_account.domain_storage.name}.dfs.core.windows.net/"
  credential_name = "uc_storage_credential" 
  force_destroy   = true
}

# 6. O Catálogo do Domínio (um por ambiente: marketing_prod, marketing_dev, etc.)
resource "databricks_catalog" "domain_catalog" {
  name           = "${var.databricks_domain_name}_${var.environment}"
  storage_root   = databricks_external_location.domain_external.url
  comment        = "Catalogo ${var.environment} do Dominio de ${var.databricks_domain_name} - Data Mesh"
  force_destroy  = true
}

# 7. Criação das Camadas do Lakehouse dentro do Catálogo dinâmico
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.domain_catalog.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.domain_catalog.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.domain_catalog.name
  name         = "gold"
}

# Schema separado para metadados operacionais dos pipelines (ex: controle de
# watermark incremental), fora das camadas de negocio do medallion architecture.
resource "databricks_schema" "controle" {
  catalog_name = databricks_catalog.domain_catalog.name
  name         = "controle"
  comment      = "Metadados operacionais dos pipelines (ex: watermark_pipelines) - nao e dado de negocio"
}