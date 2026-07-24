# Busca as informações do Workspace que já existe na Azure (necessário para o provider)
data "azurerm_databricks_workspace" "sales_workspace" {
  name                = var.databricks_workspace_name   
  resource_group_name = var.databricks_rg_name
}

# Instância de PRODUÇÃO: cria o cluster interativo do domínio + catálogo "_prod"
module "domain_config_prod" {
  source                     = "../../../modules/databricks_domain_config"
  azure_subscription_id     = var.azure_subscription_id
  databricks_rg_name        = var.databricks_rg_name
  databricks_workspace_name = var.databricks_workspace_name
  databricks_domain_name    = var.databricks_domain_name
  environment                = "prod"
  manage_cluster             = true
}

# Instância de DEV: reaproveita o cluster já criado acima, cria apenas
# storage/external location/catálogo "_dev" isolados
module "domain_config_dev" {
  source                     = "../../../modules/databricks_domain_config"
  azure_subscription_id     = var.azure_subscription_id
  databricks_rg_name        = var.databricks_rg_name
  databricks_workspace_name = var.databricks_workspace_name
  databricks_domain_name    = var.databricks_domain_name
  environment                = "dev"
  manage_cluster             = false
}