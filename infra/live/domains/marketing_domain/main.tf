
# Busca as informações do Workspace que já existe na Azure (necessário para o provider)
data "azurerm_databricks_workspace" "sales_workspace" {
  name                = var.databricks_workspace_name   
  resource_group_name = var.databricks_rg_name
}

# Invoca o módulo que agora contém TODA a inteligência de criação de infraestrutura do Domínio
module "domain_config" {
  source                     = "../../../modules/databricks_domain_config"
  azure_subscription_id     = var.azure_subscription_id
  databricks_rg_name        = var.databricks_rg_name
  databricks_workspace_name = var.databricks_workspace_name
  databricks_domain_name    = var.databricks_domain_name  
}
