# Instanciando a rede para o domínio de Vendas usando o nosso módulo
module "sales_network" {
  source = "../../modules/network"

  resource_group_name = "rg-datamesh-sales-prod"
  location            = "eastus2" # Região excelente com custo reduzido e total suporte a recursos Databricks
  vnet_name           = "vnet-sales-prod"
  
  # Definição sênior de endereçamento (Evitando conflitos futuros no Mesh)
  vnet_cidr           = "10.1.0.0/16"
  private_subnet_cidr = "10.1.1.0/24"
  public_subnet_cidr  = "10.1.2.0/24"
}