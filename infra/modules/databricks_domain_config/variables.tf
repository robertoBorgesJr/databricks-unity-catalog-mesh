variable "azure_subscription_id" { type = string }
variable "databricks_rg_name" { type = string }
variable "databricks_workspace_name" { type = string }
variable "databricks_domain_name" { type = string }

variable "environment" {
  type        = string
  description = "Ambiente do catalogo criado por esta instancia do modulo: 'prod' ou 'dev'"
}

variable "manage_cluster" {
  type        = bool
  default     = true
  description = "Se true, este modulo cria o cluster interativo do dominio. Usar false ao instanciar o modulo para um segundo ambiente (ex: dev), pois o cluster ja existente (criado na instancia 'prod') e reaproveitado -- Unity Catalog permite que o mesmo cluster consulte catalogos diferentes."
}