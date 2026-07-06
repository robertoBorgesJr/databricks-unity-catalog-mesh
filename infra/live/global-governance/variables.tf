variable "azure_subscription_id" {
  type        = string
  description = "O ID da sua assinatura Azure"
}

variable "databricks_rg_name" {
  type        = string
  description = "Nome do grupo de recursos onde está o workspace do Databricks"
}

variable "databricks_workspace_name" {
  type        = string
  description = "Nome do workspace do Databricks"
}

variable "databricks_workspace_id" {
  type        = string
  description = "ID do workspace do Databricks"
}
