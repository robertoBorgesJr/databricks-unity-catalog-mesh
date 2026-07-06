variable "azure_subscription_id"     { type = string }
variable "databricks_rg_name"         { type = string }
variable "databricks_workspace_name" { type = string }

variable "uc_access_connector_principal_id" {
  type        = string
  description = "O ID do Objeto (Principal ID) da Managed Identity criada na governanca"
}