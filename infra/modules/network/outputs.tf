output "vnet_id" {
  value = azurerm_virtual_network.vnet.id
}
output "private_subnet_name" {
  value = azurerm_subnet.private_subnet.name
}
output "public_subnet_name" {
  value = azurerm_subnet.public_subnet.name
}
output "rg_name" {
  value = azurerm_resource_group.rg.name
}