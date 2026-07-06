terraform {
    required_providers {
        azurerm = {
            source  = "hashicorp/azurerm"
            version = "~> 3.0"
        }
    }       
}

# criação do grupo de recursos
resource "azurerm_resource_group" "rg" {
    name     = var.resource_group_name
    location = var.location
}

# Rede Virtual (Vnet)
resource "azurerm_virtual_network" "vnet" {
    name                = var.vnet_name
    address_space       = [var.vnet_cidr]
    location            = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name
}

# Subnet privada (container subnet)
resource "azurerm_subnet" "private_subnet" {
    name                 = "${var.vnet_name}-private-sn"
    resource_group_name  = azurerm_resource_group.rg.name
    virtual_network_name = azurerm_virtual_network.vnet.name
    address_prefixes     = [var.private_subnet_cidr]

    delegation {
        name = "databricks-del-private"
        service_delegation {
            name    = "Microsoft.Databricks/workspaces"
            actions = [
                "Microsoft.Network/virtualNetworks/subnets/action",
                "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
                "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
            ]
        }
    }
}

# Subnet pública (host subnet)
resource "azurerm_subnet" "public_subnet" {
    name                 = "${var.vnet_name}-public-sn"
    resource_group_name  = azurerm_resource_group.rg.name
    virtual_network_name = azurerm_virtual_network.vnet.name
    address_prefixes     = [var.public_subnet_cidr]

    delegation {
        name = "databricks-del-public"
        service_delegation {
            name    = "Microsoft.Databricks/workspaces"
            actions = [
                "Microsoft.Network/virtualNetworks/subnets/action",
                "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
                "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
            ]
        }
    }
}

# Network Security Group (NSG) obrigatório para o Databricks
resource "azurerm_network_security_group" "nsg" {
    name                = "${var.vnet_name}-nsg"
    location            = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name
}

# Associação do NSG à Subnet privada
resource "azurerm_subnet_network_security_group_association" "private_assoc" {
    subnet_id                 = azurerm_subnet.private_subnet.id
    network_security_group_id = azurerm_network_security_group.nsg.id
}


# Associação do NSG à Subnet pública
resource "azurerm_subnet_network_security_group_association" "public_assoc" {
    subnet_id                 = azurerm_subnet.public_subnet.id
    network_security_group_id = azurerm_network_security_group.nsg.id
}
