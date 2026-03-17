locals {
  ssh_key = file(var.ssh_public_key_path)

  tags = {
    project = "joke-assignment"
  }
}

# -----------------------------------
# Resource Group
# -----------------------------------
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.resource_group_location
  tags     = local.tags
}

# -----------------------------------
# Virtual Networks
# -----------------------------------
resource "azurerm_virtual_network" "vnet_indonesia" {
  name                = "jokes-vnet-indonesia"
  location            = var.indonesia_location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.10.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "subnet_indonesia" {
  name                 = "jokes-subnet-indonesia"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet_indonesia.name
  address_prefixes     = ["10.10.1.0/24"]
}

resource "azurerm_virtual_network" "vnet_india" {
  name                = "jokes-vnet-india"
  location            = var.india_location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.20.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "subnet_india" {
  name                 = "jokes-subnet-india"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet_india.name
  address_prefixes     = ["10.20.1.0/24"]
}

resource "azurerm_virtual_network" "vnet_malaysia" {
  name                = "jokes-vnet-malaysia"
  location            = var.malaysia_location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.30.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "subnet_malaysia" {
  name                 = "jokes-subnet-malaysia"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet_malaysia.name
  address_prefixes     = ["10.30.1.0/24"]
}

# -----------------------------------
# VNet Peerings
# -----------------------------------
resource "azurerm_virtual_network_peering" "indonesia_to_india" {
  name                         = "indonesia-to-india"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_indonesia.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_india.id
  allow_virtual_network_access = true
}

resource "azurerm_virtual_network_peering" "india_to_indonesia" {
  name                         = "india-to-indonesia"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_india.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_indonesia.id
  allow_virtual_network_access = true
}

resource "azurerm_virtual_network_peering" "indonesia_to_malaysia" {
  name                         = "indonesia-to-malaysia"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_indonesia.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_malaysia.id
  allow_virtual_network_access = true
}

resource "azurerm_virtual_network_peering" "malaysia_to_indonesia" {
  name                         = "malaysia-to-indonesia"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_malaysia.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_indonesia.id
  allow_virtual_network_access = true
}

resource "azurerm_virtual_network_peering" "india_to_malaysia" {
  name                         = "india-to-malaysia"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_india.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_malaysia.id
  allow_virtual_network_access = true
}

resource "azurerm_virtual_network_peering" "malaysia_to_india" {
  name                         = "malaysia-to-india"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = azurerm_virtual_network.vnet_malaysia.name
  remote_virtual_network_id    = azurerm_virtual_network.vnet_india.id
  allow_virtual_network_access = true
}

# -----------------------------------
# Public IP for Kong only
# -----------------------------------
resource "azurerm_public_ip" "kong_pip" {
  name                = "kong-public-ip"
  location            = var.india_location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
  domain_name_label   = var.kong_dns_label
  tags                = local.tags
}

# -----------------------------------
# NSG for Kong only
# -----------------------------------
resource "azurerm_network_security_group" "kong_nsg" {
  name                = "kong-vm-nsg"
  location            = var.india_location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_network_security_rule" "kong_ssh" {
  name                        = "Allow-SSH"
  priority                    = 100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.rg.name
  network_security_group_name = azurerm_network_security_group.kong_nsg.name
}

resource "azurerm_network_security_rule" "kong_http" {
  name                        = "Allow-HTTP"
  priority                    = 110
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "80"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.rg.name
  network_security_group_name = azurerm_network_security_group.kong_nsg.name
}

resource "azurerm_network_security_rule" "kong_https" {
  name                        = "Allow-HTTPS"
  priority                    = 120
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "443"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.rg.name
  network_security_group_name = azurerm_network_security_group.kong_nsg.name
}

# -----------------------------------
# NSGs for private VMs
# -----------------------------------
resource "azurerm_network_security_group" "indonesia_nsg" {
  name                = "indonesia-private-vm-nsg"
  location            = var.indonesia_location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_network_security_rule" "indonesia_allow_vnet_inbound" {
  name                        = "Allow-VNet-Inbound"
  priority                    = 100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "VirtualNetwork"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.rg.name
  network_security_group_name = azurerm_network_security_group.indonesia_nsg.name
}

resource "azurerm_network_security_group" "malaysia_nsg" {
  name                = "malaysia-private-vm-nsg"
  location            = var.malaysia_location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_network_security_rule" "malaysia_allow_vnet_inbound" {
  name                        = "Allow-VNet-Inbound"
  priority                    = 100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "VirtualNetwork"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.rg.name
  network_security_group_name = azurerm_network_security_group.malaysia_nsg.name
}

# -----------------------------------
# NICs
# -----------------------------------

# Kong - Central India
resource "azurerm_network_interface" "kong_nic" {
  name                = "kong-vm-nic"
  location            = var.india_location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet_india.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.20.1.10"
    public_ip_address_id          = azurerm_public_ip.kong_pip.id
  }

  tags = local.tags
}

resource "azurerm_network_interface_security_group_association" "kong_assoc" {
  network_interface_id      = azurerm_network_interface.kong_nic.id
  network_security_group_id = azurerm_network_security_group.kong_nsg.id
}

# Joke - Southeast Asia
resource "azurerm_network_interface" "joke_nic" {
  name                = "joke-db-etl-vm-nic"
  location            = var.indonesia_location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet_indonesia.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.10.1.20"
  }

  tags = local.tags
}

resource "azurerm_network_interface_security_group_association" "joke_assoc" {
  network_interface_id      = azurerm_network_interface.joke_nic.id
  network_security_group_id = azurerm_network_security_group.indonesia_nsg.id
}

# RabbitMQ - Southeast Asia
resource "azurerm_network_interface" "rabbitmq_nic" {
  name                = "rabbitmq-vm-nic"
  location            = var.indonesia_location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet_indonesia.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.10.1.30"
  }

  tags = local.tags
}

resource "azurerm_network_interface_security_group_association" "rabbitmq_assoc" {
  network_interface_id      = azurerm_network_interface.rabbitmq_nic.id
  network_security_group_id = azurerm_network_security_group.indonesia_nsg.id
}

# Submit - Malaysia
resource "azurerm_network_interface" "submit_nic" {
  name                = "submit-vm-nic"
  location            = var.malaysia_location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet_malaysia.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.30.1.20"
  }

  tags = local.tags
}

resource "azurerm_network_interface_security_group_association" "submit_assoc" {
  network_interface_id      = azurerm_network_interface.submit_nic.id
  network_security_group_id = azurerm_network_security_group.malaysia_nsg.id
}

# Moderate - Malaysia
resource "azurerm_network_interface" "moderate_nic" {
  name                = "moderate-vm-nic"
  location            = var.malaysia_location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet_malaysia.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.30.1.30"
  }

  tags = local.tags
}

resource "azurerm_network_interface_security_group_association" "moderate_assoc" {
  network_interface_id      = azurerm_network_interface.moderate_nic.id
  network_security_group_id = azurerm_network_security_group.malaysia_nsg.id
}

# -----------------------------------
# Virtual Machines
# -----------------------------------

# Kong - Central India
resource "azurerm_linux_virtual_machine" "kong_vm" {
  name                = "kong-vm"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.india_location
  size                = var.kong_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.kong_nic.id
  ]

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = local.ssh_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-arm64"
    version   = "latest"
  }

  tags = local.tags
}

# Joke - Southeast Asia
resource "azurerm_linux_virtual_machine" "joke_vm" {
  name                = "joke-db-etl-vm"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.indonesia_location
  size                = var.indonesia_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.joke_nic.id
  ]

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = local.ssh_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  tags = local.tags
}

# RabbitMQ - Southeast Asia
resource "azurerm_linux_virtual_machine" "rabbitmq_vm" {
  name                = "rabbitmq-vm"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.indonesia_location
  size                = var.indonesia_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.rabbitmq_nic.id
  ]

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = local.ssh_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  tags = local.tags
}

# Submit - Malaysia
resource "azurerm_linux_virtual_machine" "submit_vm" {
  name                = "submit-vm"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.malaysia_location
  size                = var.malaysia_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.submit_nic.id
  ]

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = local.ssh_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  tags = local.tags
}

# Moderate - Malaysia
resource "azurerm_linux_virtual_machine" "moderate_vm" {
  name                = "moderate-vm"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.malaysia_location
  size                = var.malaysia_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.moderate_nic.id
  ]

  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = local.ssh_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  tags = local.tags
}