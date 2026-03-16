output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "kong_public_ip" {
  value = azurerm_public_ip.kong_pip.ip_address
}

output "kong_fqdn" {
  value = azurerm_public_ip.kong_pip.fqdn
}

output "kong_private_ip" {
  value = azurerm_network_interface.kong_nic.private_ip_address
}

output "joke_private_ip" {
  value = azurerm_network_interface.joke_nic.private_ip_address
}

output "rabbitmq_private_ip" {
  value = azurerm_network_interface.rabbitmq_nic.private_ip_address
}

output "submit_private_ip" {
  value = azurerm_network_interface.submit_nic.private_ip_address
}

output "moderate_private_ip" {
  value = azurerm_network_interface.moderate_nic.private_ip_address
}