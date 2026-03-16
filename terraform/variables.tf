variable "subscription_id" {
  type        = string
  description = "Azure subscription ID"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name"
}

variable "resource_group_location" {
  type        = string
  description = "Resource group location"
}

variable "admin_username" {
  type        = string
  description = "Admin username for all VMs"
}

variable "ssh_public_key_path" {
  type        = string
  description = "Path to SSH public key"
}

variable "kong_dns_label" {
  type        = string
  description = "DNS label for Kong public IP"
}

variable "indonesia_location" {
  type        = string
  description = "Azure region for Indonesia resources"
}

variable "india_location" {
  type        = string
  description = "Azure region for India resources"
}

variable "malaysia_location" {
  type        = string
  description = "Azure region for Malaysia resources"
}

variable "kong_vm_size" {
  type        = string
  description = "VM size for Kong VM"
}

variable "default_vm_size" {
  type        = string
  description = "Default VM size for the other VMs"
}