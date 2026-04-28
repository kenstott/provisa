terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# ── Resource Group ─────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = merge(var.tags, { Project = "provisa" })
}

# ── Networking ─────────────────────────────────────────────────────────────────

resource "azurerm_virtual_network" "main" {
  name                = "provisa-vnet"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = [var.vnet_cidr]
  tags                = merge(var.tags, { Project = "provisa" })
}

resource "azurerm_subnet" "nodes" {
  name                 = "provisa-nodes"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_cidr, 8, 0)]
}

# ── Network Security Group ─────────────────────────────────────────────────────

resource "azurerm_network_security_group" "nodes" {
  name                = "provisa-nodes-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = merge(var.tags, { Project = "provisa" })

  security_rule {
    name                       = "AllowHTTPAPI"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8000"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowArrowFlight"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8815"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowIntraCluster"
    priority                   = 120
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }

  dynamic "security_rule" {
    for_each = var.ssh_public_key != "" && var.admin_cidr != "" ? [1] : []
    content {
      name                       = "AllowSSH"
      priority                   = 130
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = "22"
      source_address_prefix      = var.admin_cidr
      destination_address_prefix = "*"
    }
  }
}

resource "azurerm_subnet_network_security_group_association" "nodes" {
  subnet_id                 = azurerm_subnet.nodes.id
  network_security_group_id = azurerm_network_security_group.nodes.id
}

# ── Managed Identity — Blob Storage read for AppImage download ─────────────────

resource "azurerm_user_assigned_identity" "provisa" {
  name                = "provisa-node"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = merge(var.tags, { Project = "provisa" })
}

data "azurerm_storage_account" "appimage" {
  name                = var.storage_account_name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_role_assignment" "blob_reader" {
  scope                = "${data.azurerm_storage_account.appimage.id}/blobServices/default/containers/${var.storage_container}"
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.provisa.principal_id
}

# ── Public IPs for Load Balancer ───────────────────────────────────────────────

resource "azurerm_public_ip" "api" {
  name                = "provisa-api-pip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })
}

resource "azurerm_public_ip" "flight" {
  name                = "provisa-flight-pip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })
}

# ── Load Balancer — HTTP API (port 8000) ───────────────────────────────────────

resource "azurerm_lb" "api" {
  name                = "provisa-api-lb"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })

  frontend_ip_configuration {
    name                 = "provisa-api-frontend"
    public_ip_address_id = azurerm_public_ip.api.id
  }
}

resource "azurerm_lb_backend_address_pool" "api" {
  name            = "provisa-api-pool"
  loadbalancer_id = azurerm_lb.api.id
}

resource "azurerm_lb_probe" "api" {
  name            = "provisa-api-health"
  loadbalancer_id = azurerm_lb.api.id
  protocol        = "Http"
  port            = 8000
  request_path    = "/health"
  interval_in_seconds = 30
  number_of_probes    = 2
}

resource "azurerm_lb_rule" "api" {
  name                           = "provisa-api-rule"
  loadbalancer_id                = azurerm_lb.api.id
  protocol                       = "Tcp"
  frontend_port                  = 8000
  backend_port                   = 8000
  frontend_ip_configuration_name = "provisa-api-frontend"
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.api.id]
  probe_id                       = azurerm_lb_probe.api.id
}

# ── Load Balancer — Arrow Flight / gRPC (port 8815) ───────────────────────────

resource "azurerm_lb" "flight" {
  name                = "provisa-flight-lb"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })

  frontend_ip_configuration {
    name                 = "provisa-flight-frontend"
    public_ip_address_id = azurerm_public_ip.flight.id
  }
}

resource "azurerm_lb_backend_address_pool" "flight" {
  name            = "provisa-flight-pool"
  loadbalancer_id = azurerm_lb.flight.id
}

resource "azurerm_lb_probe" "flight" {
  name                = "provisa-flight-health"
  loadbalancer_id     = azurerm_lb.flight.id
  protocol            = "Tcp"
  port                = 8815
  interval_in_seconds = 30
  number_of_probes    = 2
}

resource "azurerm_lb_rule" "flight" {
  name                           = "provisa-flight-rule"
  loadbalancer_id                = azurerm_lb.flight.id
  protocol                       = "Tcp"
  frontend_port                  = 8815
  backend_port                   = 8815
  frontend_ip_configuration_name = "provisa-flight-frontend"
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.flight.id]
  probe_id                       = azurerm_lb_probe.flight.id
}

# ── Locals ─────────────────────────────────────────────────────────────────────

locals {
  vm_ram = {
    "Standard_D4s_v3"  = 16
    "Standard_D8s_v3"  = 32
    "Standard_D16s_v3" = 64
    "Standard_D32s_v3" = 128
    "Standard_E8s_v3"  = 64
    "Standard_E16s_v3" = 128
    "Standard_E32s_v3" = 256
  }
  effective_ram        = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.vm_ram, var.vm_size, 32)
  effective_worker_ram = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.vm_ram, var.worker_vm_size, 128)

  appimage_url = "https://${var.storage_account_name}.blob.core.windows.net/${var.storage_container}/${var.appimage_blob}"

  base_cloud_init = <<-YAML
    #cloud-config
    packages:
      - azure-cli
      - fuse
    runcmd:
      - az login --identity --username ${azurerm_user_assigned_identity.provisa.client_id}
      - az storage blob download --account-name ${var.storage_account_name} --container-name ${var.storage_container} --name ${var.appimage_blob} --file /opt/Provisa.AppImage --auth-mode login
      - chmod +x /opt/Provisa.AppImage
  YAML

  ssh_key_config = var.ssh_public_key != "" ? [{
    username   = var.admin_username
    public_key = var.ssh_public_key
  }] : []
}

# ── Primary Node ───────────────────────────────────────────────────────────────

resource "azurerm_network_interface" "primary" {
  name                = "provisa-primary-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = merge(var.tags, { Project = "provisa" })

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.nodes.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_network_interface_backend_address_pool_association" "primary_api" {
  network_interface_id    = azurerm_network_interface.primary.id
  ip_configuration_name   = "primary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.api.id
}

resource "azurerm_network_interface_backend_address_pool_association" "primary_flight" {
  network_interface_id    = azurerm_network_interface.primary.id
  ip_configuration_name   = "primary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.flight.id
}

resource "azurerm_linux_virtual_machine" "primary" {
  name                = "provisa-primary"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  size                = var.vm_size
  admin_username      = var.admin_username
  tags                = merge(var.tags, { Project = "provisa", Role = "primary" })

  network_interface_ids = [azurerm_network_interface.primary.id]

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.provisa.id]
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = var.os_disk_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  dynamic "admin_ssh_key" {
    for_each = local.ssh_key_config
    content {
      username   = admin_ssh_key.value.username
      public_key = admin_ssh_key.value.public_key
    }
  }

  disable_password_authentication = true

  custom_data = base64encode(<<-YAML
    ${local.base_cloud_init}
      - /opt/Provisa.AppImage --non-interactive --role primary --ram-gb ${local.effective_ram}
  YAML
  )
}

# ── Secondary Nodes ────────────────────────────────────────────────────────────

resource "azurerm_network_interface" "secondary" {
  count               = max(var.node_count - 1, 0)
  name                = "provisa-secondary-${count.index + 1}-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = merge(var.tags, { Project = "provisa" })

  ip_configuration {
    name                          = "secondary"
    subnet_id                     = azurerm_subnet.nodes.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_network_interface_backend_address_pool_association" "secondary_api" {
  count                   = max(var.node_count - 1, 0)
  network_interface_id    = azurerm_network_interface.secondary[count.index].id
  ip_configuration_name   = "secondary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.api.id
}

resource "azurerm_network_interface_backend_address_pool_association" "secondary_flight" {
  count                   = max(var.node_count - 1, 0)
  network_interface_id    = azurerm_network_interface.secondary[count.index].id
  ip_configuration_name   = "secondary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.flight.id
}

resource "azurerm_linux_virtual_machine" "secondary" {
  count               = max(var.node_count - 1, 0)
  name                = "provisa-secondary-${count.index + 1}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  size                = var.worker_vm_size
  admin_username      = var.admin_username
  tags                = merge(var.tags, { Project = "provisa", Role = "secondary" })

  network_interface_ids = [azurerm_network_interface.secondary[count.index].id]

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.provisa.id]
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = var.os_disk_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  dynamic "admin_ssh_key" {
    for_each = local.ssh_key_config
    content {
      username   = admin_ssh_key.value.username
      public_key = admin_ssh_key.value.public_key
    }
  }

  disable_password_authentication = true

  depends_on = [azurerm_linux_virtual_machine.primary]

  custom_data = base64encode(<<-YAML
    ${local.base_cloud_init}
      - /opt/Provisa.AppImage --non-interactive --role secondary --primary-ip ${azurerm_network_interface.primary.ip_configuration[0].private_ip_address} --ram-gb ${local.effective_worker_ram}
  YAML
  )
}
