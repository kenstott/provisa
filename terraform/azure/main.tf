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

  # One inbound Allow rule per enabled protocol port (REQ-1253). The shared LB
  # fronts all of them on a single frontend IP; this opens each port to the internet
  # so `{org}.provisa.dev:<port>` reaches the node. Priorities are spaced from 100.
  dynamic "security_rule" {
    for_each = local.enabled_protocols
    content {
      name                       = "AllowProtocol${title(security_rule.key)}"
      priority                   = 100 + index(keys(local.enabled_protocols), security_rule.key) * 10
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = tostring(security_rule.value.port)
      source_address_prefix      = "*"
      destination_address_prefix = "*"
    }
  }

  security_rule {
    name                       = "AllowIntraCluster"
    priority                   = 200
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
      priority                   = 210
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

# ── Standard Load Balancer — ONE shared IP, every protocol port ────────────────
# The subdomain-as-org model (REQ-1233/1253) requires {org}.provisa.dev to reach
# every protocol on a single A record: the org name resolves to one IP and the
# client connects the protocol's port (bolt 7687, pgwire 5439, …). A single
# Standard LB with one frontend public IP fronts every listener; one LB rule per
# enabled protocol preserves the destination port to the node, so a single
# `*.provisa.dev` record serves api/ui/flight/pgwire/bolt/mcp/grpc alike. The api
# rule gets an HTTPS /health probe (REQ-1227: TLS on every endpoint); the rest use
# a TCP connect probe. The NSG (local.enabled_protocols) still restricts which
# ports actually reach the node.

resource "azurerm_public_ip" "shared" {
  name                = "provisa-shared-pip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })
}

resource "azurerm_lb" "shared" {
  name                = "provisa-shared-lb"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  tags                = merge(var.tags, { Project = "provisa" })

  frontend_ip_configuration {
    name                 = "provisa-shared-frontend"
    public_ip_address_id = azurerm_public_ip.shared.id
  }
}

resource "azurerm_lb_backend_address_pool" "shared" {
  name            = "provisa-shared-pool"
  loadbalancer_id = azurerm_lb.shared.id
}

# One probe per enabled protocol. api uses an HTTPS /health probe (Azure Standard
# LB does not validate the cert, so the node's self-signed / operator wildcard cert
# is accepted); the rest use a TCP connect probe. first-launch brings all listeners
# up together, so app-level /health on the API port is the liveness signal.
resource "azurerm_lb_probe" "shared" {
  for_each        = local.enabled_protocols
  name            = "provisa-${each.key}-health"
  loadbalancer_id = azurerm_lb.shared.id
  protocol        = each.value.probe == "https" ? "Https" : "Tcp"
  port            = each.value.port
  request_path    = each.value.probe == "https" ? each.value.path : null

  interval_in_seconds = 30
  number_of_probes    = 2
}

# One rule per enabled protocol, all sharing the single frontend IP and backend
# pool. frontend_port == backend_port preserves the destination port to the node.
resource "azurerm_lb_rule" "shared" {
  for_each                       = local.enabled_protocols
  name                           = "provisa-${each.key}-rule"
  loadbalancer_id                = azurerm_lb.shared.id
  protocol                       = "Tcp"
  frontend_port                  = each.value.port
  backend_port                   = each.value.port
  frontend_ip_configuration_name = "provisa-shared-frontend"
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.shared.id]
  probe_id                       = azurerm_lb_probe.shared[each.key].id
}

# ── Private DNS ────────────────────────────────────────────────────────────────

resource "azurerm_private_dns_zone" "internal" {
  name                = "provisa.internal"
  resource_group_name = azurerm_resource_group.main.name
  tags                = merge(var.tags, { Project = "provisa" })
}

resource "azurerm_private_dns_zone_virtual_network_link" "internal" {
  name                  = "provisa-internal-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.internal.name
  virtual_network_id    = azurerm_virtual_network.main.id
  registration_enabled  = false
}

resource "azurerm_private_dns_a_record" "primary" {
  name                = "primary"
  zone_name           = azurerm_private_dns_zone.internal.name
  resource_group_name = azurerm_resource_group.main.name
  ttl                 = 30
  records             = [azurerm_network_interface.primary.ip_configuration[0].private_ip_address]
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

  # Core-images zip lives in the same container as the AppImage blob. When the blob
  # name carries a virtual-directory prefix, keep it; a bare blob name (dirname ".")
  # means the zip sits at the container root alongside it.
  images_zip  = "provisa-core-images-amd64-${var.provisa_version}.zip"
  images_blob = dirname(var.appimage_blob) == "." ? local.images_zip : "${dirname(var.appimage_blob)}/${local.images_zip}"

  # ── Protocol surface ─────────────────────────────────────────────────────────
  # One row per externally-reachable protocol. `enabled` gates the whole chain: NSG
  # port, LB probe, LB rule, and (for wire protocols) the container listener env var.
  # Add a row here and every layer follows. api uses an HTTPS /health probe (REQ-1227:
  # TLS on every endpoint); the rest use a TCP connect probe. `env` drives first-launch's
  # protocol overlay.
  protocols = {
    api    = { port = 8000, enabled = true, probe = "https", path = "/health", env = null }
    ui     = { port = 443, enabled = true, probe = "tcp", path = null, env = null }
    flight = { port = 8815, enabled = true, probe = "tcp", path = null, env = "FLIGHT_PORT" }
    pgwire = { port = 5439, enabled = var.enable_pgwire, probe = "tcp", path = null, env = "PROVISA_PGWIRE_PORT" }
    bolt   = { port = 7687, enabled = var.enable_bolt, probe = "tcp", path = null, env = "PROVISA_BOLT_PORT" }
    mcp    = { port = 8009, enabled = var.enable_mcp, probe = "tcp", path = null, env = "PROVISA_MCP_PORT" }
    grpc   = { port = 50051, enabled = var.enable_grpc, probe = "tcp", path = null, env = "GRPC_PORT" }
  }
  enabled_protocols = { for k, v in local.protocols : k => v if v.enabled }

  # Inline `env` assignments for each enabled wire protocol (the container listener
  # env var first-launch persists; its protocol overlay publishes the matching
  # container port and the shared LB fronts it). api/ui are served unconditionally
  # by the app image, so their env is null and skipped here.
  protocol_env = join(" ", [for k, v in local.enabled_protocols : "${v.env}=${v.port}" if v.env != null])

  base_cloud_init = <<-YAML
    #cloud-config
    packages:
      - azure-cli
      - fuse
      - unzip
    runcmd:
      - az login --identity --username ${azurerm_user_assigned_identity.provisa.client_id}
      - az storage blob download --account-name ${var.storage_account_name} --container-name ${var.storage_container} --name ${var.appimage_blob} --file /opt/Provisa.AppImage --auth-mode login
      - chmod +x /opt/Provisa.AppImage
      # Stage the amd64 core-images zip beside the AppImage; first-launch docker-loads
      # it locally (airgap path) when run from /opt with PROVISA_VERSION set (below).
      - az storage blob download --account-name ${var.storage_account_name} --container-name ${var.storage_container} --name ${local.images_blob} --file /opt/${local.images_zip} --auth-mode login
    %{~if var.tls_cert_pem != "" && var.tls_key_pem != ""~}
      # Operator-supplied wildcard TLS cert (REQ-1239). Written here and adopted by
      # first-launch's ensure_tls_certs via PROVISA_TLS_CERT/KEY (in deploy_env below),
      # which fans it out to every listener. base64 sidesteps PEM newline/quoting
      # hazards in the cloud-init YAML.
      - mkdir -p /etc/provisa/tls
      - printf '%s' '${base64encode(var.tls_cert_pem)}' | base64 -d > /etc/provisa/tls/node.crt
      - printf '%s' '${base64encode(var.tls_key_pem)}' | base64 -d > /etc/provisa/tls/node.key
      - chmod 600 /etc/provisa/tls/node.key
    %{~endif~}
  YAML

  # Deployment choices (parity with the desktop wizard, REQ-972..979) forwarded to
  # the AppImage as env — cloud-init runcmd runs a non-login shell, so prefix the
  # invocation inline rather than relying on exported env persisting. Carries the
  # deployment choices, the enabled wire-protocol listener ports (local.protocol_env),
  # the MCP host/role, the auth/IDP settings, and (when supplied) the operator TLS
  # cert paths written by base_cloud_init above.
  deploy_env = join(" ", compact([
    "env",
    "PROVISA_VERSION='${var.provisa_version}'",
    "PROVISA_ENGINE='${var.federation_engine}'",
    "PROVISA_ENGINE_URL='${var.engine_url}'",
    "PROVISA_MATERIALIZE_URL='${var.materialize_url}'",
    "PROVISA_OBS_MODE='${var.obs_mode}'",
    "PROVISA_OTLP_ENDPOINT='${var.otlp_endpoint}'",
    "PROVISA_INSTALL_DEMO='${var.install_demo ? "y" : "n"}'",
    local.protocol_env,
    # UI host-publish port: base compose publishes ${UI_PORT}:3000; 443 lets
    # https://cloud.provisa.dev resolve without a port suffix (.dev is HSTS-preloaded).
    "UI_PORT=${local.protocols.ui.port}",
    var.enable_mcp ? "PROVISA_MCP_HOST=0.0.0.0 PROVISA_MCP_ROLE='${var.mcp_role}'" : "",
    "PROVISA_IDP='${var.auth_provider}'",
    "FIREBASE_PROJECT_ID='${var.firebase_project_id}'",
    "FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'",
    var.tls_cert_pem != "" && var.tls_key_pem != "" ? "PROVISA_TLS_CERT=/etc/provisa/tls/node.crt PROVISA_TLS_KEY=/etc/provisa/tls/node.key" : "",
  ]))

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

resource "azurerm_network_interface_backend_address_pool_association" "primary" {
  network_interface_id    = azurerm_network_interface.primary.id
  ip_configuration_name   = "primary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.shared.id
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
      - cd /opt && ${local.deploy_env} /opt/Provisa.AppImage --non-interactive --role primary --ram-gb ${local.effective_ram}
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

resource "azurerm_network_interface_backend_address_pool_association" "secondary" {
  count                   = max(var.node_count - 1, 0)
  network_interface_id    = azurerm_network_interface.secondary[count.index].id
  ip_configuration_name   = "secondary"
  backend_address_pool_id = azurerm_lb_backend_address_pool.shared.id
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
      - cd /opt && ${local.deploy_env} /opt/Provisa.AppImage --non-interactive --role secondary --primary-ip primary.provisa.internal --ram-gb ${local.effective_worker_ram}
  YAML
  )
}
