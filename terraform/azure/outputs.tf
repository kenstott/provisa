output "shared_endpoint" {
  description = "The single shared LB IP fronting every protocol port (REQ-1253). Point cloud.provisa.dev and *.provisa.dev here."
  value       = azurerm_public_ip.shared.ip_address
}

output "api_endpoint" {
  description = "HTTPS API load balancer URL (REQ-1227: TLS on every endpoint; self-signed cert)"
  value       = "https://${azurerm_public_ip.shared.ip_address}:${local.protocols.api.port}"
}

output "ui_endpoint" {
  description = "Web UI load balancer URL (HTTPS; self-signed cert)"
  value       = "https://${azurerm_public_ip.shared.ip_address}:${local.protocols.ui.port}"
}

output "protocol_endpoints" {
  description = "host:port for every exposed protocol on the shared LB IP (api, ui, flight, and any enabled pgwire/bolt/mcp/grpc)"
  value       = { for k, v in local.enabled_protocols : k => "${azurerm_public_ip.shared.ip_address}:${v.port}" }
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = azurerm_network_interface.primary.ip_configuration[0].private_ip_address
}

output "primary_dns" {
  description = "Private DNS name for the primary node (stable across replacements)"
  value       = "${azurerm_private_dns_a_record.primary.name}.${azurerm_private_dns_zone.internal.name}"
}

output "secondary_ips" {
  description = "Private IPs of all secondary nodes"
  value       = [for nic in azurerm_network_interface.secondary : nic.ip_configuration[0].private_ip_address]
}

output "node_count" {
  description = "Total nodes deployed"
  value       = var.node_count
}

output "managed_identity_client_id" {
  description = "Client ID of the user-assigned managed identity"
  value       = azurerm_user_assigned_identity.provisa.client_id
}
