output "api_endpoint" {
  description = "HTTP API load balancer URL"
  value       = "http://${azurerm_public_ip.api.ip_address}:8000"
}

output "flight_endpoint" {
  description = "Arrow Flight / gRPC load balancer endpoint"
  value       = "${azurerm_public_ip.flight.ip_address}:8815"
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = azurerm_network_interface.primary.ip_configuration[0].private_ip_address
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
