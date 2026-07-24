output "api_endpoint" {
  description = "HTTPS API load balancer URL (REQ-1227: TLS on every endpoint; self-signed cert)"
  value       = "https://${google_compute_address.protocol["api"].address}:${local.protocols.api.port}"
}

output "ui_endpoint" {
  description = "Web UI load balancer URL (HTTPS; self-signed cert)"
  value       = "https://${google_compute_address.protocol["ui"].address}:${local.protocols.ui.port}"
}

output "protocol_endpoints" {
  description = "host:port for every exposed protocol NetLB (api, ui, flight, and any enabled pgwire/bolt/mcp/grpc)"
  value       = { for k, v in local.enabled_protocols : k => "${google_compute_address.protocol[k].address}:${v.port}" }
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = google_compute_instance.primary.network_interface[0].network_ip
}

output "primary_dns" {
  description = "Private DNS name for the primary node (stable across replacements)"
  value       = trimsuffix(google_dns_record_set.primary.name, ".")
}

output "primary_public_ip" {
  description = "Public IP of the primary node (SSH access)"
  value       = google_compute_instance.primary.network_interface[0].access_config[0].nat_ip
}

output "secondary_ips" {
  description = "Private IPs of all secondary nodes"
  value       = [for inst in google_compute_instance.secondary : inst.network_interface[0].network_ip]
}

output "node_count" {
  description = "Total nodes deployed"
  value       = var.node_count
}

output "service_account_email" {
  description = "Service account email used by Provisa nodes"
  value       = google_service_account.provisa.email
}
