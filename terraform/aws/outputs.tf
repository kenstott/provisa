output "shared_endpoint" {
  description = "The single shared NLB DNS name fronting every protocol port (REQ-1253). Point cloud.provisa.dev and *.provisa.dev here (CNAME / DNS-only)."
  value       = aws_lb.shared.dns_name
}

output "api_endpoint" {
  description = "HTTPS API load balancer URL (REQ-1227: TLS on every endpoint; self-signed cert)"
  value       = "https://${aws_lb.shared.dns_name}:${local.protocols.api.port}"
}

output "ui_endpoint" {
  description = "Web UI load balancer URL (HTTPS; self-signed cert)"
  value       = "https://${aws_lb.shared.dns_name}:${local.protocols.ui.port}"
}

output "protocol_endpoints" {
  description = "host:port for every exposed protocol on the shared NLB DNS name (api, ui, flight, and any enabled pgwire/bolt/mcp/grpc)"
  value       = { for k, v in local.enabled_protocols : k => "${aws_lb.shared.dns_name}:${v.port}" }
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = aws_instance.primary.private_ip
}

output "primary_dns" {
  description = "Private DNS name for the primary node (stable across replacements)"
  value       = aws_route53_record.primary.name
}

output "primary_public_ip" {
  description = "Public IP of the primary node (SSH access)"
  value       = aws_instance.primary.public_ip
}

output "secondary_ips" {
  description = "Private IPs of all secondary nodes"
  value       = aws_instance.secondary[*].private_ip
}

output "node_count" {
  description = "Total nodes deployed"
  value       = var.node_count
}
