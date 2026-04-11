output "api_endpoint" {
  description = "HTTP API load balancer URL"
  value       = "http://${aws_lb.api.dns_name}:8000"
}

output "flight_endpoint" {
  description = "Arrow Flight / gRPC load balancer endpoint"
  value       = "${aws_lb.flight.dns_name}:8815"
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = aws_instance.primary.private_ip
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
