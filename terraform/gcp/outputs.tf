output "api_endpoint" {
  description = "HTTP API load balancer URL"
  value       = "http://${google_compute_address.api.address}:8000"
}

output "flight_endpoint" {
  description = "Arrow Flight / gRPC load balancer endpoint"
  value       = "${google_compute_address.flight.address}:8815"
}

output "primary_ip" {
  description = "Private IP of the primary node"
  value       = google_compute_instance.primary.network_interface[0].network_ip
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
