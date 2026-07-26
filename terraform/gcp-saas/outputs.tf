output "shared_ip" {
  description = "The single shared NetLB IP fronting every protocol port. Point provisa.dev and *.provisa.dev (subdomain-per-org) here (DNS-only)."
  value       = google_compute_address.shared.address
}

output "api_endpoint" {
  description = "HTTPS API load balancer URL (TLS on every endpoint)."
  value       = "https://${google_compute_address.shared.address}:${local.protocols.api.port}"
}

output "ui_endpoint" {
  description = "Web UI load balancer URL (HTTPS)."
  value       = "https://${google_compute_address.shared.address}:${local.protocols.ui.port}"
}

output "protocol_endpoints" {
  description = "host:port for every exposed protocol on the shared NetLB IP."
  value       = { for k, v in local.enabled_protocols : k => "${google_compute_address.shared.address}:${v.port}" }
}

output "coordinator_public_ip" {
  description = "Public IP of the coordinator (SSH access, TCP protocol origin)."
  value       = google_compute_instance.coordinator.network_interface[0].access_config[0].nat_ip
}

output "coordinator_dns" {
  description = "Private DNS name for the coordinator (workers' control/query endpoint)."
  value       = trimsuffix(google_dns_record_set.coordinator.name, ".")
}

output "cloudsql_instance" {
  description = "Cloud SQL control-plane instance name."
  value       = google_sql_database_instance.main.name
}

output "cloudsql_private_ip" {
  description = "Private IP of the Cloud SQL control-plane instance."
  value       = google_sql_database_instance.main.private_ip_address
}

output "cloudsql_password" {
  description = "Generated Cloud SQL 'provisa' user password."
  value       = random_password.db.result
  sensitive   = true
}

output "worker_min_nodes" {
  description = "Minimum autoscaled worker count (0 = scale-to-zero)."
  value       = var.worker_min_nodes
}

output "worker_max_nodes" {
  description = "Maximum autoscaled worker count."
  value       = var.worker_max_nodes
}

output "service_account_email" {
  description = "Service account email used by Provisa SaaS nodes."
  value       = google_service_account.provisa.email
}
