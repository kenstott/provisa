output "shared_ip" {
  description = "The single shared IP (front-door e2-micro) fronting every protocol port. Point provisa.dev and *.provisa.dev (subdomain-per-org) here (DNS-only)."
  value       = google_compute_address.shared.address
}

output "api_endpoint" {
  description = "HTTPS API URL via the front door (TLS on every endpoint)."
  value       = "https://${google_compute_address.shared.address}:${local.protocols.api.port}"
}

output "ui_endpoint" {
  description = "Web UI URL via the front door (HTTPS)."
  value       = "https://${google_compute_address.shared.address}:${local.protocols.ui.port}"
}

output "protocol_endpoints" {
  description = "host:port for every exposed protocol on the shared front-door IP."
  value       = { for k, v in local.enabled_protocols : k => "${google_compute_address.shared.address}:${v.port}" }
}

output "front_door_status_endpoint" {
  description = "Authenticated wake/verify API (GET /status, POST /wake; Authorization: Bearer <front_door_status_token>)."
  value       = "https://${google_compute_address.shared.address}:${var.front_door_status_port}/status"
}

output "front_door_status_token" {
  description = "Bearer token for the front-door wake/verify API."
  value       = random_password.front_door_token.result
  sensitive   = true
}

output "control_plane_public_ip" {
  description = "Public IP of the control-plane VM (SSH access, TCP protocol origin)."
  value       = google_compute_instance.coordinator.network_interface[0].access_config[0].nat_ip
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

output "engine_cluster_name" {
  description = "GKE cluster every federation engine runs on."
  value       = google_container_cluster.engine.name
}

output "shared_shard_endpoint" {
  description = "host:port the control plane dials for the shared (Starter) shard, as Cloud DNS publishes it VPC-wide."
  value       = "trino-shared-1.${local.engine_hostname_suffix}:8080"
}

output "service_account_email" {
  description = "Service account email used by Provisa SaaS nodes."
  value       = google_service_account.provisa.email
}
