# The deployment's *.provisa.dev certificate, kept in ONE place: Secret Manager. Every VM installs
# it from there at each boot and hourly (tls/provisa-tls-sync.sh); the front door renews it there
# daily once fewer than tls_renew_days remain (tls/provisa-tls-renew.sh). Terraform creates the
# secrets and who may read and write them, never their contents: the first certificate is stored
# by terraform/gcp/scripts/issue-wildcard-cert.sh (PROJECT set), the Cloudflare token by the
# operator. A certificate in a VM's metadata was rewritten at every boot, so an expired one came
# back with the next wake.

resource "google_project_service" "secretmanager" {
  project = var.project
  service = "secretmanager.googleapis.com"
  # Leave the API on if the stack is torn down: other secrets in the project may use it.
  disable_on_destroy = false
}

locals {
  tls_cert_secret  = "provisa-tls-cert"
  tls_key_secret   = "provisa-tls-key"
  tls_token_secret = "provisa-cloudflare-dns-token"
}

resource "google_secret_manager_secret" "tls" {
  for_each  = toset([local.tls_cert_secret, local.tls_key_secret, local.tls_token_secret])
  secret_id = each.value
  labels    = local.all_labels
  replication {
    auto {}
  }
  depends_on = [google_project_service.secretmanager]
}

# Every VM reads the certificate and key; the front door alone reads the Cloudflare token and
# stores new versions of the two.
locals {
  tls_readers = {
    for pair in setproduct(
      [local.tls_cert_secret, local.tls_key_secret],
      [
        "serviceAccount:${google_service_account.provisa.email}",
        "serviceAccount:${google_service_account.front_door.email}",
      ],
    ) : "${pair[0]}|${pair[1]}" => { secret = pair[0], member = pair[1] }
  }
}

resource "google_secret_manager_secret_iam_member" "tls_reader" {
  for_each  = local.tls_readers
  secret_id = google_secret_manager_secret.tls[each.value.secret].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}

resource "google_secret_manager_secret_iam_member" "tls_renewer" {
  for_each  = toset([local.tls_cert_secret, local.tls_key_secret])
  secret_id = google_secret_manager_secret.tls[each.value].secret_id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = "serviceAccount:${google_service_account.front_door.email}"
}

resource "google_secret_manager_secret_iam_member" "dns_token_reader" {
  secret_id = google_secret_manager_secret.tls[local.tls_token_secret].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.front_door.email}"
}
