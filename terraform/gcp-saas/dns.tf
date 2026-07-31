# ── Public DNS — the two records the subdomain-per-org model depends on ─────────
#
# REQ-1233/1253: every org is a subdomain of one base domain, and REQ-1348 layers
# cross-subdomain sign-in on top of that — the control plane at `cloud.<base>` is the only
# host that ever runs a sign-in, and `{org}.<base>` acquires its bearer from it. Both halves
# stop working the moment either record is missing, and neither is recreatable from anything
# else in this module: they were hand-entered in the Cloudflare dashboard, so nothing in the
# repo recorded that `*.provisa.dev` must exist or where it points. That is what this file
# fixes.
#
# The front door owns the shared IP and preserves the destination port to the node, so one
# wildcard A record serves ui/api/pgwire/bolt/flight/gRPC/MCP alike — there is no per-protocol
# or per-org record to create when an org is provisioned, which is the property REQ-1054
# depends on (org creation must never write to external auth or DNS config).
#
# DNS-only (`proxied = false`) is required, not a preference. Cloudflare's proxy terminates
# HTTP(S) on a small set of ports and cannot carry the wire protocols at all; proxying would
# also hide the client IP from the front door's idle-stop accounting.
#
# Unmanaged by default: `dns_zone = ""` leaves every record alone, so an operator running this
# module against a domain on some other registrar is unaffected. Set `dns_zone` and
# `cloudflare_api_token` to hand the two records to terraform.
#
# The records already exist on provisa.dev, so adopt rather than recreate — a plain apply
# fails with "record already exists":
#
#   terraform import cloudflare_record.control_plane  <zone_id>/<record_id>
#   terraform import cloudflare_record.org_wildcard   <zone_id>/<record_id>
#
# (`GET /client/v4/zones/<zone_id>/dns_records` lists the ids.) Existing TTLs were 120 on
# `cloud` and 300 on `*`; both normalize to var.dns_ttl on the first apply after import.

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

locals {
  manage_dns = var.dns_zone != ""
}

data "cloudflare_zone" "public" {
  count = local.manage_dns ? 1 : 0
  name  = var.dns_zone
}

# The control plane. Named explicitly rather than covered by the wildcard below because
# lib/authHost.ts treats `cloud` as a reserved label — it is the one host that is not an org.
resource "cloudflare_record" "control_plane" {
  count   = local.manage_dns ? 1 : 0
  zone_id = data.cloudflare_zone.public[0].id
  name    = "cloud"
  type    = "A"
  content = google_compute_address.shared.address
  ttl     = var.dns_ttl
  proxied = false
  comment = "REQ-1348 control plane; the only host that runs a sign-in"
}

# Every org, present and future. An org is provisioned entirely inside the deployment; DNS
# must already answer for a subdomain nobody has created yet, which only a wildcard does.
resource "cloudflare_record" "org_wildcard" {
  count   = local.manage_dns ? 1 : 0
  zone_id = data.cloudflare_zone.public[0].id
  name    = "*"
  type    = "A"
  content = google_compute_address.shared.address
  ttl     = var.dns_ttl
  proxied = false
  comment = "REQ-1240 subdomain-per-org; DNS-only so in-container TLS terminates"
}
