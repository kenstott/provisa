terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = merge(var.tags, { Project = "provisa" }) }
}

# ── Data ───────────────────────────────────────────────────────────────────────

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ── Networking ─────────────────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
}

data "aws_availability_zones" "available" { state = "available" }

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ── Security Groups ────────────────────────────────────────────────────────────
# An NLB is a passthrough that preserves the client IP — there is no LB security
# group to source from, so the node SG opens every enabled protocol port straight
# to the internet (REQ-1253: one shared endpoint fronting every protocol port).
# One ingress rule per enabled protocol, driven by local.enabled_protocols.

resource "aws_security_group" "nodes" {
  name        = "provisa-nodes"
  description = "Provisa nodes — every enabled protocol port, internal services"
  vpc_id      = aws_vpc.main.id

  # One rule per externally-reachable protocol (NLB preserves client IP, so the
  # node itself is the ingress boundary — no LB SG to source from).
  dynamic "ingress" {
    for_each = local.enabled_protocols
    content {
      description = "${ingress.key} protocol"
      from_port   = ingress.value.port
      to_port     = ingress.value.port
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }
  # Internal cluster — all ports between nodes
  ingress {
    description = "Intra-cluster"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  # SSH (optional)
  dynamic "ingress" {
    for_each = var.admin_cidr != "" && var.key_pair != "" ? [1] : []
    content {
      description = "SSH admin"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [var.admin_cidr]
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── IAM — S3 read for AppImage download ───────────────────────────────────────

resource "aws_iam_role" "provisa" {
  name = "provisa-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "s3_appimage" {
  name = "provisa-s3-appimage"
  role = aws_iam_role.provisa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject"]
      Resource = [
        "arn:aws:s3:::${var.appimage_s3_bucket}/${var.appimage_s3_key}",
        "arn:aws:s3:::${var.appimage_s3_bucket}/${local.images_key}",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "provisa" {
  name = "provisa-node"
  role = aws_iam_role.provisa.name
}

# ── Local: compute RAM budget for user-data ───────────────────────────────────

locals {
  # Map instance type to RAM GB for Trino worker calculation
  instance_ram = {
    "m7i.xlarge"   = 16
    "m7i.2xlarge"  = 32
    "m7i.4xlarge"  = 64
    "m7i.8xlarge"  = 128
    "r7i.2xlarge"  = 64
    "r7i.4xlarge"  = 128
    "r7i.8xlarge"  = 256
    "r7i.12xlarge" = 384
  }
  effective_ram        = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.instance_ram, var.instance_type, 32)
  effective_worker_ram = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.instance_ram, var.worker_instance_type, 128)

  appimage_url = "s3://${var.appimage_s3_bucket}/${var.appimage_s3_key}"

  # Core-images zip lives in the same S3 directory as the AppImage. dirname of a
  # bare key (no slash) is ".", so keep appimage_s3_key prefixed (default: releases/).
  images_zip = "provisa-core-images-amd64-${var.provisa_version}.zip"
  images_key = "${dirname(var.appimage_s3_key)}/${local.images_zip}"

  # ── Protocol surface ─────────────────────────────────────────────────────────
  # One row per externally-reachable protocol. `enabled` gates the whole chain:
  # node SG ingress port, NLB target group, listener, target attachments, and (for
  # wire protocols) the container listener env var. Add a row here and every layer
  # follows. api uses an HTTPS /health probe (REQ-1227: TLS on every endpoint); the
  # rest use a TCP connect probe. `env`/`port` drive first-launch's protocol overlay.
  protocols = {
    api    = { port = 8000, enabled = true, probe = "https", path = "/health", env = null }
    ui     = { port = 443, enabled = true, probe = "tcp", path = null, env = null }
    flight = { port = 8815, enabled = true, probe = "tcp", path = null, env = "FLIGHT_PORT" }
    pgwire = { port = 5439, enabled = var.enable_pgwire, probe = "tcp", path = null, env = "PROVISA_PGWIRE_PORT" }
    bolt   = { port = 7687, enabled = var.enable_bolt, probe = "tcp", path = null, env = "PROVISA_BOLT_PORT" }
    mcp    = { port = 8009, enabled = var.enable_mcp, probe = "tcp", path = null, env = "PROVISA_MCP_PORT" }
    grpc   = { port = 50051, enabled = var.enable_grpc, probe = "tcp", path = null, env = "GRPC_PORT" }
  }
  enabled_protocols = { for k, v in local.protocols : k => v if v.enabled }
  # Shell `export` lines fed into base_user_data: the container listener env var for
  # each enabled wire protocol (first-launch persists these and its protocol overlay
  # publishes the matching container port). api/ui/flight are served unconditionally
  # by the app image, so only the opt-in protocols need a toggle.
  protocol_exports = join("\n    ", concat(
    [for k, v in local.enabled_protocols : "export ${v.env}=${v.port}" if contains(["pgwire", "bolt", "mcp", "grpc"], k)],
    var.enable_mcp ? ["export PROVISA_MCP_HOST=0.0.0.0", "export PROVISA_MCP_ROLE=${var.mcp_role}"] : []
  ))

  base_user_data = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    apt-get update -qq
    apt-get install -y -qq awscli fuse unzip
    aws s3 cp ${local.appimage_url} /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
    # Stage the amd64 core-images zip beside the AppImage. first-launch searches cwd
    # for provisa-core-images-amd64-$PROVISA_VERSION.zip and docker-loads it locally
    # (airgap path), so we cd /opt before launching below.
    aws s3 cp s3://${var.appimage_s3_bucket}/${local.images_key} /opt/${local.images_zip}
    export PROVISA_VERSION="${var.provisa_version}"
    # Deployment choices (parity with the desktop wizard, REQ-972..979). The Linux
    # first-launch reads these env vars in --non-interactive mode.
    export PROVISA_ENGINE="${var.federation_engine}"
    export PROVISA_ENGINE_URL="${var.engine_url}"
    export PROVISA_MATERIALIZE_URL="${var.materialize_url}"
    export PROVISA_OBS_MODE="${var.obs_mode}"
    export PROVISA_OTLP_ENDPOINT="${var.otlp_endpoint}"
    export PROVISA_INSTALL_DEMO="${var.install_demo ? "y" : "n"}"
    # Auth (REQ-972..979 parity). PROVISA_IDP drives _auto_configure_idp; the
    # server reads these at runtime, so first-launch persists them into the
    # systemd unit's EnvironmentFile.
    export PROVISA_IDP="${var.auth_provider}"
    export FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'
    # Opt-in wire-protocol listeners (pgwire/bolt/mcp/grpc). first-launch persists
    # these into the systemd EnvironmentFile and its protocol overlay publishes the
    # matching container ports; the NLB above fronts each one.
    ${local.protocol_exports}
    # UI host-publish port. The UI container listens on 3000 (fixed in the node
    # overlay's uvicorn command); the base compose publishes $${UI_PORT}:3000 on the
    # host. Moving it to 443 lets https://cloud.provisa.dev resolve with no port
    # suffix (.dev is HSTS-preloaded, so browsers force https:443). first-launch
    # persists UI_PORT into the systemd EnvironmentFile so `provisa start`'s compose
    # interpolation picks it up.
    export UI_PORT=${local.protocols.ui.port}
%{ if var.tls_cert_pem != "" && var.tls_key_pem != "" }
    # Operator-supplied wildcard TLS cert (REQ-1239). Written here and adopted by
    # first-launch's ensure_tls_certs via PROVISA_TLS_CERT/KEY, which fans it out to
    # every listener. base64 sidesteps PEM newline/quoting hazards in the user-data heredoc.
    mkdir -p /etc/provisa/tls
    printf '%s' '${base64encode(var.tls_cert_pem)}' | base64 -d > /etc/provisa/tls/node.crt
    printf '%s' '${base64encode(var.tls_key_pem)}' | base64 -d > /etc/provisa/tls/node.key
    chmod 600 /etc/provisa/tls/node.key
    export PROVISA_TLS_CERT=/etc/provisa/tls/node.crt
    export PROVISA_TLS_KEY=/etc/provisa/tls/node.key
%{ endif }
    cd /opt
  SHELL

  ssh_key_args = var.key_pair != "" ? "key_name = \"${var.key_pair}\"" : ""
}

# ── Private DNS ────────────────────────────────────────────────────────────────

resource "aws_route53_zone" "internal" {
  name = "provisa.internal"
  vpc {
    vpc_id = aws_vpc.main.id
  }
}

resource "aws_route53_record" "primary" {
  zone_id = aws_route53_zone.internal.zone_id
  name    = "primary.provisa.internal"
  type    = "A"
  ttl     = 30
  records = [aws_instance.primary.private_ip]
}

# ── Primary Node ───────────────────────────────────────────────────────────────

resource "aws_instance" "primary" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.nodes.id]
  iam_instance_profile   = aws_iam_instance_profile.provisa.name
  key_name               = var.key_pair != "" ? var.key_pair : null

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
  }

  user_data = <<-SHELL
    ${local.base_user_data}
    /opt/Provisa.AppImage \
      --non-interactive \
      --role primary \
      --ram-gb ${local.effective_ram}
  SHELL

  tags = { Name = "provisa-primary" }
}

# ── Secondary Nodes ────────────────────────────────────────────────────────────

resource "aws_instance" "secondary" {
  count                  = max(var.node_count - 1, 0)
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.worker_instance_type
  subnet_id              = aws_subnet.public[count.index % 2].id
  vpc_security_group_ids = [aws_security_group.nodes.id]
  iam_instance_profile   = aws_iam_instance_profile.provisa.name
  key_name               = var.key_pair != "" ? var.key_pair : null

  depends_on = [aws_instance.primary]

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
  }

  user_data = <<-SHELL
    ${local.base_user_data}
    /opt/Provisa.AppImage \
      --non-interactive \
      --role secondary \
      --primary-ip primary.provisa.internal \
      --ram-gb ${local.effective_worker_ram}
  SHELL

  tags = { Name = "provisa-secondary-${count.index + 1}" }
}

# ── Network Load Balancer — ONE shared endpoint, every protocol port ──────────
# The subdomain-as-org model (REQ-1233/1253) requires {org}.provisa.dev to reach
# every protocol on a single DNS name: the org name resolves to the NLB and the
# client connects the protocol's port (bolt 7687, pgwire 5439, …). A single
# passthrough NLB fronts every listener — the destination port is preserved to the
# node, so one `*.provisa.dev` record serves api/ui/flight/pgwire/bolt/mcp/grpc
# alike. This is the AWS equivalent of GCP's all_ports passthrough forwarding rule:
# one target group + listener per enabled protocol, all on the same NLB DNS name.
# The api target group probes app-level HTTPS /health (REQ-1227); the rest use a
# TCP connect probe, since first-launch brings all listeners up together.

resource "aws_lb" "shared" {
  name               = "provisa-shared"
  load_balancer_type = "network"
  subnets            = aws_subnet.public[*].id
  internal           = false
}

resource "aws_lb_target_group" "protocol" {
  for_each = local.enabled_protocols
  name     = "provisa-${each.key}"
  port     = each.value.port
  protocol = "TCP"
  vpc_id   = aws_vpc.main.id

  # api probes app-level HTTPS /health (REQ-1227); AWS does not validate the cert,
  # so the node's self-signed (or operator wildcard) cert is accepted. Every other
  # protocol uses a TCP connect probe — first-launch brings all listeners up together.
  health_check {
    protocol            = each.value.probe == "https" ? "HTTPS" : "TCP"
    path                = each.value.probe == "https" ? each.value.path : null
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "protocol" {
  for_each          = local.enabled_protocols
  load_balancer_arn = aws_lb.shared.arn
  port              = each.value.port
  protocol          = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.protocol[each.key].arn
  }
}

resource "aws_lb_target_group_attachment" "primary" {
  for_each         = local.enabled_protocols
  target_group_arn = aws_lb_target_group.protocol[each.key].arn
  target_id        = aws_instance.primary.id
  port             = each.value.port
}

# One attachment per (enabled protocol × secondary node). Flatten the product into
# a single map keyed "protocol-index" so a single for_each covers every combination.
resource "aws_lb_target_group_attachment" "secondary" {
  for_each = {
    for pair in setproduct(keys(local.enabled_protocols), range(max(var.node_count - 1, 0))) :
    "${pair[0]}-${pair[1]}" => { protocol = pair[0], index = pair[1] }
  }
  target_group_arn = aws_lb_target_group.protocol[each.value.protocol].arn
  target_id        = aws_instance.secondary[each.value.index].id
  port             = local.enabled_protocols[each.value.protocol].port
}
