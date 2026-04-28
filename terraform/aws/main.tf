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

resource "aws_security_group" "lb" {
  name        = "provisa-lb"
  description = "ALB/NLB — HTTP API and Arrow Flight"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP API"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "Arrow Flight / gRPC"
    from_port   = 8815
    to_port     = 8815
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "nodes" {
  name        = "provisa-nodes"
  description = "Provisa nodes — API, Flight, internal services"
  vpc_id      = aws_vpc.main.id

  # API from LB
  ingress {
    description     = "HTTP API from LB"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.lb.id]
  }
  # Flight from LB
  ingress {
    description     = "Arrow Flight from LB"
    from_port       = 8815
    to_port         = 8815
    protocol        = "tcp"
    security_groups = [aws_security_group.lb.id]
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
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = "arn:aws:s3:::${var.appimage_s3_bucket}/${var.appimage_s3_key}"
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

  base_user_data = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    apt-get update -qq
    apt-get install -y -qq awscli fuse
    aws s3 cp ${local.appimage_url} /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
  SHELL

  ssh_key_args = var.key_pair != "" ? "key_name = \"${var.key_pair}\"" : ""
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
      --primary-ip ${aws_instance.primary.private_ip} \
      --ram-gb ${local.effective_worker_ram}
  SHELL

  tags = { Name = "provisa-secondary-${count.index + 1}" }
}

# ── ALB — HTTP API (port 8000) ─────────────────────────────────────────────────

resource "aws_lb" "api" {
  name               = "provisa-api"
  load_balancer_type = "application"
  security_groups    = [aws_security_group.lb.id]
  subnets            = aws_subnet.public[*].id
  internal           = false
}

resource "aws_lb_target_group" "api" {
  name     = "provisa-api"
  port     = 8000
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id

  health_check {
    path                = "/health"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "api" {
  load_balancer_arn = aws_lb.api.arn
  port              = 8000
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

resource "aws_lb_target_group_attachment" "api_primary" {
  target_group_arn = aws_lb_target_group.api.arn
  target_id        = aws_instance.primary.id
  port             = 8000
}

resource "aws_lb_target_group_attachment" "api_secondary" {
  count            = max(var.node_count - 1, 0)
  target_group_arn = aws_lb_target_group.api.arn
  target_id        = aws_instance.secondary[count.index].id
  port             = 8000
}

# ── NLB — Arrow Flight / gRPC (port 8815) ─────────────────────────────────────

resource "aws_lb" "flight" {
  name               = "provisa-flight"
  load_balancer_type = "network"
  subnets            = aws_subnet.public[*].id
  internal           = false
}

resource "aws_lb_target_group" "flight" {
  name     = "provisa-flight"
  port     = 8815
  protocol = "TCP"
  vpc_id   = aws_vpc.main.id

  health_check {
    protocol            = "TCP"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "flight" {
  load_balancer_arn = aws_lb.flight.arn
  port              = 8815
  protocol          = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.flight.arn
  }
}

resource "aws_lb_target_group_attachment" "flight_primary" {
  target_group_arn = aws_lb_target_group.flight.arn
  target_id        = aws_instance.primary.id
  port             = 8815
}

resource "aws_lb_target_group_attachment" "flight_secondary" {
  count            = max(var.node_count - 1, 0)
  target_group_arn = aws_lb_target_group.flight.arn
  target_id        = aws_instance.secondary[count.index].id
  port             = 8815
}
