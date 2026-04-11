#!/usr/bin/env bash
# Interactive Provisa deployment wrapper.
# Collects deployment parameters, validates credentials, writes terraform.tfvars,
# then runs terraform init + apply.
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

info()  { printf "${CYAN}[deploy]${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}[deploy]${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}[deploy]${NC} %s\n" "$*"; }
err()   { printf "${RED}[deploy]${NC} %s\n" "$*" >&2; }
fatal() { err "$*"; exit 1; }

ask() {
  local prompt="$1" default="${2:-}" var
  if [ -n "$default" ]; then
    printf "%s [%s]: " "$prompt" "$default"
  else
    printf "%s: " "$prompt"
  fi
  read -r var
  echo "${var:-$default}"
}

ask_secret() {
  local prompt="$1" var
  printf "%s: " "$prompt"
  read -rs var
  echo
  echo "$var"
}

pick() {
  local prompt="$1"; shift
  local options=("$@")
  echo "$prompt"
  local i=1
  for opt in "${options[@]}"; do
    printf "  [%d] %s\n" "$i" "$opt"
    (( i++ ))
  done
  local choice
  while true; do
    printf "Choice [1-%d]: " "${#options[@]}"
    read -r choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#options[@]} )); then
      echo "${options[$((choice-1))]}"
      return
    fi
    warn "Invalid choice."
  done
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Banner ─────────────────────────────────────────────────────────────────────

printf "\n${BOLD}Provisa Cloud Deployment${NC}\n"
printf "══════════════════════════════════════════\n\n"

# ── Cloud provider ─────────────────────────────────────────────────────────────

PROVIDER=$(pick "Cloud provider:" "AWS" "Azure" "GCP")

case "$PROVIDER" in
  AWS)   TERRAFORM_DIR="${SCRIPT_DIR}/aws" ;;
  Azure) TERRAFORM_DIR="${SCRIPT_DIR}/azure" ;;
  GCP)   TERRAFORM_DIR="${SCRIPT_DIR}/gcp" ;;
esac

# ── Node topology (all providers) ──────────────────────────────────────────────

echo
printf "Node topology:\n"
printf "  1 node  — single primary (no load balancer, no redundancy)\n"
printf "  2 nodes — 1 primary + 1 secondary + load balancer\n"
printf "  N nodes — 1 primary + N-1 secondaries + load balancer\n"
echo
NODE_COUNT=$(ask "Number of nodes" "2")
[[ "$NODE_COUNT" =~ ^[0-9]+$ ]] && (( NODE_COUNT >= 1 )) || fatal "node_count must be a positive integer."

# ── Provider-specific questions ────────────────────────────────────────────────

case "$PROVIDER" in

# ════════════════════════════════════════════════════════════════════════════════
AWS)

  info "Checking AWS credentials..."
  if aws sts get-caller-identity &>/dev/null 2>&1; then
    IDENTITY=$(aws sts get-caller-identity --query 'Arn' --output text 2>/dev/null)
    ok "Authenticated as: ${IDENTITY}"
  else
    warn "No active AWS session found."
    echo
    CRED_METHOD=$(pick "How to authenticate:" \
      "Enter AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY" \
      "Use named AWS profile" \
      "Exit and configure credentials manually")

    case "$CRED_METHOD" in
      "Enter AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
        export AWS_ACCESS_KEY_ID=$(ask_secret "AWS_ACCESS_KEY_ID")
        export AWS_SECRET_ACCESS_KEY=$(ask_secret "AWS_SECRET_ACCESS_KEY")
        AWS_SESSION_TOKEN=$(ask_secret "AWS_SESSION_TOKEN (leave blank if not using STS)")
        [ -n "$AWS_SESSION_TOKEN" ] && export AWS_SESSION_TOKEN
        ;;
      "Use named AWS profile")
        AWS_PROFILE=$(ask "Profile name" "default")
        export AWS_PROFILE
        ;;
      *)
        info "Configure credentials then re-run this script."
        info "  export AWS_ACCESS_KEY_ID=..."
        info "  export AWS_SECRET_ACCESS_KEY=..."
        exit 0
        ;;
    esac

    aws sts get-caller-identity &>/dev/null || fatal "AWS credentials invalid."
    ok "Credentials validated."
  fi

  echo
  REGION=$(ask "AWS region" "us-east-1")

  echo
  printf "Instance sizing guide:\n"
  printf "  m7i.xlarge   (4 vCPU,  16 GB) — dev / small datasets\n"
  printf "  m7i.2xlarge  (8 vCPU,  32 GB) — small prod, 1 Trino worker\n"
  printf "  m7i.4xlarge  (16 vCPU, 64 GB) — medium prod, 2 Trino workers\n"
  printf "  m7i.8xlarge  (32 vCPU,128 GB) — large prod, 4 Trino workers\n"
  echo
  INSTANCE_TYPE=$(ask "EC2 instance type" "m7i.2xlarge")
  ROOT_VOLUME_GB=$(ask "Root volume size (GB per node)" "100")
  RAM_BUDGET_GB=$(ask "RAM budget GB (0 = all available RAM)" "0")

  echo
  info "The Provisa AppImage must be uploaded to S3 before deployment."
  APPIMAGE_S3_BUCKET=$(ask "S3 bucket containing Provisa.AppImage")
  [ -z "$APPIMAGE_S3_BUCKET" ] && fatal "S3 bucket is required."
  APPIMAGE_S3_KEY=$(ask "S3 key" "releases/Provisa.AppImage")

  info "Verifying s3://${APPIMAGE_S3_BUCKET}/${APPIMAGE_S3_KEY}..."
  aws s3api head-object \
    --bucket "$APPIMAGE_S3_BUCKET" \
    --key "$APPIMAGE_S3_KEY" \
    --region "$REGION" &>/dev/null \
    || fatal "Object not found: s3://${APPIMAGE_S3_BUCKET}/${APPIMAGE_S3_KEY}"
  ok "AppImage found."

  echo
  SSH_ENABLED=$(pick "Enable SSH access to nodes?" "Yes" "No")
  KEY_PAIR=""
  ADMIN_CIDR=""
  if [ "$SSH_ENABLED" = "Yes" ]; then
    KEY_PAIR=$(ask "EC2 key pair name (must already exist in ${REGION})")
    ADMIN_CIDR=$(ask "CIDR allowed SSH access (e.g. 203.0.113.0/24)")
  fi

  VPC_CIDR=$(ask "VPC CIDR" "10.0.0.0/16")

  echo
  printf "${BOLD}Deployment summary${NC}\n"
  printf "══════════════════════════════════════════\n"
  printf "  Provider:     %s\n" "$PROVIDER"
  printf "  Region:       %s\n" "$REGION"
  printf "  Nodes:        %s\n" "$NODE_COUNT"
  printf "  Instance:     %s\n" "$INSTANCE_TYPE"
  printf "  Disk:         %s GB/node\n" "$ROOT_VOLUME_GB"
  printf "  RAM budget:   %s GB (0=all)\n" "$RAM_BUDGET_GB"
  printf "  AppImage:     s3://%s/%s\n" "$APPIMAGE_S3_BUCKET" "$APPIMAGE_S3_KEY"
  printf "  SSH key:      %s\n" "${KEY_PAIR:-disabled}"
  printf "  Admin CIDR:   %s\n" "${ADMIN_CIDR:-n/a}"
  printf "  VPC CIDR:     %s\n" "$VPC_CIDR"
  echo

  CONFIRM=$(pick "Proceed?" "Yes — deploy" "No — abort")
  [ "$CONFIRM" = "No — abort" ] && { info "Aborted."; exit 0; }

  TFVARS="${TERRAFORM_DIR}/terraform.tfvars"
  cat > "$TFVARS" <<EOF
region              = "${REGION}"
node_count          = ${NODE_COUNT}
instance_type       = "${INSTANCE_TYPE}"
root_volume_gb      = ${ROOT_VOLUME_GB}
ram_budget_gb       = ${RAM_BUDGET_GB}
appimage_s3_bucket  = "${APPIMAGE_S3_BUCKET}"
appimage_s3_key     = "${APPIMAGE_S3_KEY}"
key_pair            = "${KEY_PAIR}"
admin_cidr          = "${ADMIN_CIDR}"
vpc_cidr            = "${VPC_CIDR}"
EOF
  ;;

# ════════════════════════════════════════════════════════════════════════════════
Azure)

  info "Checking Azure credentials..."
  if az account show &>/dev/null 2>&1; then
    IDENTITY=$(az account show --query 'user.name' -o tsv 2>/dev/null)
    ok "Authenticated as: ${IDENTITY}"
  else
    warn "No active Azure session found."
    info "Run: az login"
    info "Then re-run this script."
    exit 1
  fi

  echo
  LOCATION=$(ask "Azure region" "eastus")
  RESOURCE_GROUP=$(ask "Resource group name" "provisa")

  echo
  printf "VM sizing guide:\n"
  printf "  Standard_D4s_v3  (4 vCPU,  16 GB) — dev / small datasets\n"
  printf "  Standard_D8s_v3  (8 vCPU,  32 GB) — small prod, 1 Trino worker\n"
  printf "  Standard_D16s_v3 (16 vCPU, 64 GB) — medium prod, 2 Trino workers\n"
  printf "  Standard_D32s_v3 (32 vCPU,128 GB) — large prod, 4 Trino workers\n"
  echo
  VM_SIZE=$(ask "VM size" "Standard_D8s_v3")
  OS_DISK_GB=$(ask "OS disk size (GB per node)" "100")
  RAM_BUDGET_GB=$(ask "RAM budget GB (0 = all available RAM)" "0")

  echo
  info "The Provisa AppImage must be uploaded to Azure Blob Storage before deployment."
  STORAGE_ACCOUNT=$(ask "Storage account name")
  [ -z "$STORAGE_ACCOUNT" ] && fatal "Storage account name is required."
  STORAGE_CONTAINER=$(ask "Blob container" "releases")
  APPIMAGE_BLOB=$(ask "Blob name" "Provisa.AppImage")

  info "Verifying blob..."
  az storage blob show \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$STORAGE_CONTAINER" \
    --name "$APPIMAGE_BLOB" \
    --auth-mode login &>/dev/null \
    || fatal "Blob not found: ${STORAGE_ACCOUNT}/${STORAGE_CONTAINER}/${APPIMAGE_BLOB}"
  ok "AppImage found."

  echo
  SSH_ENABLED=$(pick "Enable SSH access to nodes?" "Yes" "No")
  SSH_PUBLIC_KEY=""
  ADMIN_CIDR=""
  if [ "$SSH_ENABLED" = "Yes" ]; then
    SSH_PUBLIC_KEY_FILE=$(ask "Path to SSH public key file" "${HOME}/.ssh/id_rsa.pub")
    [ -f "$SSH_PUBLIC_KEY_FILE" ] || fatal "File not found: ${SSH_PUBLIC_KEY_FILE}"
    SSH_PUBLIC_KEY=$(cat "$SSH_PUBLIC_KEY_FILE")
    ADMIN_CIDR=$(ask "CIDR allowed SSH access (e.g. 203.0.113.0/24)")
  fi

  VNET_CIDR=$(ask "VNet CIDR" "10.0.0.0/16")

  echo
  printf "${BOLD}Deployment summary${NC}\n"
  printf "══════════════════════════════════════════\n"
  printf "  Provider:       %s\n" "$PROVIDER"
  printf "  Location:       %s\n" "$LOCATION"
  printf "  Resource group: %s\n" "$RESOURCE_GROUP"
  printf "  Nodes:          %s\n" "$NODE_COUNT"
  printf "  VM size:        %s\n" "$VM_SIZE"
  printf "  Disk:           %s GB/node\n" "$OS_DISK_GB"
  printf "  RAM budget:     %s GB (0=all)\n" "$RAM_BUDGET_GB"
  printf "  AppImage:       %s/%s/%s\n" "$STORAGE_ACCOUNT" "$STORAGE_CONTAINER" "$APPIMAGE_BLOB"
  printf "  SSH:            %s\n" "${ADMIN_CIDR:-disabled}"
  printf "  VNet CIDR:      %s\n" "$VNET_CIDR"
  echo

  CONFIRM=$(pick "Proceed?" "Yes — deploy" "No — abort")
  [ "$CONFIRM" = "No — abort" ] && { info "Aborted."; exit 0; }

  TFVARS="${TERRAFORM_DIR}/terraform.tfvars"
  cat > "$TFVARS" <<EOF
location               = "${LOCATION}"
resource_group_name    = "${RESOURCE_GROUP}"
node_count             = ${NODE_COUNT}
vm_size                = "${VM_SIZE}"
os_disk_gb             = ${OS_DISK_GB}
ram_budget_gb          = ${RAM_BUDGET_GB}
storage_account_name   = "${STORAGE_ACCOUNT}"
storage_container      = "${STORAGE_CONTAINER}"
appimage_blob          = "${APPIMAGE_BLOB}"
ssh_public_key         = "${SSH_PUBLIC_KEY}"
admin_cidr             = "${ADMIN_CIDR}"
vnet_cidr              = "${VNET_CIDR}"
EOF
  ;;

# ════════════════════════════════════════════════════════════════════════════════
GCP)

  info "Checking GCP credentials..."
  if gcloud auth print-access-token &>/dev/null 2>&1; then
    IDENTITY=$(gcloud config get-value account 2>/dev/null)
    ok "Authenticated as: ${IDENTITY}"
  else
    warn "No active GCP session found."
    info "Run: gcloud auth login && gcloud auth application-default login"
    info "Then re-run this script."
    exit 1
  fi

  echo
  GCP_PROJECT=$(ask "GCP project ID" "$(gcloud config get-value project 2>/dev/null || echo '')")
  [ -z "$GCP_PROJECT" ] && fatal "GCP project ID is required."
  GCP_REGION=$(ask "GCP region" "us-central1")
  GCP_ZONE=$(ask "GCP zone" "${GCP_REGION}-a")

  echo
  printf "Machine sizing guide:\n"
  printf "  n2-standard-4  (4 vCPU,  16 GB) — dev / small datasets\n"
  printf "  n2-standard-8  (8 vCPU,  32 GB) — small prod, 1 Trino worker\n"
  printf "  n2-standard-16 (16 vCPU, 64 GB) — medium prod, 2 Trino workers\n"
  printf "  n2-standard-32 (32 vCPU,128 GB) — large prod, 4 Trino workers\n"
  echo
  MACHINE_TYPE=$(ask "Machine type" "n2-standard-8")
  DISK_GB=$(ask "Boot disk size (GB per node)" "100")
  RAM_BUDGET_GB=$(ask "RAM budget GB (0 = all available RAM)" "0")

  echo
  info "The Provisa AppImage must be uploaded to GCS before deployment."
  GCS_BUCKET=$(ask "GCS bucket name")
  [ -z "$GCS_BUCKET" ] && fatal "GCS bucket is required."
  GCS_OBJECT=$(ask "Object path" "releases/Provisa.AppImage")

  info "Verifying gs://${GCS_BUCKET}/${GCS_OBJECT}..."
  gsutil stat "gs://${GCS_BUCKET}/${GCS_OBJECT}" &>/dev/null \
    || fatal "Object not found: gs://${GCS_BUCKET}/${GCS_OBJECT}"
  ok "AppImage found."

  echo
  SSH_ENABLED=$(pick "Enable SSH access to nodes?" "Yes" "No")
  SSH_PUBLIC_KEY=""
  ADMIN_CIDR=""
  if [ "$SSH_ENABLED" = "Yes" ]; then
    ADMIN_USERNAME=$(ask "SSH username" "provisa")
    SSH_PUBLIC_KEY_FILE=$(ask "Path to SSH public key file" "${HOME}/.ssh/id_rsa.pub")
    [ -f "$SSH_PUBLIC_KEY_FILE" ] || fatal "File not found: ${SSH_PUBLIC_KEY_FILE}"
    SSH_PUBLIC_KEY="${ADMIN_USERNAME}:$(cat "$SSH_PUBLIC_KEY_FILE")"
    ADMIN_CIDR=$(ask "CIDR allowed SSH access (e.g. 203.0.113.0/24)")
  fi

  NETWORK_CIDR=$(ask "Subnet CIDR" "10.0.0.0/16")

  echo
  printf "${BOLD}Deployment summary${NC}\n"
  printf "══════════════════════════════════════════\n"
  printf "  Provider:     %s\n" "$PROVIDER"
  printf "  Project:      %s\n" "$GCP_PROJECT"
  printf "  Region/Zone:  %s / %s\n" "$GCP_REGION" "$GCP_ZONE"
  printf "  Nodes:        %s\n" "$NODE_COUNT"
  printf "  Machine type: %s\n" "$MACHINE_TYPE"
  printf "  Disk:         %s GB/node\n" "$DISK_GB"
  printf "  RAM budget:   %s GB (0=all)\n" "$RAM_BUDGET_GB"
  printf "  AppImage:     gs://%s/%s\n" "$GCS_BUCKET" "$GCS_OBJECT"
  printf "  SSH:          %s\n" "${ADMIN_CIDR:-disabled}"
  printf "  Subnet CIDR:  %s\n" "$NETWORK_CIDR"
  echo

  CONFIRM=$(pick "Proceed?" "Yes — deploy" "No — abort")
  [ "$CONFIRM" = "No — abort" ] && { info "Aborted."; exit 0; }

  TFVARS="${TERRAFORM_DIR}/terraform.tfvars"
  cat > "$TFVARS" <<EOF
project        = "${GCP_PROJECT}"
region         = "${GCP_REGION}"
zone           = "${GCP_ZONE}"
node_count     = ${NODE_COUNT}
machine_type   = "${MACHINE_TYPE}"
disk_gb        = ${DISK_GB}
ram_budget_gb  = ${RAM_BUDGET_GB}
gcs_bucket     = "${GCS_BUCKET}"
gcs_object     = "${GCS_OBJECT}"
ssh_public_key = "${SSH_PUBLIC_KEY}"
admin_cidr     = "${ADMIN_CIDR}"
network_cidr   = "${NETWORK_CIDR}"
EOF
  ;;

esac

ok "Wrote ${TFVARS}"

# ── Terraform ──────────────────────────────────────────────────────────────────

cd "$TERRAFORM_DIR"

info "Running terraform init..."
terraform init -input=false

info "Running terraform apply..."
terraform apply -var-file="terraform.tfvars" -auto-approve

echo
ok "Deployment complete."
terraform output
