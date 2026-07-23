#!/usr/bin/env bash
# One-shot Provisa teardown. Picks a provider and destroys everything in that
# provider's Terraform state (VMs, load balancers, IPs, DNS, network, SA).
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

info()  { printf "${CYAN}[destroy]${NC} %s\n" "$*" >&2; }
ok()    { printf "${GREEN}[destroy]${NC} %s\n" "$*" >&2; }
warn()  { printf "${YELLOW}[destroy]${NC} %s\n" "$*" >&2; }
err()   { printf "${RED}[destroy]${NC} %s\n" "$*" >&2; }
fatal() { err "$*"; exit 1; }

pick() {
  local prompt="$1"; shift
  local options=("$@")
  echo "$prompt" >&2
  local i=1
  for opt in "${options[@]}"; do
    printf "  [%d] %s\n" "$i" "$opt" >&2
    (( i++ ))
  done
  local choice
  while true; do
    printf "Choice [1-%d]: " "${#options[@]}" >&2
    read -r choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#options[@]} )); then
      echo "${options[$((choice-1))]}"
      return
    fi
    warn "Invalid choice."
  done
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

printf "\n${BOLD}Provisa Cloud Teardown${NC}\n"
printf "══════════════════════════════════════════\n\n"

PROVIDER=$(pick "Cloud provider:" "AWS" "Azure" "GCP")

case "$PROVIDER" in
  AWS)   TERRAFORM_DIR="${SCRIPT_DIR}/aws" ;;
  Azure) TERRAFORM_DIR="${SCRIPT_DIR}/azure" ;;
  GCP)   TERRAFORM_DIR="${SCRIPT_DIR}/gcp" ;;
esac

[ -f "${TERRAFORM_DIR}/terraform.tfstate" ] \
  || warn "No terraform.tfstate in ${TERRAFORM_DIR}. Nothing may be provisioned (or state is remote)."

cd "$TERRAFORM_DIR"

echo
printf "${BOLD}Resources to be destroyed (${PROVIDER}):${NC}\n"
terraform plan -destroy -var-file="terraform.tfvars" 2>/dev/null | grep -E '^\s*#|will be destroyed' || true
echo

CONFIRM=$(pick "This permanently destroys all provisioned ${PROVIDER} resources. Proceed?" \
  "No — abort" "Yes — destroy everything")
[ "$CONFIRM" = "No — abort" ] && { info "Aborted."; exit 0; }

info "Running terraform destroy..."
terraform destroy -var-file="terraform.tfvars" -auto-approve

echo
ok "Teardown complete."
