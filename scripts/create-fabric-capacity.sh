#!/usr/bin/env bash
# One-time creation of the Microsoft Fabric capacity tests/integration/fabric_capacity.py's
# ensure_capacity_resumed()/suspend_capacity() resume/suspend before/after fabric e2e runs.
#
# This is a REAL, BILLABLE Azure resource (F-SKU capacity) — not something the test suite
# creates on its own (see fabric_capacity.py's own docstring for why). Run this once, by hand,
# then add the two env vars it prints to .env. The suite only ever resumes/suspends it after
# that; it never creates or deletes it.
#
# Usage:
#   ./scripts/create-fabric-capacity.sh <resource-group> <capacity-name> <location> [admin-upn-or-object-id]
#
# Example:
#   ./scripts/create-fabric-capacity.sh provisa-fabric-rg provisae2ecapacity eastus you@yourdomain.com
#
# Requires: az CLI logged in (`az login`) against the target subscription, and the `fabric`
# extension (installed automatically below if missing).

set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <resource-group> <capacity-name> <location> [admin-upn-or-object-id]" >&2
  echo "  capacity-name: lowercase alphanumeric, must start with a letter, 3-63 chars" >&2
  exit 1
fi

RESOURCE_GROUP="$1"
CAPACITY_NAME="$2"
LOCATION="$3"
# Defaults to the currently logged-in az account's UPN if no admin is given explicitly.
ADMIN="${4:-$(az account show --query user.name -o tsv)}"

echo "Subscription: $(az account show --query name -o tsv) ($(az account show --query id -o tsv))"
echo "Resource group: $RESOURCE_GROUP"
echo "Capacity name:  $CAPACITY_NAME"
echo "Location:       $LOCATION"
echo "Admin member:   $ADMIN"
echo

az extension add --name fabric --upgrade -y >/dev/null 2>&1 || true

if ! az group show --name "$RESOURCE_GROUP" >/dev/null 2>&1; then
  echo "Resource group '$RESOURCE_GROUP' does not exist — creating it in $LOCATION..."
  az group create --name "$RESOURCE_GROUP" --location "$LOCATION" >/dev/null
fi

echo "Creating Fabric capacity (SKU F2)... this can take a few minutes."
# --administration and --sku take shorthand-syntax JSON objects, not bare strings — confirmed
# against the official `az fabric capacity create` reference (learn.microsoft.com/en-us/cli/
# azure/fabric/capacity), whose own example is:
#   az fabric capacity create --resource-group TestRG --capacity-name azsdktest \
#     --administration "{members:[azsdktest@microsoft.com]}" --sku "{name:F2,tier:Fabric}" \
#     --location westcentralus
# There is no --admin-members flag.
az fabric capacity create \
  --resource-group "$RESOURCE_GROUP" \
  --capacity-name "$CAPACITY_NAME" \
  --location "$LOCATION" \
  --sku "{name:F2,tier:Fabric}" \
  --administration "{members:[$ADMIN]}"

echo
echo "Waiting for capacity to reach Active state..."
az fabric capacity wait \
  --resource-group "$RESOURCE_GROUP" \
  --capacity-name "$CAPACITY_NAME" \
  --custom "properties.state=='Active'" \
  --timeout 600

echo
echo "Done. Add these to .env:"
echo "  FABRIC_RESOURCE_GROUP=$RESOURCE_GROUP"
echo "  FABRIC_CAPACITY_NAME=$CAPACITY_NAME"
echo
echo "The fabric e2e test (provisa-ui/e2e/source-to-query-cloud-warehouse.spec.ts) will pick"
echo "these up automatically and resume/suspend this capacity around each run — it will not"
echo "create or delete it. To pause it yourself right now (avoid idle billing):"
echo "  az fabric capacity suspend --resource-group $RESOURCE_GROUP --capacity-name $CAPACITY_NAME"
