# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b9f4a-1d7c-4e83-9a5f-3c8d0b6e4f21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bring the Fabric capacity the fabric e2e/integration suites target up before anything connects.

A Microsoft Fabric capacity (F-SKU) is a paused-by-default, billable compute unit: a SQL warehouse
built on top of it (``MssqlWarehouseRuntime`` / ``mssql_warehouse.py``) will not accept a connection
while the capacity backing it is ``Paused``/``Suspended`` — the ODBC connect fails with a driver-level
"system update" / login rejection (18456) that reads like broken credentials or a broken workspace,
when the capacity is merely paused. The resume itself now lives in the product (provisa/federation/fabric_capacity.py, REQ-1775) —
the Fabric engine resumes its own capacity before connecting. This module keeps the lane API: call ``ensure_capacity_resumed()`` before connecting, then
``suspend_capacity()`` on teardown to avoid leaving billable compute running between test runs.

This is a **control-plane** (ARM) operation — a different Azure AD scope
(``https://management.azure.com/.default``) from ``fabric_shortcuts.py``'s data-plane Fabric API
scope (``https://api.fabric.microsoft.com/.default``); the same ``DefaultAzureCredential`` (``az
login`` / managed identity) acquires both, just against different resources.

The subscription id is resolved from the active ``az login`` session (``az account show``), the same
credential source and lookup ``tests/integration/synapse_provision.py`` already uses for its own
ARM calls — no separate subscription-id env var. ``FABRIC_RESOURCE_GROUP`` and ``FABRIC_CAPACITY_NAME``
name the specific capacity resource and have no equivalent elsewhere in .env (``FABRIC_SQL_SERVER``/
``FABRIC_DATABASE``/``FABRIC_WORKSPACE_ID`` name the SQL warehouse and workspace, not the ARM capacity
resource, and cannot be derived from them). Per this project's CLAUDE.md ("never add fallback
values"), a missing env var raises rather than silently no-op.

One-time capacity creation (not performed by this module — a real-money Azure resource decision left
to whoever owns the subscription):

    az extension add --name fabric  # if not already installed
    az fabric capacity create \\
        --resource-group <FABRIC_RESOURCE_GROUP> \\
        --capacity-name <FABRIC_CAPACITY_NAME (lowercase alphanumeric, starts with a letter, 3-63 chars)> \\
        --location <azure-region, e.g. eastus> \\
        --sku "{name:F2,tier:Fabric}" \\
        --administration "{members:[<your-aad-object-id-or-upn>]}"

    Or use scripts/create-fabric-capacity.sh, which wraps the above and also creates the resource
    group and polls to Active. Note the resource group's region must have nonzero Fabric CU quota
    (az rest --method get --url ".../providers/Microsoft.Fabric/locations/<region>/usages?api-
    version=2023-11-01") and the capacity's region must match the Fabric tenant's workspace region
    or workspace-to-capacity assignment fails — check with an existing capacity/workspace's region
    in the Fabric portal before picking one.

After creation, set FABRIC_RESOURCE_GROUP / FABRIC_CAPACITY_NAME in .env (with `az login` active
for the target subscription) and the fabric e2e/integration lanes will resume it automatically
before connecting.
"""

from __future__ import annotations

from provisa.federation import fabric_capacity as _product


def ensure_capacity_resumed() -> None:
    """Resume the lane's capacity via the product's own resume (REQ-1775) — the lane requires it
    to be configured, so an unset capacity is a lane misconfiguration, not "managed externally"."""
    if not _product.capacity_configured():
        raise RuntimeError(
            "FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME are not set — the fabric lane resumes "
            "its capacity before connecting (see this module's docstring)"
        )
    _product.ensure_capacity_resumed()


def suspend_capacity() -> None:
    """Pause the lane's capacity on Fabric test teardown so it stops billing. A failed pause leaves
    billable compute running, so it fails the teardown loudly rather than being logged away."""
    if not _product.capacity_configured():
        raise RuntimeError(
            "FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME are not set — the fabric lane cannot pause "
            "its capacity on teardown (see this module's docstring)"
        )
    _product.suspend_capacity()
