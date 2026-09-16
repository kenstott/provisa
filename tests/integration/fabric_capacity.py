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
when the capacity is merely paused. This module is the ARM control-plane resume/suspend the connector
will not perform on its own: call ``ensure_capacity_resumed()`` before connecting, then
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

import logging
import os
import subprocess
import time

import httpx

logger = logging.getLogger(__name__)

_ARM_SCOPE = "https://management.azure.com/.default"
_API_VERSION = "2023-11-01"
_RESUME_TIMEOUT_S = 600.0
_POLL_S = 10.0

_ACTIVE = "Active"
_RESUMABLE_STATES = ("Paused", "Suspended")
_TERMINAL_FAILURE_STATES = ("Failed", "Deleting")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set — a Fabric capacity must exist first (see this module's "
            "docstring for the one-time `az fabric capacity create` command), then "
            "FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME must be set in .env"
        )
    return value


def _subscription_id() -> str:
    """The active `az login` session's subscription — same lookup synapse_provision.py's own
    ARM calls use (_resolve_location's `_az("account", "show", "--query", "id", ...)`). No
    separate subscription-id env var: the az CLI session already carries this."""
    try:
        proc = subprocess.run(
            ["az", "account", "show", "--query", "id", "-o", "tsv"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "az CLI not installed — required to resolve the active subscription id"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(f"az account show failed ({proc.returncode}): {proc.stderr.strip()}")
    subscription_id = proc.stdout.strip()
    if not subscription_id:
        raise RuntimeError("az account show returned no subscription id — is `az login` active?")
    return subscription_id


def _capacity_url() -> str:
    resource_group = _required_env("FABRIC_RESOURCE_GROUP")
    capacity_name = _required_env("FABRIC_CAPACITY_NAME")
    return (
        f"https://management.azure.com/subscriptions/{_subscription_id()}/resourceGroups/"
        f"{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}"
    )


def _client() -> httpx.Client:
    from azure.identity import DefaultAzureCredential

    token = DefaultAzureCredential().get_token(_ARM_SCOPE).token
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=60.0)


def ensure_capacity_resumed() -> None:
    """Resume the configured Fabric capacity if it is not Active, and block until it is."""
    url = _capacity_url()

    with _client() as client:
        state = (
            client.get(url, params={"api-version": _API_VERSION})
            .raise_for_status()
            .json()["properties"]["state"]
        )
        if state == _ACTIVE:
            return

        deadline = time.monotonic() + _RESUME_TIMEOUT_S

        if state in _RESUMABLE_STATES:
            while True:
                # ARM's resume endpoint 411s a bodyless POST unless Content-Length is sent
                # explicitly — httpx omits it when content= isn't passed. Without this, every
                # resume call fails with a permanent 411, which the 4xx-retry logic below
                # would otherwise mistake for "still transitioning" and burn the whole
                # _RESUME_TIMEOUT_S budget on a request that could never succeed.
                resp = client.post(
                    f"{url}/resume",
                    params={"api-version": _API_VERSION},
                    content=b"",
                    headers={"Content-Length": "0"},
                )
                if resp.status_code < 400:
                    break
                # A capacity still settling out of Suspending/Resuming can reject a resume call
                # with a 4xx ("still transitioning") the same way a Databricks warehouse rejects
                # a start call while the workspace gatekeeper is settling it out of idle — retry
                # on 4xx until the timeout budget below is exhausted, per
                # databricks_warehouse.py's ensure_warehouse_running().
                if resp.status_code >= 500 or time.monotonic() >= deadline:
                    resp.raise_for_status()
                time.sleep(_POLL_S)
        elif state in _TERMINAL_FAILURE_STATES:
            raise RuntimeError(f"Fabric capacity is in terminal state {state!r}, cannot resume")
        # else: already mid-transition (Provisioning/Updating/Resuming/Scaling/Preparing) —
        # fall through to the poll loop below without issuing our own resume call.

        while time.monotonic() < deadline:
            body = client.get(url, params={"api-version": _API_VERSION}).raise_for_status().json()
            state = body["properties"]["state"]
            if state == _ACTIVE:
                return
            if state in _TERMINAL_FAILURE_STATES:
                raise RuntimeError(
                    f"Fabric capacity settled at {state} after a resume request: {body.get('properties', {})}"
                )
            time.sleep(_POLL_S)

    raise RuntimeError(
        f"Fabric capacity did not reach {_ACTIVE} within {_RESUME_TIMEOUT_S:.0f}s (last state {state})"
    )


def suspend_capacity() -> None:
    """Best-effort suspend of the configured Fabric capacity on teardown.

    Leaving a capacity briefly running is a cost concern, not a correctness one — unlike
    ``ensure_capacity_resumed()``, a failure here must never fail the calling test, so it is
    logged rather than raised.
    """
    try:
        url = _capacity_url()
        with _client() as client:
            client.post(
                f"{url}/suspend",
                params={"api-version": _API_VERSION},
                content=b"",
                headers={"Content-Length": "0"},
            ).raise_for_status()
    except Exception:
        logger.warning(
            "Fabric capacity suspend failed (best-effort, not test-failing)", exc_info=True
        )
