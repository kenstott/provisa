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

The capacity is the run's own (maintainer 2026-11-10, "Paid cloud resources"): ``create_run_capacity``
makes it, tagged with the run, and assigns the lane's workspace (FABRIC_WORKSPACE_ID) to it;
``delete_run_capacity`` unassigns the workspace, deletes the capacity and reads it back gone. A lane
calls them as ``scripts/fabric_e2e.py up`` before its tests and ``down`` after them, whatever
happened (integration-suite-lanes.yml warehouse job; scripts/ci/lanes/ui-core-prepare.sh and
ui-core-after.sh for the warehouse UI job). Locally, the same two commands, with ``az login`` or the
CI principal's AZURE_* in the environment.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from provisa.federation import fabric_capacity as _product

# States in which the capacity was brought up by someone other than the test about to use it.
_ALREADY_RUNNING = ("Active", "Resuming")


def _require_configured() -> None:
    if not _product.capacity_configured():
        raise RuntimeError(
            "FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME are not set — the fabric lane resumes "
            "its capacity before connecting (see this module's docstring)"
        )


def capacity_state() -> str:
    """The lane's capacity state as ARM reports it (Active, Paused, Resuming, ...)."""
    _require_configured()
    with _product._client() as client:
        return _product._locate(client)[1]


@contextmanager
def capacity_resumed() -> Generator[None, None, None]:
    """The lane's capacity Active for the block, and paused again after it however the block ends
    (REQ-254: a test pauses the compute it resumes) -- if the block resumed it.

    Wrap everything that can resume the capacity: the Fabric engine resumes it itself on connect
    (REQ-1775), so constructing an ``MssqlWarehouseRuntime`` belongs inside the block too.

    The state is read before anything else: a capacity already Active or Resuming was brought up
    by someone else (another lane, an operator), and pausing it would take it from under them, so
    it is left as it was found (REQ-254 covers only what the test resumed). The lane's own
    always-run step (integration-suite-lanes.yml, warehouse job) pauses it whatever happened."""
    found = capacity_state()
    try:
        ensure_capacity_resumed()
        yield
    finally:
        if found in _ALREADY_RUNNING:
            # REQ-254: the test did not resume it; leave it running as it was found.
            print(f"Fabric capacity was {found} before the test: left running", flush=True)
        else:
            ensure_capacity_paused()


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
    """Pause the lane's capacity on Fabric test teardown so it stops billing. Suspended only from
    Active: a capacity still settling from the resume is waited out first, one already paused is
    left so (``ensure_capacity_paused``). A failed pause leaves billable compute running, so it
    fails the teardown loudly rather than being logged away."""
    ensure_capacity_paused()


_PAUSED_STATES = ("Paused", "Suspended")
# Fabric's own pause of a capacity has run past ten minutes (run 40115205350: still "Pausing"
# after 600 s, "Paused" soon after); twice that before the lane says it may be billing.
_PAUSE_TIMEOUT_S = 1200.0
_PAUSE_POLL_S = 10.0


def ensure_capacity_paused(*, sleep=None, clock=None) -> str:
    """Leave the lane's capacity PAUSED, whatever state it is found in, and return that state.

    A lane's always-run cleanup cannot assume the capacity is Active: the spec that resumed it
    may already have paused it, or it may be between states. A bare suspend is refused (400) in
    every state but Active -- which says nothing about whether compute is billing (run
    38050265191 reported "NOT paused" on such a refusal without knowing). So the state is read:
    paused is done; Active is suspended; anything else is waited out. Raises if the capacity is
    not paused within the bound -- then it may be billing, and the caller must say so.

    The state found, then each change, is printed with the seconds since the first read: the lane's
    log is where how long a pause takes is read from (the bound is sized from it)."""
    import time

    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    if not _product.capacity_configured():
        raise RuntimeError(
            "FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME are not set — the fabric lane cannot pause "
            "its capacity (see this module's docstring)"
        )
    with _product._client() as client:
        url, state = _product._locate(client)
        start = clock()
        deadline = start + _PAUSE_TIMEOUT_S
        print(f"Fabric capacity: {state} at 0s", flush=True)
        suspend_sent = False
        while True:
            if state in _PAUSED_STATES:
                return state
            if state == "Active" and not suspend_sent:
                client.post(
                    f"{url}/suspend",
                    params={"api-version": _product._API_VERSION},
                    content=b"",
                    headers={"Content-Length": "0"},
                ).raise_for_status()
                suspend_sent = True
            if clock() >= deadline:
                raise RuntimeError(
                    f"Fabric capacity is {state!r} after {_PAUSE_TIMEOUT_S:.0f}s, not paused: "
                    "it may be billing"
                )
            sleep(_PAUSE_POLL_S)
            was, state = state, _product._state(client, url)
            if state != was:
                print(f"Fabric capacity: {was} -> {state} at {clock() - start:.0f}s", flush=True)


# ------------------------------------------------------------------ the run's own capacity
#
# Maintainer 2026-11-10 ("Paid cloud resources"): no test relies on a standing paid resource. A
# Fabric capacity bills only while Active, but a standing one is still a standing paid resource,
# and the cloud audit deletes every untagged one in the test resource group. So each run that
# needs Fabric creates the capacity (named FABRIC_CAPACITY_NAME, in FABRIC_RESOURCE_GROUP), tagged
# with the run, assigns the lane's workspace (FABRIC_WORKSPACE_ID) to it, and on the way out
# unassigns the workspace, deletes the capacity and reads it back gone. The workspace and its
# warehouse are Fabric items, not Azure resources: without a capacity they bill nothing, and are
# reachable again once a capacity of the same region is assigned. The live lanes run one at a
# time (the live-cloud-warehouses concurrency group), so the one name never has two runs.

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
# The region of the Fabric tenant's workspace, which a capacity it is assigned to must share: the
# standing capacity provisatest2's (Central US), read 2026-11-10.
CAPACITY_LOCATION = "centralus"
CAPACITY_SKU = {"name": "F2", "tier": "Fabric"}
_LIFECYCLE_TIMEOUT_S = 1800.0
_LIFECYCLE_POLL_S = 10.0


def _wait_for(what: str, done, *, sleep=None, clock=None) -> None:
    import time

    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    deadline = clock() + _LIFECYCLE_TIMEOUT_S
    while not done():
        if clock() >= deadline:
            raise RuntimeError(f"{what}: not done after {_LIFECYCLE_TIMEOUT_S:.0f}s")
        sleep(_LIFECYCLE_POLL_S)


def _capacity_url(client) -> str:
    """The ARM url the capacity has, in whichever subscription holds the test resource group."""
    import os

    group = os.environ["FABRIC_RESOURCE_GROUP"]
    name = os.environ["FABRIC_CAPACITY_NAME"]
    subs = client.get(
        f"{_product._ARM}/subscriptions",
        params={"api-version": _product._SUBSCRIPTIONS_API_VERSION},
    ).raise_for_status()
    for sub in subs.json().get("value", []):
        group_url = f"{_product._ARM}/subscriptions/{sub['subscriptionId']}/resourceGroups/{group}"
        got = client.get(group_url, params={"api-version": "2021-04-01"})
        if got.status_code == 200:
            return f"{group_url}/providers/Microsoft.Fabric/capacities/{name}"
        if got.status_code not in (403, 404):
            got.raise_for_status()
    raise RuntimeError(f"resource group {group!r} is in no subscription the credential can read")


def _principal_object_id(token: str) -> str:
    """The object id of the principal a token was issued to (its ``oid`` claim)."""
    import base64
    import json

    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return claims["oid"]


def _fabric_client():
    import httpx
    from azure.identity import DefaultAzureCredential

    token = DefaultAzureCredential().get_token(_FABRIC_SCOPE).token
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=60.0)


def _fabric_capacity_id(fabric, name: str) -> str | None:
    listed = fabric.get(f"{_FABRIC_API}/capacities").raise_for_status().json()
    ids = [c["id"] for c in listed.get("value", []) if c.get("displayName") == name]
    return ids[0] if ids else None


def _workspace(fabric, workspace_id: str) -> dict:
    return fabric.get(f"{_FABRIC_API}/workspaces/{workspace_id}").raise_for_status().json()


def create_run_capacity() -> None:
    """Create the run's capacity (tagged with the run), wait for it Active, and assign the lane's
    workspace to it. A capacity of that name already there is another run's or a leaked one: the
    run fails, naming it, rather than using or deleting it (the cloud audit deletes one whose run
    is over)."""
    import os

    from azure.identity import DefaultAzureCredential

    from tests.integration.cloud_run_tag import run_tags

    _require_configured()
    name = os.environ["FABRIC_CAPACITY_NAME"]
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    arm_token = DefaultAzureCredential().get_token(_product._ARM_SCOPE).token
    with _product._client() as client:
        url = _capacity_url(client)
        params = {"api-version": _product._API_VERSION}
        found = client.get(url, params=params)
        if found.status_code != 404:
            found.raise_for_status()
            raise RuntimeError(
                f"Fabric capacity {name!r} already exists (tags {found.json().get('tags')}): "
                "another run's, or one a run left; the cloud audit deletes one whose run is over"
            )
        made = client.put(
            url,
            params=params,
            json={
                "location": CAPACITY_LOCATION,
                "sku": CAPACITY_SKU,
                "tags": run_tags(),
                "properties": {"administration": {"members": [_principal_object_id(arm_token)]}},
            },
        )
        if made.status_code not in (200, 201, 202):
            raise RuntimeError(
                f"Fabric capacity {name!r} not created: HTTP {made.status_code}: {made.text[:1000]}"
            )

        def active() -> bool:
            body = client.get(url, params=params).raise_for_status().json()
            if body["properties"].get("provisioningState") == "Failed":
                raise RuntimeError(f"Fabric capacity {name!r} provisioning failed")
            return body["properties"]["state"] == _product._ACTIVE

        _wait_for(f"Fabric capacity {name!r} Active", active)
    print(f"Fabric capacity {name}: created for this run, Active", flush=True)
    with _fabric_client() as fabric:
        capacity_id: str | None = None

        def listed() -> bool:
            nonlocal capacity_id
            capacity_id = _fabric_capacity_id(fabric, name)
            return capacity_id is not None

        _wait_for(f"Fabric capacity {name!r} listed by the Fabric API", listed)
        fabric.post(
            f"{_FABRIC_API}/workspaces/{workspace_id}/assignToCapacity",
            json={"capacityId": capacity_id},
        ).raise_for_status()

        def assigned() -> bool:
            ws = _workspace(fabric, workspace_id)
            progress = ws.get("capacityAssignmentProgress")
            if progress == "Failed":
                raise RuntimeError(f"workspace {workspace_id} assignment to {name!r} failed")
            return ws.get("capacityId") == capacity_id and progress == "Completed"

        _wait_for(f"workspace {workspace_id} assigned to {name!r}", assigned)
    print(f"Fabric workspace {workspace_id}: assigned to {name}", flush=True)


def delete_run_capacity() -> None:
    """Unassign the lane's workspace from the run's capacity, delete the capacity and read it back
    gone. Every step is attempted and every failure raised together: a capacity left behind may
    bill. A capacity that is not there (``create_run_capacity`` never got that far) is done."""
    import os

    from tests.integration.live_teardown import run_every

    _require_configured()
    name = os.environ["FABRIC_CAPACITY_NAME"]
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]

    def unassign() -> None:
        with _fabric_client() as fabric:
            capacity_id = _fabric_capacity_id(fabric, name)
            if (
                capacity_id is None
                or _workspace(fabric, workspace_id).get("capacityId") != capacity_id
            ):
                return
            fabric.post(
                f"{_FABRIC_API}/workspaces/{workspace_id}/unassignFromCapacity"
            ).raise_for_status()
            _wait_for(
                f"workspace {workspace_id} unassigned from {name!r}",
                lambda: _workspace(fabric, workspace_id).get("capacityId") != capacity_id,
            )

    def delete() -> None:
        with _product._client() as client:
            url = _capacity_url(client)
            params = {"api-version": _product._API_VERSION}
            deleted = client.delete(url, params=params)
            if deleted.status_code not in (200, 202, 204, 404):
                raise RuntimeError(
                    f"Fabric capacity {name!r} not deleted: HTTP {deleted.status_code}: "
                    f"{deleted.text[:500]}"
                )

            def gone() -> bool:
                got = client.get(url, params=params)
                if got.status_code == 404:
                    return True
                got.raise_for_status()
                return False

            _wait_for(f"Fabric capacity {name!r} deleted", gone)

    run_every(
        [
            (f"unassign workspace {workspace_id} from {name}", unassign),
            (f"delete Fabric capacity {name}", delete),
        ]
    )
    print(f"Fabric capacity {name}: deleted, read back gone", flush=True)
