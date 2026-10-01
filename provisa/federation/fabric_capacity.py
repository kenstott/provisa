# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9f5b7e-8a4d-4f13-b6e2-1d7a3c9e0f48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resume the Microsoft Fabric capacity backing the Fabric engine before it connects.

A Fabric capacity (F-SKU) is billable compute that operators pause between uses. A SQL warehouse on
a Paused/Suspended capacity refuses the ODBC login outright (an 18456-style rejection that reads like
broken credentials). The Fabric engine therefore resumes its capacity through the ARM control plane
before connecting, and waits until it is Active.

Opt-in by configuration: ``FABRIC_CAPACITY_NAME`` + ``FABRIC_RESOURCE_GROUP`` name the capacity.
A deployment that leaves both unset manages its capacity itself and this module does nothing; setting
only one of them is a misconfiguration and raises. The subscription is found by listing the
subscriptions the engine's credential can see and locating the named capacity — no subscription id
setting and no ``az`` CLI dependency, so it works with a managed identity in a container. The same
``DefaultAzureCredential`` the engine already authenticates with acquires the ARM token.
"""

# Requirements: REQ-1775

from __future__ import annotations

import logging
import os
import time

import httpx

log = logging.getLogger(__name__)

_ARM = "https://management.azure.com"
_ARM_SCOPE = f"{_ARM}/.default"
_API_VERSION = "2023-11-01"
_SUBSCRIPTIONS_API_VERSION = "2022-12-01"
_RESUME_TIMEOUT_S = 600.0
_POLL_S = 10.0

_ACTIVE = "Active"
_RESUMABLE_STATES = ("Paused", "Suspended")
_TERMINAL_FAILURE_STATES = ("Failed", "Deleting")


def capacity_configured() -> bool:
    """Whether this deployment asks Provisa to manage its Fabric capacity."""
    name = os.environ.get("FABRIC_CAPACITY_NAME")
    group = os.environ.get("FABRIC_RESOURCE_GROUP")
    if bool(name) != bool(group):
        raise RuntimeError(
            "FABRIC_CAPACITY_NAME and FABRIC_RESOURCE_GROUP must be set together (both name the "
            "capacity Provisa resumes) or both left unset (the capacity is managed externally)"
        )
    return bool(name)


def _client() -> httpx.Client:
    from azure.identity import DefaultAzureCredential

    token = DefaultAzureCredential().get_token(_ARM_SCOPE).token
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=60.0)


def _locate(client: httpx.Client) -> tuple[str, str]:
    """``(ARM resource URL, current state)`` of the capacity, in whichever visible subscription
    holds it — the lookup GET already carries the state, so it is not fetched twice."""
    group = os.environ["FABRIC_RESOURCE_GROUP"]
    name = os.environ["FABRIC_CAPACITY_NAME"]
    subs = (
        client.get(f"{_ARM}/subscriptions", params={"api-version": _SUBSCRIPTIONS_API_VERSION})
        .raise_for_status()
        .json()
        .get("value", [])
    )
    for sub in subs:
        url = (
            f"{_ARM}/subscriptions/{sub['subscriptionId']}/resourceGroups/{group}"
            f"/providers/Microsoft.Fabric/capacities/{name}"
        )
        resp = client.get(url, params={"api-version": _API_VERSION})
        if resp.status_code == 200:
            return url, resp.json()["properties"]["state"]
        if resp.status_code != 404:
            resp.raise_for_status()
    raise RuntimeError(
        f"Fabric capacity {name!r} in resource group {group!r} is not visible in any subscription "
        "the engine's Azure credential can read"
    )


def _state(client: httpx.Client, url: str) -> str:
    body = client.get(url, params={"api-version": _API_VERSION}).raise_for_status().json()
    return body["properties"]["state"]


def ensure_capacity_resumed() -> None:
    """Resume the configured capacity if it is not Active, and block until it is (or time out)."""
    with _client() as client:
        url, state = _locate(client)
        if state == _ACTIVE:
            return
        if state in _TERMINAL_FAILURE_STATES:
            raise RuntimeError(f"Fabric capacity is in terminal state {state!r}, cannot resume")
        deadline = time.monotonic() + _RESUME_TIMEOUT_S
        if state in _RESUMABLE_STATES:
            log.info("Fabric capacity is %s — resuming before connecting", state)
            while True:
                # ARM 411s a bodyless POST unless Content-Length is sent explicitly.
                resp = client.post(
                    f"{url}/resume",
                    params={"api-version": _API_VERSION},
                    content=b"",
                    headers={"Content-Length": "0"},
                )
                if resp.status_code < 400:
                    break
                # A capacity still settling out of Suspending rejects resume with a 4xx; retry
                # those until the budget runs out. A 5xx, or the budget exhausted, raises.
                if resp.status_code >= 500 or time.monotonic() >= deadline:
                    resp.raise_for_status()
                time.sleep(_POLL_S)
        # Otherwise already transitioning (Resuming/Provisioning/Updating/…): just wait.
        while time.monotonic() < deadline:
            state = _state(client, url)
            if state == _ACTIVE:
                return
            if state in _TERMINAL_FAILURE_STATES:
                raise RuntimeError(f"Fabric capacity settled at {state!r} after a resume request")
            time.sleep(_POLL_S)
    raise RuntimeError(
        f"Fabric capacity did not reach {_ACTIVE} within {_RESUME_TIMEOUT_S:.0f}s "
        f"(last state {state!r})"
    )


def suspend_capacity() -> None:
    """Suspend the configured capacity (test/ops teardown to stop billing)."""
    with _client() as client:
        url, _state_now = _locate(client)
        client.post(
            f"{url}/suspend",
            params={"api-version": _API_VERSION},
            content=b"",
            headers={"Content-Length": "0"},
        ).raise_for_status()
