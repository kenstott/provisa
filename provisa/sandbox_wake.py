# Copyright (c) 2026 Kenneth Stott
# Canary: cfcaebe4-dd78-4ef7-8598-8479a1bd2d4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Starting the sandbox tier's VM (REQ-2006).

Where the sandbox tier runs on a VM of its own, that VM powers itself off when no demo sandbox
has been used for a while, and the UI server starts it again when a request for the tier finds
it down: the VM's Compute Engine instance is started through the API with this node's own
service account (the metadata server's token). The visitor is answered "starting" meanwhile."""

from __future__ import annotations

import os
import threading
import time

import httpx

#: The sandbox tier's Compute Engine instance, ``projects/<p>/zones/<z>/instances/<name>``.
#: Unset where the tier runs on this node, which then has nothing to start.
SANDBOX_INSTANCE = os.environ.get("PROVISA_SANDBOX_INSTANCE")

_METADATA_TOKEN = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)
_COMPUTE = "https://compute.googleapis.com/compute/v1/"
#: Instance states a start brings back (SUSPENDED is resumed instead).
_STOPPED = frozenset({"TERMINATED", "STOPPED"})
#: One start call at a time, and none again within this many seconds of the last.
_DEBOUNCE_S = 30.0

_claim = threading.Lock()
_last_call = float("-inf")


async def wake(instance: str) -> str:
    """Start ``instance`` if it is stopped (resume it if suspended) and return the state it was
    found in. A failed call to the metadata server or the Compute API raises."""
    async with _client() as client:
        token = (
            await _ok(
                client.get(_METADATA_TOKEN, headers={"Metadata-Flavor": "Google"}),
                "the metadata server's token",
            )
        ).json()["access_token"]
        auth = {"Authorization": f"Bearer {token}"}
        found = await _ok(client.get(_COMPUTE + instance, headers=auth), instance)
        status = found.json()["status"]
        verb = "start" if status in _STOPPED else "resume" if status == "SUSPENDED" else None
        if verb is None or not _claimed():
            return status
        await _ok(client.post(f"{_COMPUTE}{instance}/{verb}", headers=auth), f"{verb} {instance}")
        return status


def _claimed() -> bool:
    """Whether this caller makes the start call: none was made within ``_DEBOUNCE_S``. Every
    request that finds the VM down asks; one of them starts it."""
    global _last_call
    with _claim:
        now = time.monotonic()
        if now - _last_call < _DEBOUNCE_S:
            return False
        _last_call = now
        return True


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=10)


async def _ok(sent, what: str) -> httpx.Response:
    response = await sent
    if response.status_code >= 400:
        raise RuntimeError(f"{what}: {response.status_code} {response.text[:300]}")
    return response
