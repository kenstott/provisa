# Copyright (c) 2026 Kenneth Stott
# Canary: cfcaebe4-dd78-4ef7-8598-8479a1bd2d4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Starting and stopping the sandbox tier's VM (REQ-2006).

Where the sandbox tier runs on a VM of its own, that VM powers itself off when no demo sandbox
has been used for a while, and the UI server starts it again when a request for the tier finds
it down: the VM's Compute Engine instance is started through the API with this node's own
service account (the metadata server's token). The visitor is answered "starting" meanwhile."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

import httpx

#: The sandbox tier's Compute Engine instance, ``projects/<p>/zones/<z>/instances/<name>``.
#: Unset where the tier runs on this node, which then has nothing to start.
#: The compose passes the variable on every node, empty where there is no such VM.
SANDBOX_INSTANCE = os.environ.get("PROVISA_SANDBOX_INSTANCE") or None

_METADATA_TOKEN = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)
_COMPUTE = "https://compute.googleapis.com/compute/v1/"
#: Instance states a start brings back (SUSPENDED is resumed instead).
_STOPPED = frozenset({"TERMINATED", "STOPPED"})
#: One start call at a time, and none again within this many seconds of the last.
_DEBOUNCE_S = 30.0
#: A VM found STOPPING (its idle stop under way) cannot be started yet: it is looked at again
#: every this many seconds and started once it is down, for at most _STOPPING_LIMIT_S.
_STOPPING_POLL_S = 5.0
_STOPPING_LIMIT_S = 300.0

_log = logging.getLogger(__name__)
_claim = threading.Lock()
_last_call = float("-inf")
#: The one task following a stopping VM to its start; held so the loop does not drop it.
_followers: set[asyncio.Task] = set()


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
        if status == "STOPPING":
            _follow_stop(instance)
            return status
        verb = "start" if status in _STOPPED else "resume" if status == "SUSPENDED" else None
        if verb is None or not _claimed():
            return status
        await _ok(client.post(f"{_COMPUTE}{instance}/{verb}", headers=auth), f"{verb} {instance}")
        return status


async def stop(instance: str) -> None:
    """Stop ``instance`` -- the sandbox tier stopping its own VM when it has been idle
    (provisa.scheduler.jobs.stop_idle_sandbox_vm). A failed call raises."""
    async with _client() as client:
        token = (
            await _ok(
                client.get(_METADATA_TOKEN, headers={"Metadata-Flavor": "Google"}),
                "the metadata server's token",
            )
        ).json()["access_token"]
        await _ok(
            client.post(f"{_COMPUTE}{instance}/stop", headers={"Authorization": f"Bearer {token}"}),
            f"stop {instance}",
        )


def _follow_stop(instance: str) -> None:
    """A request found the VM stopping: start it once it is down, after this request has been
    answered "starting" -- nothing else would, unless another request came. One follower at a
    time; a failure is logged, as the request that asked has long been answered."""
    with _claim:
        if _followers:
            return
        task = asyncio.get_running_loop().create_task(_start_once_stopped(instance))
        _followers.add(task)
    task.add_done_callback(_followed)


async def _start_once_stopped(instance: str) -> None:
    deadline = time.monotonic() + _STOPPING_LIMIT_S
    while True:
        await asyncio.sleep(_STOPPING_POLL_S)
        # wake() starts it once it is down; while it is still stopping it finds this task
        # already following and returns.
        if await wake(instance) != "STOPPING":
            return
        if time.monotonic() > deadline:
            raise RuntimeError(f"{instance} still STOPPING after {_STOPPING_LIMIT_S:.0f}s")


def _followed(task: asyncio.Task) -> None:
    with _claim:
        _followers.discard(task)
    if not task.cancelled() and task.exception() is not None:
        _log.error(
            "the sandbox tier's VM was not started after it stopped", exc_info=task.exception()
        )


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
