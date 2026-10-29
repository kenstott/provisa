# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1d84-9c25-4b70-a1e3-58c0d7e2b419
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bring the SQL Warehouse the Databricks suites target up before anything connects to it.

A serverless warehouse auto-suspends, and the Thrift ``OpenSession`` a stopped one receives is
rejected outright — ``400 BAD_REQUEST: Cannot create the resource, please try again later``,
marked non-retryable, so the connector gives up on its first attempt. That surfaces as every
Databricks test erroring at once with a message that reads like broken credentials or a broken
workspace, when the warehouse is merely asleep. The REST start endpoint is the wake-up the
connector will not perform: call it, wait for RUNNING, then let the suite connect.

The start endpoint itself can reject the very first wake call the same way, while the workspace
gatekeeper is still settling the warehouse out of idle (``DENY_NEW_AND_EXISTING_RESOURCES`` /
``denyReason: INACTIVE``, same "try again later" wording). That is the gatekeeper asking for a
retry, not a refusal, so the start call is retried on 4xx until it either succeeds or the
timeout budget below is exhausted.

A running warehouse bills. A test that starts it stops it again in teardown, however the test
ends (REQ-254): ``warehouse_running()`` reads the state first, starts it, and stops it after only
if it was not already running.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Generator
from contextlib import contextmanager

import httpx

_START_TIMEOUT_S = 600.0
_STOP_TIMEOUT_S = 600.0
_POLL_S = 10.0
# States in which the warehouse was started by someone other than the test about to use it.
_ALREADY_RUNNING = ("RUNNING", "STARTING")


def _warehouse_id() -> str:
    """The warehouse id embedded in DATABRICKS_HTTP_PATH (``/sql/1.0/warehouses/<id>``)."""
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    match = re.search(r"/warehouses/([0-9a-fA-F]+)", http_path)
    if match is None:
        raise ValueError(
            f"DATABRICKS_HTTP_PATH does not name a SQL Warehouse: {http_path!r} — expected "
            "'/sql/1.0/warehouses/<id>'"
        )
    return match.group(1)


def _client() -> httpx.Client:
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    # The REST client honours the same CA trust the connector does; a proxy that intercepts one
    # intercepts the other. verify=False only when TLS verification is explicitly disabled.
    verify: bool | str = True
    if os.environ.get("DATABRICKS_TLS_NO_VERIFY") == "1":
        verify = False
    elif ca_file := (
        os.environ.get("DATABRICKS_TLS_CA_FILE")
        or os.environ.get("REQUESTS_CA_BUNDLE")
        or os.environ.get("SSL_CERT_FILE")
    ):
        verify = ca_file
    return httpx.Client(headers=headers, verify=verify, timeout=60.0)


def _base() -> str:
    return f"https://{os.environ['DATABRICKS_SERVER_HOSTNAME']}/api/2.0/sql/warehouses"


def _state(client: httpx.Client, warehouse_id: str) -> str:
    return client.get(f"{_base()}/{warehouse_id}").raise_for_status().json()["state"]


def warehouse_state() -> str:
    """The configured warehouse's state as Databricks reports it (RUNNING, STOPPED, ...)."""
    with _client() as client:
        return _state(client, _warehouse_id())


def ensure_warehouse_running() -> None:
    """Start the configured warehouse if it is not already serving, and block until it is."""
    warehouse_id = _warehouse_id()
    base = _base()

    with _client() as client:
        state = _state(client, warehouse_id)
        if state == "RUNNING":
            return

        deadline = time.monotonic() + _START_TIMEOUT_S

        if state != "STARTING":
            while True:
                resp = client.post(f"{base}/{warehouse_id}/start")
                if resp.status_code < 400:
                    break
                if resp.status_code >= 500 or time.monotonic() >= deadline:
                    resp.raise_for_status()
                time.sleep(_POLL_S)

        while time.monotonic() < deadline:
            body = client.get(f"{base}/{warehouse_id}").raise_for_status().json()
            state = body["state"]
            if state == "RUNNING":
                return
            if state in ("STOPPED", "DELETED", "DELETING"):
                raise RuntimeError(
                    f"Databricks warehouse {warehouse_id} settled at {state} after a start "
                    f"request: {body.get('health', {})}"
                )
            time.sleep(_POLL_S)

    raise RuntimeError(
        f"Databricks warehouse {warehouse_id} did not reach RUNNING within "
        f"{_START_TIMEOUT_S:.0f}s (last state {state})"
    )


def stop_warehouse() -> None:
    """Stop the configured warehouse and block until Databricks reports it STOPPED. A warehouse
    still running when the bound passes may be billing, and that raises.

    The stop is sent once. A warehouse that was STOPPING and then runs again was restarted by a
    query from someone else -- another run on the same warehouse -- and that raises at once,
    naming it: stopping it again would fight that user, and waiting would only reach the bound.
    CI keeps runs off each other's warehouse (the live-cloud-warehouses concurrency group), so
    this is a user outside it, and the warehouse is left for a person to check."""
    warehouse_id = _warehouse_id()
    with _client() as client:
        state = _state(client, warehouse_id)
        deadline = time.monotonic() + _STOP_TIMEOUT_S
        stop_sent = False
        seen_stopping = False
        while state != "STOPPED":
            if state in ("DELETED", "DELETING"):
                raise RuntimeError(f"Databricks warehouse {warehouse_id} is {state}, not STOPPED")
            if state == "STOPPING":
                seen_stopping = True
            if state in ("STARTING", "RUNNING") and seen_stopping:
                raise RuntimeError(
                    f"Databricks warehouse {warehouse_id} is {state} again after it was stopping: "
                    "something else restarted it with a query (another run on the same "
                    "warehouse?), not STOPPED: it may be billing"
                )
            if state in ("STARTING", "RUNNING") and not stop_sent:
                client.post(f"{_base()}/{warehouse_id}/stop").raise_for_status()
                stop_sent = True
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Databricks warehouse {warehouse_id} is {state} after "
                    f"{_STOP_TIMEOUT_S:.0f}s, not STOPPED: it may be billing"
                )
            time.sleep(_POLL_S)
            state = _state(client, warehouse_id)


@contextmanager
def warehouse_running() -> Generator[None, None, None]:
    """The configured warehouse RUNNING for the block, and stopped again after it however the
    block ends (REQ-254: a test stops the compute it starts) -- if the block started it.

    The state is read before anything else: a warehouse already RUNNING or STARTING was started
    by someone else (another session, a dashboard, a scheduled job), and stopping it would take
    it from under them, so it is left as it was found (REQ-254 covers only what the test
    started). The start is inside the guarded block, so a start that fails or times out still
    stops whatever it set going."""
    found = warehouse_state()
    try:
        ensure_warehouse_running()
        yield
    finally:
        if found in _ALREADY_RUNNING:
            # REQ-254: the test did not start it; leave it running as it was found.
            print(f"Databricks warehouse was {found} before the test: left running", flush=True)
        else:
            stop_warehouse()
