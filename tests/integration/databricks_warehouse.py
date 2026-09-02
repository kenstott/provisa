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
"""

from __future__ import annotations

import os
import re
import time

import httpx

_START_TIMEOUT_S = 600.0
_POLL_S = 10.0


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


def ensure_warehouse_running() -> None:
    """Start the configured warehouse if it is not already serving, and block until it is."""
    warehouse_id = _warehouse_id()
    base = f"https://{os.environ['DATABRICKS_SERVER_HOSTNAME']}/api/2.0/sql/warehouses"
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

    with httpx.Client(headers=headers, verify=verify, timeout=60.0) as client:
        state = client.get(f"{base}/{warehouse_id}").raise_for_status().json()["state"]
        if state == "RUNNING":
            return
        if state != "STARTING":
            client.post(f"{base}/{warehouse_id}/start").raise_for_status()

        deadline = time.monotonic() + _START_TIMEOUT_S
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
