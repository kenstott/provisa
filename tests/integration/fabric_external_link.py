# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2f71-6c3d-4a95-b0e7-91d2c5a8f364
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Remove what a Fabric external link left in the workspace (REQ-254).

Attaching an S3-compatible source on the Fabric engine (``provisa/federation/fabric_shortcuts.py``)
creates up to three things through the Fabric REST API: an ``AmazonS3Compatible`` connection, the
``provisa_shortcuts`` lakehouse, and a ``Files/<name>`` shortcut. The product names the connection
and the lakehouse itself (no per-run name can be given), so a test reads which of the two exist
before it attaches (``found``) and removes only what it created; the shortcut's name carries the
run's own source id, so it is always the run's and always removed. Each removal is read back.
"""

from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus

from provisa.federation.fabric_shortcuts import _Fabric, connection_name

_LAKEHOUSE = "provisa_shortcuts"  # the name fabric_shortcuts.ensure_lakehouse gives it


@dataclass(frozen=True)
class Found:
    """What existed in the workspace before the test attached: left in place after it."""

    lakehouse: bool
    connection: bool


def _lakehouse_id(fab: _Fabric, workspace_id: str) -> str | None:
    items = fab._listed(f"/workspaces/{workspace_id}/items")
    if items is None:
        raise RuntimeError(f"Fabric refused the item listing of workspace {workspace_id}")
    ids = [
        it["id"]
        for it in items
        if it.get("type") == "Lakehouse" and it.get("displayName") == _LAKEHOUSE
    ]
    return ids[0] if ids else None


def _connection_id(fab: _Fabric, workspace_id: str, endpoint: str) -> str | None:
    connections = fab._listed("/connections")
    if connections is None:
        raise RuntimeError("Fabric refused the connection listing")
    name = connection_name(workspace_id, endpoint)
    ids = [c["id"] for c in connections if c.get("displayName") == name]
    return ids[0] if ids else None


def found(workspace_id: str, endpoint: str) -> Found:
    """Which of the product-named lakehouse and connection exist now, before the test attaches."""
    fab = _Fabric()
    return Found(
        lakehouse=_lakehouse_id(fab, workspace_id) is not None,
        connection=_connection_id(fab, workspace_id, endpoint) is not None,
    )


def _delete(fab: _Fabric, path: str) -> None:
    st, payload = fab._req("DELETE", path)
    if st != HTTPStatus.OK:
        raise RuntimeError(f"Fabric DELETE {path} failed ({st}): {payload}")


def remove_shortcut(workspace_id: str, shortcut: str) -> None:
    """Delete ``Files/<shortcut>`` from the lakehouse, and read back that it is gone. No lakehouse,
    or no such shortcut, means the attach never got as far as creating it."""
    fab = _Fabric()
    lakehouse = _lakehouse_id(fab, workspace_id)
    if lakehouse is None:
        return
    path = f"/workspaces/{workspace_id}/items/{lakehouse}/shortcuts/Files/{shortcut}"
    st, payload = fab._req("GET", path)
    if st == HTTPStatus.OK:
        _delete(fab, path)
        st, payload = fab._req("GET", path)
    if st != HTTPStatus.NOT_FOUND:
        raise RuntimeError(f"Fabric shortcut Files/{shortcut} still answers ({st}): {payload}")


def remove_lakehouse(workspace_id: str, before: Found) -> None:
    """Delete the lakehouse if the test created it, and read back that it is gone."""
    if before.lakehouse:
        return
    fab = _Fabric()
    lakehouse = _lakehouse_id(fab, workspace_id)
    if lakehouse is None:
        return
    _delete(fab, f"/workspaces/{workspace_id}/items/{lakehouse}")
    if _lakehouse_id(fab, workspace_id) is not None:
        raise RuntimeError(f"Fabric lakehouse {_LAKEHOUSE} still listed after its delete")


def remove_connection(workspace_id: str, endpoint: str, before: Found) -> None:
    """Delete the connection if the test created it, and read back that it is gone."""
    if before.connection:
        return
    fab = _Fabric()
    connection = _connection_id(fab, workspace_id, endpoint)
    if connection is None:
        return
    _delete(fab, f"/connections/{connection}")
    if _connection_id(fab, workspace_id, endpoint) is not None:
        raise RuntimeError(
            f"Fabric connection {connection_name(workspace_id, endpoint)} still listed after its delete"
        )
