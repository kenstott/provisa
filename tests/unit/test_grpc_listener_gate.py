# Copyright (c) 2026 Kenneth Stott
# Canary: 6d17e40b-8a92-4c53-b7f1-3095e2a4c8bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1210: the gRPC listener only exists once there is a schema to serve.

gRPC is generated from the registered tables. A fresh cluster has none, so there is no descriptor
to compile and nothing to bind — the port stays closed and the NetLB backend stays unhealthy,
which reads like an outage but is the correct state for a deployment with no registered data.

The startup gate is asserted rather than the socket: what matters is that the server is not
started when there is no descriptor, and that the descriptor covers EVERY role rather than
whichever one dict order put first — a per-role descriptor leaves the other roles unservable
while the port looks perfectly healthy.
"""

# Requirements: REQ-045, REQ-143, REQ-1210

from __future__ import annotations

import ast
import types

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def startup(monkeypatch):
    """``_start_servers`` with every server it may start recorded instead of bound."""
    started: list[str] = []

    async def _start_grpc(*args, **kwargs):
        started.append("grpc")
        return types.SimpleNamespace()

    monkeypatch.setattr("provisa.grpc.server.start_grpc_server", _start_grpc)
    monkeypatch.setattr(
        "provisa.grpc.schema_gen.compile_proto",
        lambda proto, out: ("pb2.py", "pb2_grpc.py"),
    )
    return started


def _start_servers_source() -> str:
    return (_REPO_ROOT / "provisa/api/app_startup.py").read_text().split(
        "async def _start_servers"
    )[1]


def test_the_listener_is_gated_on_a_generated_descriptor():
    """The gate is the whole requirement: no registered tables, no proto, no listener."""
    source = _start_servers_source()

    assert "if state.wire_proto:" in source, (
        "the gRPC listener is no longer gated on a generated descriptor — a fresh cluster would "
        "try to compile an empty proto at boot"
    )
    grpc_start = source.index("start_grpc_server")
    gate = source.index("if state.wire_proto:")
    assert gate < grpc_start


def test_the_descriptor_is_the_union_of_every_roles_surface():
    """One grpc.aio service and one reflection pool means one descriptor. Building it from a
    single role makes the served surface depend on dict order, and the omitted roles cannot be
    served at all — with the port healthy and the client's error looking like a missing table."""
    loaders = (_REPO_ROOT / "provisa/api/app_loaders.py").read_text()
    wire = loaders.split("_wire_role = {")[1].split("state.wire_proto")[0]

    assert 'for r in roles' in wire, "the wire descriptor no longer unions every role"
    assert '"domain_access": ["*"]' in wire


def test_governance_is_not_expressed_by_the_descriptor():
    """The union descriptor declares every column, so the guarantee has to come from the RPC
    path: each query projects through the caller's own context and is governed there."""
    server = (_REPO_ROOT / "provisa/grpc/server.py").read_text()

    assert "_govern_and_route_compiled" in server
    tree = ast.parse(server)
    handlers = [
        n.name
        for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name.startswith("_handle_query")
    ]
    assert handlers, "the gRPC query handler was renamed; the governance assertion is stale"


@pytest.mark.asyncio
async def test_a_cluster_with_no_registered_tables_starts_no_grpc_listener(startup, monkeypatch):
    from provisa.api.app_startup import _start_servers
    import logging

    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(wire_proto=None, server_cfg={}, proto_files={}),
        raising=False,
    )
    # Every other server in _start_servers is best-effort and logs on failure; the assertion is
    # only about gRPC, so the rest are allowed to fail against the bare state.
    await _start_servers(logging.getLogger("test"))

    assert startup == []
