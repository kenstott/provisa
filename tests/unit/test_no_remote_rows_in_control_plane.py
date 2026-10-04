# Copyright (c) 2026 Kenneth Stott
# Canary: b02c433d-56bc-4bf6-abe4-522d140cc2e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: nothing fetched from a remote API is written into the control plane.

A table's replica is written at the resolver's address in the store; a request's calls to an
API are kept as fills in the store's API cache schema (``api_source.fill_cache``) — a cache of
answers to particular calls, not a replica, which is why it is not at the resolver's address.
The control plane holds the model and the state of replicas, never rows a remote answered.

Every module that calls a remote API on the request or boot path is checked: a connection it
takes from the tenant plane (``state.tenant_db.acquire()``) is only read through.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: The modules that call a remote API while serving a request. (The boot fill of an API
#: table's collection is gone: its replica is built by the runner.)
REMOTE_PATH = [
    "provisa/api/data/hydration.py",
    "provisa/api/data/materialization.py",
    "provisa/nl/executor.py",
    *sorted(str(p.relative_to(ROOT)) for p in (ROOT / "provisa/api_source").glob("*.py")),
]

#: What a tenant-plane connection may be used for there: reads.
READS = {"fetch", "fetchval", "fetchrow", "reflect_columns", "capabilities"}

#: Uses that are not a read, each with why it is not remote rows. (file, function) -> reason.
ALLOWED: dict[tuple[str, str], str] = {
    (
        "provisa/api_source/router_integration.py",
        "_apply_cache_promotions",
    ): "DDL (generated columns) on the API cache table, not rows; recorded for the lead as an "
    "existing defect: it addresses the store through the tenant plane's connection",
}


def _tenant_connections(tree: ast.AST):
    """(function name, connection name, the with-block) for every ``async with
    <...>.tenant_db.acquire() as NAME``."""
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(fn):
            if not isinstance(node, ast.AsyncWith):
                continue
            for item in node.items:
                call = item.context_expr
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "acquire"
                    and isinstance(call.func.value, ast.Attribute)
                    and call.func.value.attr == "tenant_db"
                    and isinstance(item.optional_vars, ast.Name)
                ):
                    yield fn.name, item.optional_vars.id, node


def _non_read_uses(name: str, block: ast.AsyncWith) -> list[str]:
    """How ``name`` is used in ``block`` other than as the receiver of a read."""
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(block):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    found = []
    for node in ast.walk(block):
        if not (isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Load)):
            continue
        parent = parents.get(node)
        if isinstance(parent, ast.Attribute) and parent.attr in READS:
            continue
        what = parent.attr if isinstance(parent, ast.Attribute) else type(parent).__name__
        found.append(f"line {node.lineno}: {what}")
    return found


def test_the_old_control_plane_cache_is_gone_and_nothing_imports_it():
    assert not (ROOT / "provisa/openapi/pg_cache.py").exists()
    importers = [
        str(p.relative_to(ROOT))
        for p in (ROOT / "provisa").rglob("*.py")
        if "openapi.pg_cache" in p.read_text() or "openapi import pg_cache" in p.read_text()
    ]
    assert importers == []


def test_a_tenant_plane_connection_on_the_remote_path_is_only_read_through():
    offenders = []
    seen_allowed = set()
    for rel in REMOTE_PATH:
        tree = ast.parse((ROOT / rel).read_text(), filename=rel)
        for fn, name, block in _tenant_connections(tree):
            uses = _non_read_uses(name, block)
            if not uses:
                continue
            if (rel, fn) in ALLOWED:
                seen_allowed.add((rel, fn))
                continue
            offenders.append(f"{rel}::{fn} uses its tenant-plane connection {name!r}: {uses}")
    assert offenders == [], "\n".join(offenders)
    # An allowance no longer needed is removed, not kept.
    assert seen_allowed == set(ALLOWED)


def test_the_guard_sees_a_write_through_the_tenant_plane():
    tree = ast.parse(
        "async def f(state, rows):\n"
        "    async with state.tenant_db.acquire() as c:\n"
        "        await c.fetch('SELECT 1')\n"
        "        await c.executemany('INSERT INTO t VALUES ($1)', rows)\n"
    )
    ((fn, name, block),) = list(_tenant_connections(tree))
    assert (fn, name) == ("f", "c")
    assert _non_read_uses(name, block) == ["line 4: executemany"]
