# Copyright (c) 2026 Kenneth Stott
# Canary: 55cee4f8-9350-402c-b45d-df22c24b19c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The audit write the ONE governed pipeline performs (REQ-596, REQ-074, REQ-1386).

query_audit_log is the fact table every ops report view reads. Before this, no production path
wrote it — the pipeline governed and executed without ever appending a row, so the reports were
empty on a live install. These tests hold the write at the pipeline (never per transport), the
identity channel that carries the acting principal into it, and the once-per-statement guarantee.
"""

# Requirements: REQ-596, REQ-074, REQ-689, REQ-1386

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest
import sqlglot

from provisa.audit.context import (
    audit_identity_scope,
    current_audit_identity,
    with_audit_identity,
)
from provisa.audit.pipeline import (
    PendingAudit,
    begin_audit,
    resolve_table_ids,
    write_audit,
    write_denial,
)

REPO = Path(__file__).resolve().parents[2]


@dataclass
class _Ctx:
    """Stands in for GovernanceContext — only table_map is read here."""

    table_map: dict[str, int]


class _CapturingState:
    """An AppState stand-in whose tenant_db records the audit insert."""

    def __init__(self, org_id: str = "default") -> None:
        self.tenant_db = object()
        self.org_id = org_id


@pytest.fixture
def captured(monkeypatch) -> list[dict]:
    rows: list[dict] = []

    async def _log_query(pool, **kwargs):  # noqa: ANN001
        rows.append(kwargs)

    monkeypatch.setattr("provisa.audit.query_log.log_query", _log_query)
    monkeypatch.setattr("provisa.encryption.runtime.encryption_service", lambda: None)
    return rows


# ------------------------------------------------------------------ table ids


def test_resolve_table_ids_uses_the_governance_table_map():
    """The ids recorded are the same registered_tables ids governance resolved — the ops
    usage spine (ops_table_usage) casts each element to INTEGER, so a name would break it."""
    tree = sqlglot.parse_one("SELECT * FROM sales.orders JOIN sales.customers USING (id)")
    ctx = _Ctx(table_map={"sales.orders": 7, "orders": 7, "sales.customers": 9, "customers": 9})
    assert resolve_table_ids(tree, ctx) == [7, 9]


def test_resolve_table_ids_skips_unregistered_references():
    """A CTE alias is not a registered table and contributes no usage row."""
    tree = sqlglot.parse_one("WITH t AS (SELECT * FROM sales.orders) SELECT * FROM t")
    ids = resolve_table_ids(tree, _Ctx(table_map={"sales.orders": 7, "orders": 7}))
    assert ids == [7]


def test_resolve_table_ids_deduplicates_a_self_join():
    tree = sqlglot.parse_one("SELECT * FROM sales.orders a JOIN sales.orders b ON a.id = b.id")
    assert resolve_table_ids(tree, _Ctx(table_map={"sales.orders": 7, "orders": 7})) == [7]


# ------------------------------------------------------------------ identity channel


def test_begin_audit_returns_none_without_an_acting_principal():
    """Startup seeding and materialization refreshes are not user queries: no identity is
    bound, so no row is opened — the pipeline must not invent a principal."""
    tree = sqlglot.parse_one("SELECT 1")
    assert begin_audit("SELECT 1", "analyst", tree, _Ctx(table_map={})) is None


def test_begin_audit_records_the_bound_principal_and_surface():
    tree = sqlglot.parse_one("SELECT * FROM sales.orders")
    with audit_identity_scope("alice", "pgwire"):
        pending = begin_audit("SELECT * FROM sales.orders", "analyst", tree, _Ctx({"orders": 7}))
    assert pending is not None
    assert (pending.user_id, pending.surface, pending.role_id) == ("alice", "pgwire", "analyst")
    assert pending.table_ids == [7]


def test_audit_identity_scope_restores_the_previous_binding():
    with audit_identity_scope("alice", "http"):
        with audit_identity_scope("bob", "grpc"):
            inner = current_audit_identity()
            assert inner is not None and inner.user_id == "bob"
        outer = current_audit_identity()
        assert outer is not None and outer.user_id == "alice"
    assert current_audit_identity() is None


def test_with_audit_identity_binds_inside_the_awaited_coroutine():
    """pgwire, Flight SQL, gRPC and the airport govern on the main loop from a worker thread via
    run_coroutine_threadsafe, which does NOT carry ContextVars across the thread boundary. The
    wrapper binds the principal where the coroutine actually runs."""

    async def _inner():
        ident = current_audit_identity()
        return None if ident is None else (ident.user_id, ident.surface)

    async def _main():
        return await with_audit_identity("carol", "flight", _inner())

    assert asyncio.run(_main()) == ("carol", "flight")
    assert current_audit_identity() is None


def test_with_audit_identity_unbinds_after_the_coroutine_raises():
    async def _boom():
        raise ValueError("terminal failed")

    async def _main():
        with pytest.raises(ValueError):
            await with_audit_identity("carol", "flight", _boom())
        return current_audit_identity()

    assert asyncio.run(_main()) is None


# ------------------------------------------------------------------ the write


def test_write_audit_appends_the_completed_statement(captured):
    pending = PendingAudit(
        user_id="alice",
        surface="pgwire",
        role_id="analyst",
        query_text="SELECT * FROM sales.orders",
        table_ids=[7],
        started=0.0,
    )
    asyncio.run(write_audit(pending, 200, _CapturingState()))
    assert len(captured) == 1
    row = captured[0]
    assert row["user_id"] == "alice"
    assert row["role_id"] == "analyst"
    assert row["source"] == "pgwire"
    assert row["status_code"] == 200
    assert row["table_ids"] == [7]
    assert row["duration_ms"] >= 0
    assert row["tenant_id"] == "default"


def test_the_recorded_tenant_is_the_org_that_owns_the_row(captured):
    """The tenant IS the org (REQ-594). This read the meta-RLS ContextVar, which production sets
    nowhere, so every row landed with a NULL tenant and every ops report showed a NULL column."""
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org("kstott")
    try:
        asyncio.run(
            write_audit(
                PendingAudit("alice", "http", "org_admin", "SELECT 1", [], 0.0),
                200,
                _CapturingState(org_id="default"),
            )
        )
    finally:
        reset_current_org(token)
    assert captured[0]["tenant_id"] == "kstott"


def test_write_audit_on_a_none_record_writes_nothing(captured):
    asyncio.run(write_audit(None, 200, _CapturingState()))
    assert captured == []


def test_write_audit_refuses_to_run_without_a_tenant_database(captured):
    """query_audit_log lives in org_<id>. No bound org runtime means the row would land in the
    wrong place (or nowhere) — that is a wiring defect, not something to swallow."""

    class _NoTenant:
        tenant_db = None
        org_id = "default"

    pending = PendingAudit("alice", "pgwire", "analyst", "SELECT 1", [], 0.0)
    with pytest.raises(RuntimeError, match="tenant database"):
        asyncio.run(write_audit(pending, 200, _NoTenant()))
    assert captured == []


def test_write_denial_records_a_403_with_the_refused_tables(captured):
    """policy_denials reads exactly these rows."""
    tree = sqlglot.parse_one("SELECT * FROM hr.salaries")
    with audit_identity_scope("mallory", "http"):
        asyncio.run(
            write_denial(
                "SELECT * FROM hr.salaries",
                "analyst",
                tree,
                _Ctx({"hr.salaries": 12, "salaries": 12}),
                _CapturingState(),
            )
        )
    assert len(captured) == 1
    assert captured[0]["status_code"] == 403
    assert captured[0]["table_ids"] == [12]
    assert captured[0]["user_id"] == "mallory"


def test_write_denial_without_a_resolved_tree_records_no_tables(captured):
    """A statement refused before governance resolved anything (unknown role) touched no
    registered table — the row says so rather than guessing."""
    with audit_identity_scope("mallory", "http"):
        asyncio.run(write_denial("SELECT 1", "ghost", None, None, _CapturingState()))
    assert len(captured) == 1
    assert captured[0]["table_ids"] == []


def test_write_denial_without_an_identity_writes_nothing(captured):
    tree = sqlglot.parse_one("SELECT * FROM hr.salaries")
    asyncio.run(write_denial("SELECT * FROM hr.salaries", "analyst", tree, _Ctx({}), _CapturingState()))
    assert captured == []


# ------------------------------------------------------------------ once per statement


def _plan_with_audit(pending: PendingAudit | None):
    from provisa.pgwire._pipeline import _Plan
    from provisa.transpiler.router import Route

    return _Plan(route=Route.ENGINE, sql="SELECT 1", source_id="s", dialect="duckdb", audit=pending)


def test_finalize_audit_writes_once_per_plan(captured):
    """The govern-then-stream surfaces finalize at their own terminal; a plan that also reaches
    _execute_plan must not produce a second row."""
    from provisa.pgwire._pipeline import finalize_audit

    plan = _plan_with_audit(PendingAudit("alice", "flight", "analyst", "SELECT 1", [], 0.0))

    async def _main():
        await finalize_audit(plan, 200, _CapturingState())
        await finalize_audit(plan, 500, _CapturingState())

    asyncio.run(_main())
    assert len(captured) == 1
    assert captured[0]["status_code"] == 200


# ------------------------------------------------------------------ surface coverage


def _read(rel: str) -> str:
    return (REPO / rel).read_text()


@pytest.mark.parametrize(
    ("surface", "module", "marker"),
    [
        ("http", "provisa/auth/middleware.py", "audit_identity_scope"),
        ("pgwire", "provisa/pgwire/server.py", "with_audit_identity"),
        ("pgwire COPY", "provisa/pgwire/copy_handler.py", "with_audit_identity"),
        ("flight", "provisa/api/flight/server.py", "audit_identity_scope"),
        ("grpc", "provisa/grpc/auth.py", "set_audit_identity"),
        ("mcp", "provisa/api/mcp/server.py", "audit_identity_scope"),
        ("bolt", "provisa/bolt/session.py", "audit_identity_scope"),
        ("airport", "provisa/api/airport/query.py", "with_audit_identity"),
    ],
)
def test_every_caller_facing_surface_binds_an_acting_principal(surface, module, marker):
    """No identity bound means begin_audit returns None and the statement goes unaudited. A new
    surface that forgets to bind one silently drops itself out of every ops report."""
    assert marker in _read(module), f"{surface} does not bind an audit identity"


@pytest.mark.parametrize(
    ("surface", "module"),
    [
        ("pgwire", "provisa/pgwire/server.py"),
        ("pgwire COPY", "provisa/pgwire/copy_handler.py"),
        ("flight", "provisa/api/flight/server.py"),
        ("grpc", "provisa/grpc/server.py"),
        ("cypher REST exec", "provisa/api/rest/cypher_exec.py"),
        ("cypher REST router", "provisa/api/rest/cypher_router.py"),
        ("airport", "provisa/api/airport/query.py"),
        ("ctas", "provisa/executor/ctas.py"),
    ],
)
def test_every_govern_then_stream_terminal_finalizes_its_audit(surface, module):
    """These surfaces drain the engine themselves and never reach _execute_plan, so they must
    write the row at their own terminal — otherwise their traffic never reaches the reports."""
    assert "finalize_audit" in _read(module), f"{surface} terminal never writes its audit row"


def test_require_governed_plan_callers_are_the_known_surface_set():
    """Each require_governed_plan call outside the pipeline marks a govern-then-stream surface.
    A new one must be added to the finalize coverage above, not left silently unaudited."""
    import subprocess

    out = [
        path
        for path in subprocess.run(
            ["grep", "-rln", "require_governed_plan", "provisa"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
        if path.endswith(".py")
    ]
    known = {
        "provisa/pgwire/_pipeline.py",
        "provisa/pgwire/server.py",
        "provisa/pgwire/copy_handler.py",
        "provisa/api/flight/server.py",
        "provisa/api/airport/query.py",
        "provisa/api/rest/cypher_exec.py",
        "provisa/api/rest/cypher_router.py",
        "provisa/grpc/server.py",
        "provisa/executor/ctas.py",
    }
    assert set(out) - known == set(), (
        "a new surface verifies a governed plan but is not in the audited set"
    )
