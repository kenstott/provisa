# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1a9f43-2c85-4e17-9b06-3e7f2a4c8d51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1140: a materialized view may publish only over approved relationships.

Covers the pure gate: join-dependency extraction from view_sql, approved-relationship matching
(either orientation), and the evaluate_gate resolution that reports the missing relationships the
caller must auto-create or queue for approval.
"""

from __future__ import annotations

import pytest

from provisa.api.admin.mv_relationship_gate import (
    JoinDep,
    evaluate_gate,
    extract_join_deps,
    relationship_present,
)


# ------------------------------------------------------- extract_join_deps


def test_extract_simple_join_with_aliases():
    deps = extract_join_deps("SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id")
    assert deps == [JoinDep("orders", "cust_id", "customers", "id")]


def test_extract_multiple_joins():
    sql = (
        "SELECT * FROM orders o "
        "JOIN customers c ON o.cust_id = c.id "
        "JOIN regions r ON c.region_id = r.id"
    )
    deps = extract_join_deps(sql)
    assert JoinDep("orders", "cust_id", "customers", "id") in deps
    assert JoinDep("customers", "region_id", "regions", "id") in deps
    assert len(deps) == 2


def test_extract_ignores_literal_join_predicate():
    # a JOIN whose ON is not a column=column equality contributes no relationship dependency
    deps = extract_join_deps("SELECT * FROM a JOIN b ON b.flag = true")
    assert deps == []


def test_extract_no_joins():
    assert extract_join_deps("SELECT * FROM orders") == []


def test_extract_unparseable_is_empty_not_raise():
    assert extract_join_deps("this is not sql !!!") == []


def test_extract_dedupes():
    sql = "SELECT * FROM a JOIN b ON a.x = b.y AND a.x = b.y"
    assert extract_join_deps(sql) == [JoinDep("a", "x", "b", "y")]


# ------------------------------------------------------- relationship_present


def _rel(sid, scol, tid, tcol):
    return {
        "source_table_id": sid,
        "source_column": scol,
        "target_table_id": tid,
        "target_column": tcol,
    }


def test_relationship_present_forward():
    rels = [_rel(1, "cust_id", 2, "id")]
    assert relationship_present(rels, 1, "cust_id", 2, "id")


def test_relationship_present_reverse_orientation():
    rels = [_rel(2, "id", 1, "cust_id")]
    assert relationship_present(rels, 1, "cust_id", 2, "id")


def test_relationship_absent():
    rels = [_rel(1, "cust_id", 3, "id")]
    assert not relationship_present(rels, 1, "cust_id", 2, "id")


def test_relationship_function_target_never_matches():
    rels = [{"source_table_id": 1, "source_column": "x", "target_table_id": None, "target_column": None}]
    assert not relationship_present(rels, 1, "x", 2, "y")


# ------------------------------------------------------- evaluate_gate


def _resolver(mapping):
    async def _r(name):
        return mapping.get(name)

    return _r


@pytest.mark.asyncio
async def test_gate_satisfied_when_relationship_exists():
    decision = await evaluate_gate(
        view_sql="SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id",
        dialect=None,
        relationships=[_rel(1, "cust_id", 2, "id")],
        resolve_table_id=_resolver({"orders": 1, "customers": 2}),
    )
    assert decision.satisfied
    assert decision.missing == []


@pytest.mark.asyncio
async def test_gate_reports_missing_relationship():
    decision = await evaluate_gate(
        view_sql="SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id",
        dialect=None,
        relationships=[],
        resolve_table_id=_resolver({"orders": 1, "customers": 2}),
    )
    assert not decision.satisfied
    assert len(decision.missing) == 1
    m = decision.missing[0]
    assert (m.left_table_id, m.right_table_id) == (1, 2)
    assert m.dep == JoinDep("orders", "cust_id", "customers", "id")


@pytest.mark.asyncio
async def test_gate_skips_unresolvable_table():
    # a join over a derived/unknown alias is not a tracked relationship — it does not gate
    decision = await evaluate_gate(
        view_sql="SELECT * FROM orders o JOIN sub s ON o.x = s.y",
        dialect=None,
        relationships=[],
        resolve_table_id=_resolver({"orders": 1}),  # 'sub' unresolved
    )
    assert decision.satisfied


# ------------------------------------------------------- register_table orchestration


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        return _Ctx()


@pytest.fixture
def _gate_env(monkeypatch):
    """Patch register_table's collaborators; return a dict recording queued/created effects."""
    from types import SimpleNamespace

    import provisa.api.admin.capabilities as caps
    import provisa.api.admin.schema_mutation_ops as ops
    import provisa.core.repositories.relationship as rel_repo
    import provisa.core.repositories.table as table_repo

    rec: dict = {"created": [], "queued": []}

    monkeypatch.setattr(ops, "_get_pool", lambda: _async(_FakePool(object())))
    monkeypatch.setattr(rel_repo, "list_all", lambda conn: _async([]))
    monkeypatch.setattr(
        table_repo, "find_by_table_name", lambda conn, name: _async({"id": hash(name) % 1000})
    )

    async def _upsert(conn, rel):
        rec["created"].append(rel.id)

    monkeypatch.setattr(rel_repo, "upsert", _upsert)

    async def _queue(info, rtype, cap, inp):
        rec["queued"].append((rtype, getattr(inp, "id", getattr(inp, "table_name", "?"))))
        from provisa.api.admin.types import MutationResult

        return MutationResult(success=True, message=f"queued {rtype}")

    monkeypatch.setattr(ops, "_queue_creation_request", _queue)
    monkeypatch.setattr(caps, "_identity_from_info", lambda info: SimpleNamespace(user_id="u1"))
    return rec, caps, ops


def _async(value):
    async def _coro():
        return value

    return _coro()


def _view_input():
    from types import SimpleNamespace

    return SimpleNamespace(
        view_sql="SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id",
        materialize=True,
        table_name="order_summary",
    )


@pytest.mark.asyncio
async def test_orchestration_auto_creates_with_rights(_gate_env, monkeypatch):
    rec, caps, ops = _gate_env
    monkeypatch.setattr(caps, "has_capability", lambda info, c: True)

    result = await ops._apply_mv_relationship_gate(info=None, input=_view_input())

    assert result is None  # proceed
    assert len(rec["created"]) == 1  # relationship auto-created + approved
    assert rec["queued"] == []


@pytest.mark.asyncio
async def test_orchestration_queues_and_blocks_without_rights(_gate_env, monkeypatch):
    rec, caps, ops = _gate_env
    monkeypatch.setattr(caps, "has_capability", lambda info, c: False)

    result = await ops._apply_mv_relationship_gate(info=None, input=_view_input())

    assert result is not None  # blocked, queued result returned
    assert rec["created"] == []
    # both a relationship request and the view request are queued
    kinds = [k for k, _ in rec["queued"]]
    assert "relationship" in kinds
    assert "view" in kinds


@pytest.mark.asyncio
async def test_orchestration_noop_for_non_materialized_view(_gate_env):
    from types import SimpleNamespace

    rec, caps, ops = _gate_env
    inp = SimpleNamespace(view_sql="SELECT 1", materialize=False, table_name="v")
    assert await ops._apply_mv_relationship_gate(info=None, input=inp) is None
    assert rec["created"] == [] and rec["queued"] == []
