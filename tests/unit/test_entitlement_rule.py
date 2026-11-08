# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9c1f02-6a84-4d1b-92e7-f5b0d8a6c741
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: the first governance rule, and the response and plan cache keys."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import sqlglot

from provisa.cache.key import raw_sql_cache_key
from provisa.compiler.entitlement_rule import entitlement_violations, narrow_governance
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.entitlements import runtime
from provisa.entitlements.lists import HeldEntitlements, effective_lists
from provisa.entitlements.mask import MaskCache, MemberInput, compute_mask
from provisa.pgwire import _pipeline
from provisa.pgwire.governed_plan import PlanSlot, plan_key
from tests.unit.test_entitlement_lists import _index
from tests.unit.test_governed_plan_stages import _slot_state

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)


def _governance(**over) -> GovernanceContext:
    base = GovernanceContext(
        table_map={"revenue": 1, "orders": 2, "customers": 3},
        all_columns={
            1: [("id", "int"), ("amount", "int")],
            2: [("id", "int"), ("customer_id", "int")],
            3: [("id", "int")],
        },
        visible_columns={1: None, 2: None, 3: None},
        role_id="viewer",
    )
    return dataclasses.replace(base, **over)


def _masked(tables=None, columns=None, relationships=None):
    lists: dict = {}
    if tables is not None:
        lists["tables"] = tuple(tables)
    if columns is not None:
        lists["columns"] = tuple(columns)
    if relationships is not None:
        lists["relationships"] = tuple(relationships)
    effective = effective_lists("viewer", {"viewer": lists}, HeldEntitlements(), {})
    index = _index()
    return compute_mask(
        "viewer", index, [MemberInput("viewer", effective, index, ())], ("b", 1), NOW
    )


def _violations(sql, mask, expanded=None):
    gov = narrow_governance(_governance(), mask)
    written = sqlglot.parse_one(sql, read="postgres")
    return gov, entitlement_violations(
        written, sqlglot.parse_one(expanded, read="postgres") if expanded else written, gov
    )


def test_a_table_outside_the_mask_is_refused_naming_it():
    _, found = _violations("SELECT id FROM orders", _masked(tables=["finance.revenue"]))
    assert [v.code for v in found] == ["E001"]
    assert "orders" in found[0].message


def test_a_table_in_the_mask_passes():
    assert _violations("SELECT id FROM revenue", _masked(tables=["finance.revenue"]))[1] == []


def test_a_column_outside_the_mask_is_refused():
    mask = _masked(columns=["finance.revenue.id"])
    _, found = _violations("SELECT amount FROM revenue", mask)
    assert [v.code for v in found] == ["E002"]
    assert "amount" in found[0].message
    assert _violations("SELECT id FROM revenue", mask)[1] == []


def test_a_qualified_column_is_resolved_through_its_alias():
    mask = _masked(columns=["finance.revenue.id", "sales.orders.id"])
    _, found = _violations("SELECT r.amount, o.id FROM revenue r, orders o", mask)
    assert [v.code for v in found] == ["E002"]
    assert "amount" in found[0].message


def test_an_unqualified_column_is_judged_on_the_table_that_has_it():
    # customer_id is only on orders; revenue being narrowed says nothing about it.
    mask = _masked(columns=["finance.revenue.id", "sales.orders.customer_id"])
    assert _violations("SELECT customer_id FROM orders CROSS JOIN revenue", mask)[1] == []


def test_a_column_in_a_where_clause_is_touched():
    mask = _masked(columns=["finance.revenue.id"])
    _, found = _violations("SELECT id FROM revenue WHERE amount > 1", mask)
    assert [v.code for v in found] == ["E002"]


def test_a_star_is_not_refused_and_reads_the_narrowed_columns():
    mask = _masked(columns=["finance.revenue.id"])
    gov, found = _violations("SELECT * FROM revenue", mask)
    assert found == []
    assert gov.visible_columns[1] == frozenset({"id"})
    governed = apply_governance("SELECT * FROM revenue", gov)
    assert "amount" not in governed.lower()


def test_a_cte_name_is_not_a_table():
    mask = _masked(tables=["finance.revenue"])
    sql = "WITH orders AS (SELECT id FROM revenue) SELECT id FROM orders"
    assert _violations(sql, mask)[1] == []


def test_a_view_body_table_outside_the_mask_is_refused():
    mask = _masked(tables=["finance.revenue"])
    written = "SELECT id FROM revenue"
    expanded = "SELECT id FROM (SELECT id FROM orders) AS revenue"
    assert [v.code for v in _violations(written, mask, expanded)[1]] == ["E001"]


def test_a_view_named_outside_the_mask_is_refused_even_if_its_body_is_in():
    mask = _masked(tables=["sales.orders"])
    written = "SELECT id FROM revenue"
    expanded = "SELECT id FROM (SELECT id FROM orders) AS revenue"
    assert [v.code for v in _violations(written, mask, expanded)[1]] == ["E001"]


def test_a_join_on_a_relationship_outside_the_mask_is_refused():
    mask = _masked(tables=["sales.*"], relationships=[])
    sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id"
    assert [v.code for v in _violations(sql, mask)[1]] == ["E003"]


def test_a_join_on_a_relationship_in_the_mask_passes():
    mask = _masked(tables=["sales.*"])
    sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id"
    assert _violations(sql, mask)[1] == []


def test_a_correlated_subquery_on_a_relationship_outside_the_mask_is_refused():
    mask = _masked(tables=["sales.*"], relationships=[])
    sql = (
        "SELECT o.id, (SELECT c.id FROM customers c WHERE c.id = o.customer_id) AS c FROM orders o"
    )
    assert [v.code for v in _violations(sql, mask)[1]] == ["E003"]


def test_an_update_of_a_column_outside_the_mask_is_refused():
    mask = _masked(columns=["finance.revenue.id"])
    _, found = _violations("UPDATE revenue SET amount = 1", mask)
    assert [v.code for v in found] == ["E002"]
    _, inserted = _violations("INSERT INTO revenue (amount) VALUES (1)", mask)
    assert [v.code for v in inserted] == ["E002"]
    assert _violations("UPDATE revenue SET id = 1", mask)[1] == []


def test_a_full_mask_changes_nothing():
    gov = _governance()
    assert narrow_governance(gov, _masked()) is gov
    assert gov.visible_columns == {1: None, 2: None, 3: None}


def test_no_mask_changes_nothing():
    gov = _governance()
    assert narrow_governance(gov, None) is gov
    tree = sqlglot.parse_one("SELECT id FROM orders", read="postgres")
    assert entitlement_violations(tree, tree, gov) == []


# ── where it runs ──────────────────────────────────────────────────────────────────────────────


def _pipeline_calls(function: str) -> list[str]:
    """The calls ``function`` in the pipeline makes, in source order."""
    import ast
    from pathlib import Path

    import provisa.pgwire._pipeline as module

    tree = ast.parse(Path(module.__file__).read_text())
    body = next(
        n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == function
    )
    calls = [
        (n.lineno, n.col_offset, n.func.id)
        for n in ast.walk(body)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    ]
    return [name for _l, _c, name in sorted(calls)]


@pytest.mark.parametrize("function", ["govern_statement", "_govern_compiled"])
def test_the_rule_is_first_on_every_governed_path(function):
    calls = _pipeline_calls(function)
    assert "entitlement_violations" in calls
    first_rule = calls.index("entitlement_violations")
    assert first_rule < calls.index("validate_sql")
    assert first_rule < calls.index("apply_governance") if "apply_governance" in calls else True
    # Reading a view for this reader refuses a table it cannot reach: the mask speaks first.
    assert first_rule < calls.index("semantic_statement")


class _NothingInMask:
    """A mask that holds no table: enough to reach the refusal through the real pipeline."""

    full = False
    hash = "nothing"

    def knows_table(self, _table_id):
        return True

    def has_table(self, _table_id):
        return False

    def has_column(self, _table_id, _name):
        return False

    def knows_relationship(self, _key):
        return False

    def has_relationship(self, *_key):
        return False


@pytest.mark.asyncio
async def test_govern_statement_refuses_a_table_outside_the_mask_before_any_other_rule(
    monkeypatch,
):
    from provisa.compiler.sql_validator import GovernanceRefused
    from provisa.pgwire import _pipeline
    from tests.unit.test_v003_scope_pipeline import _SQL
    from tests.unit.test_view_inputs_in_service import _OVER_A_SERVED_TABLE, _build, _view

    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    monkeypatch.setattr(runtime, "request_mask", lambda _state, _role: _NothingInMask())

    # _SQL also trips a validate_sql rule (the unnest refusal): the mask's refusal comes first.
    with pytest.raises(GovernanceRefused, match="E001") as refused:
        await _pipeline.govern_statement(_SQL, "r")
    assert "unnest" not in str(refused.value).lower()


class _OnlyTheView(_NothingInMask):
    """A mask holding one view and none of the tables it reads."""

    def __init__(self, view_id):
        self._view_id = view_id

    def has_table(self, table_id):
        return table_id == self._view_id

    def has_column(self, table_id, _name):
        return table_id == self._view_id


@pytest.mark.asyncio
async def test_a_view_in_the_mask_over_a_table_outside_it_is_refused_by_the_mask(monkeypatch):
    """A view is judged whole for its reader, and the mask is what refuses it: reading the view's
    body for this reader finds the table unreachable too, in words that say nothing of why."""
    from provisa.compiler.sql_validator import GovernanceRefused
    from provisa.pgwire import _pipeline
    from tests.unit.test_view_inputs_in_service import (
        _OVER_A_SERVED_TABLE,
        _VIEW,
        _build,
        _view,
    )

    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    monkeypatch.setattr(runtime, "request_mask", lambda _state, _role: _OnlyTheView(_VIEW))

    with pytest.raises(GovernanceRefused, match="E001"):
        await _pipeline.govern_statement('SELECT * FROM "sales"."refund_notes" LIMIT 5', "r")


@pytest.mark.asyncio
async def test_a_view_outside_the_mask_is_refused_by_the_mask(monkeypatch):
    from provisa.compiler.sql_validator import GovernanceRefused
    from provisa.pgwire import _pipeline
    from tests.unit.test_view_inputs_in_service import _OVER_A_SERVED_TABLE, _build, _view

    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    monkeypatch.setattr(runtime, "request_mask", lambda _state, _role: _NothingInMask())

    with pytest.raises(GovernanceRefused, match="E001"):
        await _pipeline.govern_statement('SELECT * FROM "sales"."refund_notes" LIMIT 5', "r")


# ── the cache keys ─────────────────────────────────────────────────────────────────────────────


def _state():
    return SimpleNamespace(schema_boot_id="b", schema_version=1)


def test_response_key_differs_by_mask_hash():
    one = raw_sql_cache_key("SELECT 1", [], "viewer", wire_formats=None, mask_hash="aaa")
    two = raw_sql_cache_key("SELECT 1", [], "viewer", wire_formats=None, mask_hash="bbb")
    assert one != two


def test_a_full_mask_leaves_the_response_key_as_it_was():
    plain = raw_sql_cache_key("SELECT 1", [], "viewer", wire_formats=None)
    assert raw_sql_cache_key("SELECT 1", [], "viewer", wire_formats=None, mask_hash="") == plain


def test_plan_key_differs_by_request_mask(monkeypatch):
    masks = iter(["aaa", "bbb", "aaa"])
    monkeypatch.setattr(runtime, "current_mask_hash", lambda state, role_id: next(masks))
    from provisa.pgwire import governed_plan

    def key():
        return governed_plan.mask_scoped_body(_state(), "sql", "viewer", ["SELECT 1"])

    state = _state()
    a = plan_key(state, "sql", "viewer", *key())
    b = plan_key(state, "sql", "viewer", *key())
    c = plan_key(state, "sql", "viewer", *key())
    assert a != b
    assert a == c


def test_a_full_mask_leaves_the_plan_key_as_it_was(monkeypatch):
    monkeypatch.setattr(runtime, "current_mask_hash", lambda state, role_id: "")
    from provisa.pgwire import governed_plan

    body = governed_plan.mask_scoped_body(_state(), "sql", "viewer", ["SELECT 1"])
    assert body == ["SELECT 1"]


def test_the_governance_stage_is_never_keyed_by_the_mask(monkeypatch):
    def refuse(state, role_id):
        raise AssertionError("the governance stage does not read the mask")

    monkeypatch.setattr(runtime, "current_mask_hash", refuse)
    from provisa.pgwire import governed_plan

    assert governed_plan.mask_scoped_body(_state(), "governance", "viewer", [1]) == [1]


# ── the request's mask ─────────────────────────────────────────────────────────────────────────


def _runtime_state(held=None):
    from tests.unit.test_entitlement_lists import _names

    return SimpleNamespace(
        schema_boot_id="b",
        schema_version=1,
        names_table=_names(),
        roles={"viewer": {"capabilities": [], "domain_access": ["*"], "default_entitlements": {}}},
        held_entitlements=held or {},
        entitlement_masks=MaskCache(),
        data_product_refs=(),
    )


def _bind(monkeypatch, claims, user="alice"):
    from tests.unit.test_entitlement_lists import _ctx

    monkeypatch.setattr(runtime, "_role_context", lambda state, role_id: _ctx())
    identity = SimpleNamespace(
        user_id=user, raw_claims=claims, entitlements_claim="provisa_entitlements"
    )
    return identity


def test_no_bound_principal_is_unmasked(monkeypatch):
    state = _runtime_state()
    assert runtime.request_mask(state, "viewer") is None
    assert runtime.current_mask_hash(state, "viewer") == ""


def test_a_principal_with_no_lists_on_a_plain_role_needs_no_mask(monkeypatch):
    state = _runtime_state()
    identity = _bind(monkeypatch, {})
    monkeypatch.setattr(runtime, "compute_mask", _never)
    with runtime.entitlements_scope(identity):
        assert runtime.request_mask(state, "viewer") is None
        assert runtime.current_mask_hash(state, "viewer") == ""


def _never(*_a, **_k):
    raise AssertionError("a plain role grant computes no mask")


def test_a_deny_by_default_role_is_masked_with_no_lists(monkeypatch):
    state = _runtime_state()
    state.roles["viewer"]["default_entitlements"] = {"domains": []}
    identity = _bind(monkeypatch, {})
    with runtime.entitlements_scope(identity):
        mask = runtime.request_mask(state, "viewer")
        assert mask is not None and not mask.has_table(1)


def test_a_qualifier_role_with_no_schema_adds_nothing_and_breaks_nothing(monkeypatch):
    from provisa.security.meta_role import meta_role_id

    from tests.unit.test_entitlement_lists import _ctx

    state = _runtime_state()
    meta = meta_role_id(["viewer", "region__eu"])
    state.roles[meta] = state.roles["viewer"]
    monkeypatch.setattr(
        runtime, "_role_context", lambda _s, r: None if r == "region__eu" else _ctx()
    )
    identity = SimpleNamespace(
        user_id="alice",
        raw_claims={"groups": ["provisa:viewer:tables:finance.revenue"]},
        entitlements_claim=None,
    )
    with runtime.entitlements_scope(identity):
        mask = runtime.request_mask(state, meta)
        assert mask.has_table(1) and not mask.has_table(2)


def test_a_principal_with_a_list_is_masked_and_hashed(monkeypatch):
    state = _runtime_state()
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:finance.revenue"]})
    with runtime.entitlements_scope(identity):
        mask = runtime.request_mask(state, "viewer")
        assert mask.has_table(1) and not mask.has_table(2)
        assert runtime.current_mask_hash(state, "viewer") == mask.hash != ""


def test_the_mask_is_computed_once_per_key(monkeypatch):
    state = _runtime_state()
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:finance.revenue"]})
    calls = []
    real = runtime.compute_mask

    def counting(*a, **k):
        calls.append(1)
        return real(*a, **k)

    monkeypatch.setattr(runtime, "compute_mask", counting)
    with runtime.entitlements_scope(identity):
        runtime.request_mask(state, "viewer")
        runtime.request_mask(state, "viewer")
    assert len(calls) == 1


def test_a_model_build_invalidates_the_request_mask(monkeypatch):
    state = _runtime_state()
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:finance.revenue"]})
    calls = []
    real = runtime.compute_mask

    def counting(*a, **k):
        calls.append(1)
        return real(*a, **k)

    monkeypatch.setattr(runtime, "compute_mask", counting)
    with runtime.entitlements_scope(identity):
        runtime.request_mask(state, "viewer")
        state.schema_version = 2
        runtime.request_mask(state, "viewer")
    assert len(calls) == 2


def test_a_malformed_claim_is_refused_at_binding(monkeypatch):
    from provisa.entitlements.lists import EntitlementClaimError

    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables"]})
    with pytest.raises(EntitlementClaimError):
        with runtime.entitlements_scope(identity):
            pass


def test_a_held_list_for_an_unknown_identifier_warns_once(monkeypatch, caplog):
    state = _runtime_state()
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:finance.nope"]})
    with caplog.at_level("WARNING"):
        with runtime.entitlements_scope(identity):
            runtime.request_mask(state, "viewer")
            runtime.request_mask(state, "viewer")
    assert sum("finance.nope" in r.getMessage() for r in caplog.records) == 1


def test_lists_with_no_names_table_fail_closed(monkeypatch):
    state = _runtime_state()
    state.names_table = None
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:finance.revenue"]})
    with runtime.entitlements_scope(identity):
        with pytest.raises(RuntimeError, match="names table"):
            runtime.request_mask(state, "viewer")


def test_no_lists_and_no_names_table_is_unmasked(monkeypatch):
    state = _runtime_state()
    state.names_table = None
    identity = _bind(monkeypatch, {})
    with runtime.entitlements_scope(identity):
        assert runtime.request_mask(state, "viewer") is None


# ── the mask and the session zone, side by side ────────────────────────────────────────────────


def _cached_plan(zone):
    return _pipeline._Plan(
        route=None,
        sql="SELECT 1",
        source_id=None,
        dialect="duckdb",
        cache_opt_in=True,
        response_cacheable=True,
        role_id="viewer",
        cache_sql="SELECT 1",
        cache_params=[],
        cache_zone=zone,
    )


def _response_entry(monkeypatch, mask, zone):
    monkeypatch.setattr(runtime, "current_mask_hash", lambda state, role_id: mask)
    return _pipeline._response_cache_key(_cached_plan(zone), wire_formats=None)


def test_callers_differing_only_in_mask_get_separate_response_entries(monkeypatch):
    """Neither part stands in for the other: the mask separates callers in one zone, on UTC or not."""
    for zone in (None, "Asia/Tokyo"):
        narrow = _response_entry(monkeypatch, "aaa", zone)
        other = _response_entry(monkeypatch, "bbb", zone)
        assert narrow is not None and other is not None
        assert narrow != other
        assert narrow == _response_entry(monkeypatch, "aaa", zone)


def test_callers_differing_only_in_zone_get_separate_response_entries(monkeypatch):
    """The zone separates callers holding one mask, narrowed or not."""
    for mask in ("", "aaa"):
        tokyo = _response_entry(monkeypatch, mask, "Asia/Tokyo")
        new_york = _response_entry(monkeypatch, mask, "America/New_York")
        assert tokyo is not None and new_york is not None
        assert tokyo != new_york
        assert tokyo == _response_entry(monkeypatch, mask, "Asia/Tokyo")


def test_the_response_key_is_the_one_the_mask_and_zone_both_reach(monkeypatch):
    entries = {
        _response_entry(monkeypatch, m, z) for m in ("", "aaa") for z in (None, "Asia/Tokyo")
    }
    assert len(entries) == 4
    assert _response_entry(monkeypatch, "aaa", "Asia/Tokyo") == raw_sql_cache_key(
        "SELECT 1", [], "viewer", wire_formats=None, mask_hash="aaa", zone="Asia/Tokyo"
    )


def test_callers_differing_only_in_mask_get_separate_plan_entries(monkeypatch):
    from provisa.pgwire import governed_plan

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = _slot_state()
    monkeypatch.setattr(runtime, "current_mask_hash", lambda state, role_id: "aaa")
    PlanSlot(state, "sql", "analyst", "SELECT 1").keep("the plan governed for aaa")
    assert PlanSlot(state, "sql", "analyst", "SELECT 1").cached() == "the plan governed for aaa"
    monkeypatch.setattr(runtime, "current_mask_hash", lambda state, role_id: "bbb")
    assert PlanSlot(state, "sql", "analyst", "SELECT 1").cached() is None


async def test_callers_differing_only_in_zone_get_separate_engine_forms(monkeypatch):
    """A kept plan's physical text serves every zone (REQ-2171); what is written for one zone is
    its engine form, kept with the plan and derived again for another."""
    memo: dict = {}
    state = SimpleNamespace(federation_engine=object())
    zone = {"now": "Asia/Tokyo"}
    monkeypatch.setattr(_pipeline, "zone_cache_key", lambda: zone["now"])

    async def derive():
        return f"written for {zone['now']}"

    tokyo = await _pipeline._kept_engine_form(memo, "SELECT 1", state, derive)
    zone["now"] = "America/New_York"
    new_york = await _pipeline._kept_engine_form(memo, "SELECT 1", state, derive)
    assert (tokyo, new_york) == ("written for Asia/Tokyo", "written for America/New_York")
