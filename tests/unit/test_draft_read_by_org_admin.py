# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6b2d84-7a31-4c95-b2e8-5d1c9a3e7f42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN (decided 2026-11-12), on the SQL surfaces (pgwire,
/data/sql, MCP run_sql, which all reach the pipeline's top): a statement of a role holding table
registration in the draft's domain -- org_admin, a domain steward, a role inheriting the right, or
a meta role one of whose members carries it there -- that names a draft reads it as a served
table is, through the one path a draft is read by (admin/table_read.plan_reading_drafts):
governed under that role's masks, row rules and limits, nothing of it kept, and recorded as a
draft preview. Every other role's statement naming it is refused as a draft, as before.

The real role build and the real governance stage, with routing stood in for: what reaches
routing is what would be read."""

# Requirements: REQ-1921

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.admin import table_read
from provisa.api.role_surface import role_surface
from tests.unit.test_empty_domain_access_reaches_nothing import DOMAIN_OF, _tables
from tests.unit.test_table_read_as_caller import (
    _REFUNDS,
    _Routed,
    _refunds,
    multi_domain,  # noqa: F401  # pyright: ignore[reportUnusedImport]
    routing,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)

_READERS = ["org_admin", "steward", "sales_steward", "r", "s"]
_REG = "table_registration"


def _role(
    role_id: str,
    parents: list[str],
    capabilities: tuple[str, ...] = (),
    domains: tuple[str, ...] = ("sales",),
) -> dict:
    return {
        "id": role_id,
        "description": "Test role.",
        "capabilities": ["usage", *capabilities],
        "domain_access": list(domains),
        "own_domain_access": list(domains),
        "parents": parents,
    }


def _draft() -> dict:
    """``refunds``, a draft of the sales domain, its columns granted to each reader."""
    draft = _refunds()
    return {**draft, "columns": [{**c, "visible_to": list(_READERS)} for c in draft["columns"]]}


@pytest.fixture
def state(multi_domain, monkeypatch):  # noqa: F811
    """The real role build with ``refunds`` registered as a draft: org_admin, ``steward``
    inheriting from it, and ``r`` and ``s``, which hold no org_admin."""
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.compiler.introspect import ColumnMetadata

    built = appmod.state
    runtime = built._active_runtime()
    for name in ("meta_roles", "meta_resolved_at", "meta_qualifiers", "masking_rules"):
        monkeypatch.setattr(runtime, name, {}, raising=False)
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(built, name, {}, raising=False)
    monkeypatch.setattr(built, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(built, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(built, "source_catalogs", {"pg": "pg"}, raising=False)
    _build_and_register_schemas(
        roles=[
            # The right a draft is read by, in every domain (as org_admin is seeded with it)...
            _role("org_admin", [], (_REG,), ("*",)),
            # ...inherited from it,
            _role("steward", ["org_admin"]),
            # ...held in the draft's own domain alone,
            _role("sales_steward", [], (_REG,)),
            # ...and held in another domain only, by a role reaching no data of its own.
            _role("finance_registrar", [], (_REG,), ("finance",)),
            _role("r", []),
            _role("s", []),
        ],
        tables=_tables(),
        relationships=[],
        col_types_converted={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in DOMAIN_OF
        },
        naming_rules=[],
        domains=[{"id": d} for d in DOMAIN_OF.values()],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[_draft()],
    )
    monkeypatch.setattr(built, "tables", _tables(), raising=False)
    return built


_STATEMENT = 'SELECT * FROM "sales"."refunds" LIMIT 5'


async def _sent(sql: str, role_id: str) -> Any:
    from provisa.pgwire import _pipeline

    return await _pipeline._govern_and_route(sql, role_id)


@pytest.mark.parametrize("role_id", ["org_admin", "steward", "sales_steward"])
async def test_a_role_registering_tables_in_its_domain_reads_a_draft_it_names(  # noqa: F811
    state,
    routing,  # noqa: F811
    role_id,
):
    with pytest.raises(_Routed) as routed:
        await _sent(_STATEMENT, role_id)
    routing.assert_awaited_once()
    assert "refunds" in routed.value.sql


async def test_a_meta_role_with_the_right_among_its_members_reads_it(state, routing):  # noqa: F811
    from provisa.security.meta_role import ensure_meta_role

    with pytest.raises(_Routed):
        await _sent(_STATEMENT, ensure_meta_role(state, ["org_admin", "r"]))
    routing.assert_awaited_once()


async def test_every_other_role_is_refused_naming_the_draft(state, routing):  # noqa: F811
    from provisa.security.meta_role import ensure_meta_role

    with pytest.raises(PermissionError, match="draft"):
        await _sent(_STATEMENT, "r")
    with pytest.raises(PermissionError, match="draft"):
        await _sent(_STATEMENT, ensure_meta_role(state, ["r", "s"]))
    # The right in another domain, beside a role reaching sales: paired with its own domain.
    with pytest.raises(PermissionError, match="draft"):
        await _sent(_STATEMENT, ensure_meta_role(state, ["finance_registrar", "r"]))
    routing.assert_not_awaited()


async def test_the_draft_is_read_under_the_roles_own_masks(state, routing):  # noqa: F811
    from provisa.security.masking import MaskingRule, MaskType

    state.masking_rules = {
        (_REFUNDS, "org_admin"): {
            "note": (MaskingRule(mask_type=MaskType.constant, value="MASKED"), "varchar")
        }
    }
    with pytest.raises(_Routed):
        await _sent(_STATEMENT, "org_admin")
    governed = " ".join(str(a) for a in routing.await_args.args) + str(routing.await_args.kwargs)
    assert "MASKED" in governed


async def test_the_draft_is_read_under_the_roles_own_row_rule(state, routing, monkeypatch):  # noqa: F811
    from provisa.compiler.rls import read_rule

    real = table_read._surface_with

    def _ruled(state_: Any, role_id: str, drafts: list[dict]) -> Any:
        from dataclasses import replace

        surface = real(state_, role_id, drafts)
        rules = {**surface.rls.rules, _REFUNDS: read_rule("note = 'kept'")}
        return replace(surface, rls=replace(surface.rls, rules=rules))

    monkeypatch.setattr(table_read, "_surface_with", _ruled)
    with pytest.raises(_Routed):
        await _sent(_STATEMENT, "org_admin")
    governed = " ".join(str(a) for a in routing.await_args.args) + str(routing.await_args.kwargs)
    assert "'kept'" in governed


async def test_nothing_of_the_read_is_kept_and_no_other_role_reads_it_after(state, routing):  # noqa: F811
    tables_before, surface_before = list(state.tables), state.surfaces["org_admin"]
    with pytest.raises(_Routed):
        await _sent(_STATEMENT, "org_admin")
    routing.reset_mock()
    assert state.tables == tables_before and _REFUNDS not in [t["id"] for t in state.tables]
    assert state.surfaces["org_admin"] is surface_before
    surface = role_surface(state, "org_admin")
    assert surface is not None and "refunds" not in surface.context.tables
    assert not hasattr(state, "previewed_draft_ids")
    with pytest.raises(PermissionError, match="draft"):
        await _sent(_STATEMENT, "r")
    routing.assert_not_awaited()


async def test_the_read_is_recorded_as_a_draft_preview(state, routing, monkeypatch):  # noqa: F811
    from provisa.audit import pipeline as audit_pipeline

    opened: list = []
    real = audit_pipeline.begin_audit

    def _begin(*args: Any, **kwargs: Any) -> Any:
        from provisa.audit.context import audit_identity_scope

        with audit_identity_scope("u1", "pgwire"):
            record = real(*args, **kwargs)
        opened.append(record)
        return record

    monkeypatch.setattr(audit_pipeline, "begin_audit", _begin)
    with pytest.raises(_Routed):
        await _sent(_STATEMENT, "org_admin")
    (record,) = opened
    assert record.enforced["draft_preview"] == [_REFUNDS]


async def test_the_plan_runs_in_the_view_it_was_governed_in(state, monkeypatch):
    """The drafts' routes and engine face are the view's: the terminal runs the plan there, and
    no surface streams it from its own terminal around that."""
    from provisa.pgwire import _pipeline

    governed_in: list = []

    async def _govern(sql: str, role_id: str, **kw: Any) -> Any:
        governed_in.append(kw["state"])
        return _pipeline._Plan(route=None, sql=sql, source_id=None, dialect="postgres")

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    plan = await _pipeline._govern_and_route_planned(_STATEMENT, "org_admin")
    (seen,) = governed_in
    assert plan.reads_in is seen and seen.previewed_draft_ids == {_REFUNDS}
    assert not _pipeline.streams_from_terminal(plan)

    ran_in: list = []

    async def _bound(plan_: Any, state_: Any) -> Any:
        ran_in.append(state_)
        return SimpleNamespace(column_names=[], rows=[])

    monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _plan: None)
    monkeypatch.setattr(_pipeline, "_execute_plan_bound", _bound)
    await _pipeline._execute_plan(plan)
    assert ran_in == [seen]
