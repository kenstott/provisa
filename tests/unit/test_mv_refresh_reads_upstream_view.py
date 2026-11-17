# Copyright (c) 2026 Kenneth Stott
# Canary: cce0e957-4bc8-422a-a7dd-4baed8bfc9e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-966/REQ-133/REQ-135/REQ-1922: a materialized view built on another view (pets -> A -> B).

B's build is its definition through the one pipeline as the administrator, so it reads A by the
one view rule (``mv.view_read``) a reader's statement does: A is answered from its stored rows
when it is materialized, its build is fresh and the administrator sees it whole; otherwise A is
expanded into the statement as its own SQL and governed with it, reading every table it reads as
a direct statement would (a search table under its arguments among them). Its reference never
reaches the engine as a view name with no catalog."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.view_read import semantic_statement, unnarrowed_view_bodies

pytestmark = pytest.mark.unit

A_SQL = "SELECT id, price FROM pet_store.pets"
B_SQL = "SELECT id, price FROM e2e_mv_a"
VIEWS = {"e2e_mv_a": A_SQL, "e2e_mv_b": B_SQL}


def _mv(name: str, sql: str, status: MVStatus) -> MVDefinition:
    return MVDefinition(
        id=f"view-{name}",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="org_x_mv_cache",
        target_table=f"mv_{name}",
        refresh_interval=300,
        enabled=True,
        sql=sql,
        semantic_sql=sql,
        status=status,
    )


def _state(a_status: MVStatus) -> SimpleNamespace:
    registry = MVRegistry()
    registry.register(_mv("e2e_mv_a", A_SQL, a_status))
    registry.register(_mv("e2e_mv_b", B_SQL, MVStatus.STALE))
    return SimpleNamespace(
        mv_registry=registry, role_build_inputs={"draft_tables": [], "withdrawn_tables": []}
    )


def _admin() -> GovernanceContext:
    """The administrator's governance: nothing narrowed on any table."""
    gov = GovernanceContext()
    gov.table_map = {"pet_store.pets": 1, "pets": 1, "e2e_mv_a": 2, "e2e_mv_b": 3}
    return gov


def _relations(sql: str) -> set[str]:
    import sqlglot
    from sqlglot import exp

    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


def _build(state: SimpleNamespace) -> str:
    """B's definition as the pipeline reads it: one semantic statement, then optimization."""
    from provisa.compiler.view_expand import expand_view_refs

    governed, _validated = semantic_statement(B_SQL, VIEWS, state, _admin())
    return expand_view_refs(governed, unnarrowed_view_bodies(governed, VIEWS, state))


def test_a_view_on_a_fresh_materialized_view_is_built_from_its_stored_rows():
    state = _state(MVStatus.FRESH)
    governed, _ = semantic_statement(B_SQL, VIEWS, state, _admin())
    assert _relations(governed) == {"e2e_mv_a"}  # kept through governance
    assert _relations(_build(state)) == {"mat_store.org_x_mv_cache.mv_e2e_mv_a"}


@pytest.mark.parametrize("status", [MVStatus.STALE, MVStatus.REFRESHING])
def test_a_view_on_a_view_that_is_not_stored_fresh_is_built_from_its_sql(status):
    """Expanded at the semantic stage, so governance reads A's own inputs, not after it."""
    state = _state(status)
    governed, _ = semantic_statement(B_SQL, VIEWS, state, _admin())
    assert _relations(governed) == {"pet_store.pets"}
    assert _relations(_build(state)) == {"pet_store.pets"}
