# Copyright (c) 2026 Kenneth Stott
# Canary: 1d25f057-2150-4068-8859-4a0e133c10ef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SQL plane offers the columns a remote GraphQL table's relation holds.

A remote type's field pointing at ONE object of another registered type (``employee``,
``breed``) is registered as a column, but the relation a statement reads is built without it
(``graphql_remote.relation_columns``). The pgwire catalog listed it, describe counted it, and
``SELECT *`` was expanded to it, so the statement failed in the engine ("does not have a column
named "employee"").
"""

# Requirements: REQ-127

from __future__ import annotations

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.stage2 import _expand_star, build_governance_context
from provisa.graphql_remote.relation_columns import table_columns_outside_relation
from provisa.pgwire.catalog_populate import _build_catalog_index
from provisa.compiler.result_shape import derive_result_shape
from tests.unit.test_profiler_remote_object_columns import (
    _SOURCES,
    PETS,
    PROFILE_ROLE,
    _engine,
    _state,
)

_HELD = ["id", "name", "employee_id", "breed_name", "tags"]


def _ctx() -> CompilationContext:
    state = _state()
    profile_ctx = state.contexts[PROFILE_ROLE]
    ctx = CompilationContext()
    ctx.tables = {"pets": profile_ctx.tables["pets"]}
    ctx.physical_to_sql = {k: v for k, v in profile_ctx.physical_to_sql.items() if k[0] == PETS}
    ctx.columns_outside_relation = table_columns_outside_relation(
        _SOURCES, {("graphql-demo", "pets"): PETS}
    )
    return ctx


def _column_types() -> dict:
    column_types = _state().schema_build_cache["column_types"]
    for col in column_types[PETS]:
        col.is_nullable = True
    return column_types


def test_the_rule_names_each_registered_tables_reference_columns_by_id():
    assert table_columns_outside_relation(_SOURCES, {("graphql-demo", "pets"): PETS}) == {
        (PETS, "employee"),
        (PETS, "breed"),
    }
    # A remote table with no registered table has nothing to read.
    assert table_columns_outside_relation(_SOURCES, {}) == set()


def test_the_catalog_lists_only_the_columns_the_relation_holds():
    idx = _build_catalog_index(_ctx(), _column_types(), database="provisa")
    toid = idx.table_id_to_oid[PETS]
    assert [c[1] for c in idx.all_cols if c[0] == toid] == _HELD


def test_select_star_is_described_as_the_columns_the_relation_holds():
    shape = derive_result_shape("SELECT * FROM d.pets", {"d.pets": PETS}, _ctx(), _column_types())
    assert [name for name, _ in shape] == _HELD


def test_select_star_expands_to_the_columns_the_relation_holds_and_runs():
    table = {
        "id": PETS,
        "source_id": "graphql-demo",
        "domain_id": "d",
        "columns": [
            {"column_name": c.column_name, "data_type": c.data_type, "visible_to": ["r"]}
            for c in _column_types()[PETS]
        ],
    }
    role = {"id": "r", "domain_access": ["*"], "capabilities": []}
    gov = build_governance_context("r", RLSContext.empty(), {}, _ctx(), [table], role)
    expanded = [e.sql() for e in _expand_star({"p": PETS}, gov)]
    assert expanded == [f'"p"."{name}"' for name in _HELD]
    assert len(_engine().execute(f"SELECT {', '.join(expanded)} FROM d.pets AS p").fetchall()) == 20
