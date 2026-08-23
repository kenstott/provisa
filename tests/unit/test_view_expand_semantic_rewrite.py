# Copyright (c) 2026 Kenneth Stott
# Canary: 1f2e3d4c-5b6a-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Regression: view bodies referencing federated tables must have semantic refs
lowered to catalog-physical AFTER view expansion.

Bug: expand_view_refs runs AFTER rewrite_semantic_to_catalog_physical in _pipeline.py,
so semantic refs in the view body (e.g. pet_store.pets) survive into the DuckDB execution
step and cause 'schema does not exist' errors.

Fix: apply normalize_table_refs + rewrite_semantic_to_catalog_physical again after
expand_view_refs so inlined view body refs are lowered to catalog-physical.
"""

import pytest

from provisa.compiler.sql_rewrite import (
    normalize_table_refs,
    rewrite_semantic_to_catalog_physical,
)
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.view_expand import expand_view_refs


@pytest.fixture()
def sqlite_ctx() -> CompilationContext:
    """Minimal context for a SQLite-backed table: semantic pet_store.pets →
    catalog-physical "pet_store_sqlite"."pet_store"."pets"."""
    meta = TableMeta(
        table_id=1,
        field_name="pets",
        type_name="Pets",
        source_id="pet-store-sqlite",
        catalog_name="pet_store_sqlite",
        schema_name="pet_store",
        table_name="pets",
        domain_id="pet_store",
    )
    return CompilationContext(tables={"pets": meta})


def test_expand_view_with_unquoted_semantic_ref_leaves_semantic_ref(sqlite_ctx):
    """Without a second rewrite pass, the expanded view body still contains the
    unquoted semantic ref that DuckDB cannot resolve."""
    view_sql_map = {"e2e_mv_a": "SELECT id, name, price FROM pet_store.pets WHERE id = 1"}
    # Simulate outer SQL after first normalize+rewrite (view ref itself has no catalog form)
    outer = "SELECT * FROM (SELECT id, name, price FROM e2e_mv_a) _sample LIMIT 100"
    expanded = expand_view_refs(outer, view_sql_map)
    # Unquoted semantic ref appears in the expanded body — this is what DuckDB chokes on
    assert "pet_store.pets" in expanded


def test_expand_view_then_rewrite_produces_catalog_physical(sqlite_ctx):
    """After expansion, a second normalize+rewrite pass lowers semantic refs to catalog-physical."""
    view_sql_map = {"e2e_mv_a": "SELECT id, name, price FROM pet_store.pets WHERE id = 1"}
    outer = "SELECT * FROM (SELECT id, name, price FROM e2e_mv_a) _sample LIMIT 100"
    expanded = expand_view_refs(outer, view_sql_map)
    # Apply the second pass (the fix)
    fixed = rewrite_semantic_to_catalog_physical(
        normalize_table_refs(expanded, sqlite_ctx), sqlite_ctx
    )
    assert '"pet_store_sqlite"."pet_store"."pets"' in fixed
    assert "pet_store.pets" not in fixed


def test_expand_view_with_quoted_semantic_ref_also_fixed(sqlite_ctx):
    """Quoted semantic ref "pet_store"."pets" in a view body is also lowered."""
    import re

    view_sql_map = {"e2e_mv_a": 'SELECT id FROM "pet_store"."pets"'}
    outer = "SELECT * FROM (SELECT id FROM e2e_mv_a) _sample LIMIT 100"
    expanded = expand_view_refs(outer, view_sql_map)
    fixed = rewrite_semantic_to_catalog_physical(
        normalize_table_refs(expanded, sqlite_ctx), sqlite_ctx
    )
    assert '"pet_store_sqlite"."pet_store"."pets"' in fixed
    # "pet_store"."pets" must not appear as a standalone 2-part ref
    # (it will appear as a substring of the catalog-physical form, which is fine)
    assert not re.search(r'(?<!"pet_store_sqlite"\.)("pet_store"."pets")', fixed)


@pytest.fixture()
def org_scoped_sqlite_ctx() -> CompilationContext:
    """Same source under a NON-default org: REQ-1266 prefixes the engine catalog."""
    meta = TableMeta(
        table_id=1,
        field_name="pets",
        type_name="Pets",
        source_id="pet-store-sqlite",
        catalog_name="org_kstott__pet_store_sqlite",
        schema_name="pet_store",
        table_name="pets",
        domain_id="pet_store",
    )
    return CompilationContext(tables={"pets": meta})


def test_base_catalog_ref_rewrites_to_org_catalog(org_scoped_sqlite_ctx):
    """A hand-authored view_sql addresses the source by its BASE catalog name.

    Regression: the base-catalog spelling was unanchored, so the shorter "schema"."table"
    key matched its tail and spliced the org catalog into the middle, producing the 4-part
    name Trino rejects with 'Too many dots in table name'.
    """
    view_sql_map = {
        "view_dim_pet": 'SELECT "id", "name" FROM "pet_store_sqlite"."pet_store"."pets"'
    }
    outer = "SELECT * FROM (SELECT id, name FROM view_dim_pet) _s LIMIT 100"
    expanded = expand_view_refs(outer, view_sql_map)
    fixed = rewrite_semantic_to_catalog_physical(expanded, org_scoped_sqlite_ctx)
    assert '"org_kstott__pet_store_sqlite"."pet_store"."pets"' in fixed
    assert '"pet_store_sqlite"."org_kstott__pet_store_sqlite"' not in fixed


def test_org_catalog_ref_is_idempotent(org_scoped_sqlite_ctx):
    """An already org-qualified ref survives a second rewrite pass unchanged."""
    sql = 'SELECT "id" FROM "org_kstott__pet_store_sqlite"."pet_store"."pets"'
    assert rewrite_semantic_to_catalog_physical(sql, org_scoped_sqlite_ctx) == sql
