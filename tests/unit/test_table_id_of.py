# Copyright (c) 2026 Kenneth Stott
# Canary: bc264258-f0ac-4dd0-90de-67c08d8f43df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's table reference names its registered table by its catalog-physical name first
(naming.table_id_of). A view's body is read into a statement lowered to physical names, and two
sources may hold tables of one ``schema.table`` (the retail demo's EU and US ``public.customers``):
resolved without the catalog, the EU reference named the US table, so the statement's residency
never attached the EU replica, and governance judged the EU table by the US table's rules."""

# Requirements: REQ-1922, REQ-1921, REQ-272

from __future__ import annotations

from types import SimpleNamespace

import sqlglot
from sqlglot import exp

from provisa.compiler.naming import physical_relation_name, table_id_of

_EU, _US = 38, 42
_MAP = {
    # As the governance table map keys them: semantic names, schema.table, bare names ...
    "retail.eu_customers": _EU,
    "retail.us_customers": _US,
    "public.customers": _US,  # the later registration of the two wins the shared key
    "customers": _US,
    # ... and each one's catalog-physical name.
    physical_relation_name("eu_shop", "public", "customers"): _EU,
    physical_relation_name("us_shop", "public", "customers"): _US,
}


def _tables(sql: str) -> list[exp.Table]:
    return list(sqlglot.parse_one(sql, read="postgres").find_all(exp.Table))


def test_a_physical_reference_names_its_own_sources_table():
    eu, us = _tables(
        'SELECT 1 FROM "eu_shop"."public"."customers" AS e JOIN "us_shop"."public"."customers" AS u '
        "ON e.email = u.email"
    )
    assert (table_id_of(eu, _MAP), table_id_of(us, _MAP)) == (_EU, _US)


def test_a_semantic_or_bare_reference_resolves_as_before():
    semantic, bare = _tables('SELECT 1 FROM "retail"."eu_customers", customers')
    assert (table_id_of(semantic, _MAP), table_id_of(bare, _MAP)) == (_EU, _US)


def test_the_tables_a_statement_reads_are_its_own():
    from provisa.audit.pipeline import resolve_table_ids

    tree = sqlglot.parse_one(
        'SELECT 1 FROM "group_ref"."public"."subject_requests" AS r '
        'JOIN "eu_shop"."public"."customers" AS c ON c.email = r.email',
        read="postgres",
    )
    assert resolve_table_ids(tree, SimpleNamespace(table_map=_MAP)) == [_EU]


def _table(table_id: int, source: str, alias: str) -> dict:
    return {
        "id": table_id,
        "source_id": source,
        "domain_id": "retail",
        "schema_name": "public",
        "table_name": "customers",
        "alias": alias,
        "columns": [{"column_name": "email", "visible_to": ["dpo"], "native_filter_type": None}],
    }


def test_governance_tells_the_two_customers_tables_apart_in_a_lowered_view_body():
    """build_governance_context keys each table by its catalog-physical name, so a lowered
    reference to the EU table is governed as the EU table."""
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.compiler.stage2 import build_governance_context

    tables = [_table(_EU, "eu-shop", "eu_customers"), _table(_US, "us-shop", "us_customers")]
    ctx = CompilationContext()
    ctx.tables = {
        t["alias"]: TableMeta(
            table_id=t["id"],
            field_name=t["alias"],
            type_name=t["alias"].title(),
            source_id=t["source_id"],
            catalog_name=t["source_id"].replace("-", "_"),
            schema_name="public",
            table_name="customers",
            domain_id="retail",
        )
        for t in tables
    }
    role = {"id": "dpo", "domain_access": ["*"], "capabilities": ["query_development"]}
    gov = build_governance_context("dpo", RLSContext.empty(), {}, ctx, tables, role)
    eu, us = _tables(
        'SELECT 1 FROM "eu_shop"."public"."customers" AS e, "us_shop"."public"."customers" AS u'
    )
    assert (table_id_of(eu, gov.table_map), table_id_of(us, gov.table_map)) == (_EU, _US)
