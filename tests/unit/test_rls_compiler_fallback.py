# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Spec-named coverage for REQ-403 — RLS rule resolution fallback.

`_rule_for_table` (inside `inject_rls`) resolves the filter for a table by checking
the table-scoped rule first, then falling back to the table's domain rule. These tests
exercise that precedence through the public `inject_rls`; the broader domain-RLS load
path is covered in `test_rls.py::TestDomainRLS`.
"""

from provisa.compiler.rls import build_rls_context, inject_rls
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)


def _meta(domain_id="sales"):
    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id=domain_id,
    )


def _ctx(meta):
    ctx = CompilationContext()
    ctx.tables = {"orders": meta}
    ctx.joins = {}
    return ctx


def _compiled():
    return CompiledQuery(
        sql='SELECT "id" FROM "public"."orders"',
        params=[],
        root_field="orders",
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )


def test_table_rule_used_when_present():
    rls = build_rls_context(
        [{"table_id": 1, "domain_id": None, "role_id": "a", "filter_expr": "\"owner\" = 'me'"}], "a"
    )
    result = inject_rls(_compiled(), _ctx(_meta()), rls)
    assert "\"owner\" = 'me'" in result.sql


def test_domain_rule_used_as_fallback():
    rls = build_rls_context(
        [
            {
                "table_id": None,
                "domain_id": "sales",
                "role_id": "a",
                "filter_expr": "\"region\" = 'us'",
            }
        ],
        "a",
    )
    result = inject_rls(_compiled(), _ctx(_meta()), rls)
    assert "\"region\" = 'us'" in result.sql


def test_table_rule_takes_precedence_over_domain_rule():
    rls = build_rls_context(
        [
            {"table_id": 1, "domain_id": None, "role_id": "a", "filter_expr": "\"owner\" = 'me'"},
            {
                "table_id": None,
                "domain_id": "sales",
                "role_id": "a",
                "filter_expr": "\"region\" = 'us'",
            },
        ],
        "a",
    )
    result = inject_rls(_compiled(), _ctx(_meta()), rls)
    assert "\"owner\" = 'me'" in result.sql
    assert "\"region\" = 'us'" not in result.sql


def test_no_rule_for_table_or_domain_leaves_sql_unchanged():
    rls = build_rls_context(
        [{"table_id": 99, "domain_id": None, "role_id": "a", "filter_expr": "x = 1"}], "a"
    )
    compiled = _compiled()
    result = inject_rls(compiled, _ctx(_meta(domain_id="other")), rls)
    assert result.sql == compiled.sql


def test_domain_rule_does_not_apply_when_table_in_other_domain():
    rls = build_rls_context(
        [
            {
                "table_id": None,
                "domain_id": "sales",
                "role_id": "a",
                "filter_expr": "\"region\" = 'us'",
            }
        ],
        "a",
    )
    result = inject_rls(_compiled(), _ctx(_meta(domain_id="marketing")), rls)
    assert "\"region\" = 'us'" not in result.sql
