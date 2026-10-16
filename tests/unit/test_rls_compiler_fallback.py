# Copyright (c) 2026 Kenneth Stott
# Canary: a4165f9a-98e4-4996-b070-096ac6045b2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Spec-named coverage for REQ-403 — RLS rule resolution fallback.

A table's row filter is its own table rule when it has one, else its domain's rule. The rule
list is folded to one table-level rule per (role, table) at runtime build
(``security/row_filters.resolve_table_rules``), the role's RLSContext is built from that
(``build_rls_context``), and the governed pipeline (``compiler/stage2``) applies it. The broader
domain-RLS load path is covered in ``test_rls.py::TestDomainRLS``.
"""

from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.security.row_filters import resolve_table_rules
from tests.helpers import governed_for_role

_SQL = 'SELECT "id" FROM "public"."orders"'


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


def _rls(rules: list[dict], meta: TableMeta) -> RLSContext:
    tables = [{"id": meta.table_id, "domain_id": meta.domain_id}]
    return build_rls_context(resolve_table_rules(rules, tables, {"a": ["a"]}, [], {}), "a")


def _governed(rules: list[dict], meta: TableMeta, sql: str = _SQL) -> str:
    return governed_for_role(sql, _ctx(meta), "a", rls=_rls(rules, meta))


_OWNER = {"table_id": 1, "domain_id": None, "role_id": "a", "filter_expr": "\"owner\" = 'me'"}
_SALES = {
    "table_id": None,
    "domain_id": "sales",
    "role_id": "a",
    "filter_expr": "\"region\" = 'us'",
}


def test_table_rule_used_when_present():
    assert _governed([_OWNER], _meta()) == f'{_SQL} WHERE ("orders"."owner" = \'me\')'


def test_domain_rule_used_as_fallback():
    assert _governed([_SALES], _meta()) == f'{_SQL} WHERE ("orders"."region" = \'us\')'


def test_table_rule_takes_precedence_over_domain_rule():
    assert _governed([_OWNER, _SALES], _meta()) == (f'{_SQL} WHERE ("orders"."owner" = \'me\')')


def test_no_rule_for_table_or_domain_leaves_sql_unchanged():
    rules = [{"table_id": 99, "domain_id": None, "role_id": "a", "filter_expr": "x = 1"}]
    assert _governed(rules, _meta(domain_id="other")) == _SQL


def test_domain_rule_does_not_apply_when_table_in_other_domain():
    assert _governed([_SALES], _meta(domain_id="marketing")) == _SQL


def test_an_aliased_root_keeps_its_field_through_rls():
    """The statement for ``a: orders`` is governed by the ``orders`` table's rule — the rule is
    found by the table the statement reads, whatever the client named the root (REQ-544)."""
    sql = 'SELECT "t0"."id" AS "id" FROM "public"."orders" "t0"'
    assert _governed([_OWNER], _meta(), sql) == (
        'SELECT "t0"."id" AS "id" FROM "public"."orders" AS "t0" WHERE ("t0"."owner" = \'me\')'
    )
