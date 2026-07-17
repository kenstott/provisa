# Copyright (c) 2026 Kenneth Stott
# Canary: 976d127e-a57c-4069-b34a-17dc3228ece2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-367/418: cross-domain data may only enter a domain via a view.

These pin the enforcement mechanism: a role can only query tables in its
domain_access (V001). Another domain's table is therefore unreachable directly —
it can only be consumed through an import view registered in the role's own
domain (the view is a table that lives in the role's domain). New/derived data
likewise exists only as a view in the owning domain.
"""

from __future__ import annotations

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext


def _meta(table_id: int, table_name: str, domain_id: str, source_id: str = "pg") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
        domain_id=domain_id,
        source_type="postgresql",
    )


def _gov(*pairs: tuple[str, int]) -> GovernanceContext:
    gov = GovernanceContext()
    for name, tid in pairs:
        gov.table_map[name] = tid
    return gov


def _role(*domains: str) -> dict:
    return {"id": "analyst", "capabilities": ["query_development"], "domain_access": list(domains)}


def _v001(sql, ctx, gov, role):
    return [v for v in validate_sql(sql, ctx, gov, role, []) if v.code == "V001"]


class TestDomainAccessEnforcement:
    def test_direct_cross_domain_query_blocked(self):
        # finance-domain table queried by a sales-only role → V001.
        ctx = CompilationContext()
        ctx.tables = {"revenue": _meta(1, "revenue", domain_id="finance")}
        gov = _gov(("public.revenue", 1))
        sql = 'SELECT "r"."id" FROM "public"."revenue" AS "r"'
        assert _v001(sql, ctx, gov, _role("sales")), "cross-domain direct access must violate V001"

    def test_same_domain_query_passes(self):
        ctx = CompilationContext()
        ctx.tables = {"orders": _meta(1, "orders", domain_id="sales")}
        gov = _gov(("public.orders", 1))
        sql = 'SELECT "o"."id" FROM "public"."orders" AS "o"'
        assert _v001(sql, ctx, gov, _role("sales")) == []

    def test_import_view_in_role_domain_passes(self):
        # An import view registered in the role's own domain (sales) that surfaces
        # finance data is reachable — cross-domain data enters via the view.
        ctx = CompilationContext()
        ctx.tables = {
            "finance_revenue": _meta(1, "finance_revenue", domain_id="sales", source_id="__provisa__")
        }
        gov = _gov(("public.finance_revenue", 1))
        sql = 'SELECT "v"."id" FROM "public"."finance_revenue" AS "v"'
        assert _v001(sql, ctx, gov, _role("sales")) == []

    def test_wildcard_domain_access_bypasses(self):
        ctx = CompilationContext()
        ctx.tables = {"revenue": _meta(1, "revenue", domain_id="finance")}
        gov = _gov(("public.revenue", 1))
        sql = 'SELECT "r"."id" FROM "public"."revenue" AS "r"'
        assert _v001(sql, ctx, gov, _role("*")) == []
