# Copyright (c) 2026 Kenneth Stott
# Canary: 808c75c1-87f2-40e2-a431-3d51f4c0872f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement reading a view is ONE semantic statement: the view is replaced by its SQL, and
governance judges the whole of it for the reader — the relationship guard (REQ-603) included.
A view joining two tables outside the approved relationships is refused to a reader who may not
ignore relationships, and served to one who may."""

# Requirements: REQ-603, REQ-135

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio


def _meta(table_id: int, name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="sales",
        table_name=name,
        domain_id="sales",
    )


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "schema_name": "sales",
        "table_name": name,
        "domain_id": "sales",
        "columns": [
            {"column_name": c, "data_type": "integer", "visible_to": ["analyst"]} for c in columns
        ],
    }


# orders and customers have NO approved relationship; the view joins them anyway.
_VIEW_SQL = "SELECT o.id FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"


def _state(role: dict) -> SimpleNamespace:
    ctx = CompilationContext()
    ctx.tables = {
        "orders": _meta(1, "orders"),
        "customers": _meta(2, "customers"),
        "orders_customers": _meta(3, "orders_customers"),
    }
    return RoleState(
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext.empty()},
        roles={"analyst": role},
        masking_rules={},
        tables=[
            _table(1, "orders", ["id", "customer_id"]),
            _table(2, "customers", ["id"]),
            _table(3, "orders_customers", ["id"]),
        ],
        view_sql_map={"orders_customers": _VIEW_SQL},
        mv_registry=SimpleNamespace(get=lambda _key: None),
        role_build_inputs={"draft_tables": [], "withdrawn_tables": []},
        source_types={"pg": "postgresql"},
        source_pools=SimpleNamespace(source_ids=["pg"]),
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
    )


async def _read_view(monkeypatch, role: dict):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(role), raising=False)
    return await _pipeline._govern_and_route("SELECT id FROM sales.orders_customers", "analyst")


async def test_a_view_joining_outside_the_relationships_is_refused_to_its_reader(monkeypatch):
    analyst = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}

    with pytest.raises(PermissionError) as exc_info:
        await _read_view(monkeypatch, analyst)

    assert "V002" in str(exc_info.value)


async def test_a_reader_who_may_ignore_relationships_reads_the_view(monkeypatch):
    """This stand-in state stops short of route selection, so the call still raises; what matters
    is that governance clears it without a V002."""
    analyst = {"id": "analyst", "capabilities": ["ignore_relationships"], "domain_access": ["*"]}

    with pytest.raises(Exception) as exc_info:  # noqa: PT011 — asserting it is NOT the V002 denial
        await _read_view(monkeypatch, analyst)

    assert "V002" not in str(exc_info.value)
