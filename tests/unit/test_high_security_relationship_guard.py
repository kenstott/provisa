# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7c1e94-2a55-4d18-9c60-5f0a8e21d4b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-693 + REQ-603: high-security mode pins the relationship guard ON.

The guard (V002) refuses any join the approved relationship catalog does not cover. Two things
normally turn it off: the ``ignore_relationships`` capability, and a role whose
``relationship_guard`` flag is cleared combined with a ``--relationship-guard=false`` SQL comment.
In high-security mode both are IGNORED — belts and suspenders, so that a production deployment
which improperly granted the discovery capability to a role does not thereby get a break-out from
the model.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta

pytestmark = pytest.mark.asyncio

# Two tables in the same domain with NO approved join between them: any join across them is a V002.
_ORDERS = TableMeta(
    table_id=1,
    field_name="orders",
    type_name="Orders",
    source_id="pg",
    catalog_name="pg",
    schema_name="sales",
    table_name="orders",
    domain_id="sales",
)
_CUSTOMERS = TableMeta(
    table_id=2,
    field_name="customers",
    type_name="Customers",
    source_id="pg",
    catalog_name="pg",
    schema_name="sales",
    table_name="customers",
    domain_id="sales",
)

_TABLES = [
    {
        "id": 1,
        "source_id": "pg",
        "schema_name": "sales",
        "table_name": "orders",
        "domain_id": "sales",
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["modeler"]},
            {"column_name": "customer_id", "data_type": "integer", "visible_to": ["modeler"]},
        ],
    },
    {
        "id": 2,
        "source_id": "pg",
        "schema_name": "sales",
        "table_name": "customers",
        "domain_id": "sales",
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["modeler"]},
        ],
    },
]

_UNAPPROVED_JOIN = (
    "SELECT o.id FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"
)


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"orders": _ORDERS, "customers": _CUSTOMERS}
    return ctx


def _state(*, role: dict, security_high: bool) -> SimpleNamespace:
    return SimpleNamespace(
        contexts={"modeler": _ctx()},
        rls_contexts={"modeler": RLSContext.empty()},
        roles={"modeler": role},
        masking_rules={},
        tables=_TABLES,
        source_types={"pg": "postgresql"},
        source_pools=SimpleNamespace(source_ids=["pg"]),
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        security_high=security_high,
    )


async def _route(monkeypatch, *, role: dict, security_high: bool, sql: str = _UNAPPROVED_JOIN):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(
        app_mod, "state", _state(role=role, security_high=security_high), raising=False
    )
    return await _pipeline._govern_and_route(sql, "modeler")


_MODELER = {"id": "modeler", "capabilities": ["ignore_relationships"], "domain_access": ["*"]}
_GUARD_OFF = {"id": "modeler", "capabilities": [], "domain_access": ["*"], "relationship_guard": False}


async def test_ignore_relationships_bypasses_the_guard_in_ordinary_mode(monkeypatch):
    """The baseline the pin has to overturn: the grant works when security is not high.

    This fake state stops short of route selection, so the call still raises — what matters is
    that it clears governance without a V002, which is what the high-security cases assert it
    does NOT do.
    """
    with pytest.raises(Exception) as exc_info:  # noqa: PT011 — asserting it is NOT the V002 denial
        await _route(monkeypatch, role=_MODELER, security_high=False)

    assert "V002" not in str(exc_info.value)


async def test_ignore_relationships_is_ignored_in_high_security_mode(monkeypatch):
    with pytest.raises(PermissionError) as exc_info:
        await _route(monkeypatch, role=_MODELER, security_high=True)

    assert "V002" in str(exc_info.value)


async def test_the_relationship_guard_comment_is_ignored_in_high_security_mode(monkeypatch):
    """The other opt-out — a cleared role flag plus the SQL comment — is pinned shut too."""
    opted_out = f"-- relationship-guard=false\n{_UNAPPROVED_JOIN}"

    with pytest.raises(PermissionError) as exc_info:
        await _route(monkeypatch, role=_GUARD_OFF, security_high=True, sql=opted_out)

    assert "V002" in str(exc_info.value)
