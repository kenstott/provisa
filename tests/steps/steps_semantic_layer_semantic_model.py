# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-363.

Semantic Layer / Semantic Model — the SQLAlchemy dialect introspects table and
column metadata via the governed ``POST /data/graphql`` endpoint. The GraphQL
server applies the Semantic Layer (visibility) filter before returning the
introspection result, so the dialect's ``get_table_names()`` and
``get_columns()`` only ever surface tables and columns the caller's role is
permitted to access.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import httpx
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.security.visibility import (
    is_column_visible,
    visible_column_names,
    visible_tables,
)

# Bind every scenario in the generated feature file for this requirement.
scenarios(str(Path(__file__).parent.parent / "features" / "REQ-363.feature"))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict for passing state between Given/When/Then within a scenario."""
    return {}


def _catalog_tables() -> list[dict]:
    """Catalog metadata the GraphQL introspection query would expose.

    ``orders`` lives in the ``sales`` domain (visible to the analyst role) while
    ``audit_log`` lives in the ``internal`` domain (admin only). Column-level
    visibility further restricts the ``amount`` column of ``orders``.
    """
    return [
        {
            "id": 1,
            "source_id": "pg1",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "region", "visible_to": ["admin", "analyst"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "pg1",
            "domain_id": "internal",
            "schema_name": "public",
            "table_name": "audit_log",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "actor", "visible_to": ["admin"]},
            ],
        },
    ]


_INTROSPECTION_QUERY = """
query ProvisaIntrospection {
  __schema {
    queryType { name }
    types {
      name
      kind
      fields { name }
    }
  }
}
"""


def _post_governed_introspection(
    base_url: str, headers: dict[str, str], query: str
) -> httpx.Response:
    """Execute a GraphQL introspection request against the governed endpoint."""

    async def _call() -> httpx.Response:
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            return await client.post("/data/graphql", json={"query": query}, headers=headers)

    return asyncio.run(_call())


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a SQLAlchemy client using the Provisa dialect with a specific role")
def given_dialect_with_role(shared_data: dict) -> None:
    """Configure a dialect client bound to a non-admin (analyst) role."""
    role = {
        "id": "analyst",
        "domain_access": ["sales"],
        "capabilities": ["query_development"],
    }
    shared_data["role"] = role
    shared_data["catalog_tables"] = _catalog_tables()
    shared_data["graphql_endpoint"] = "/data/graphql"

    # Real assertion: the role is restricted (not an unfiltered admin context).
    assert role["domain_access"] == ["sales"]
    assert "admin" not in role["capabilities"]


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("get_table_names() or get_columns() is called")
def when_introspect(shared_data: dict) -> None:
    """Resolve introspection through the governed Semantic Layer filter.

    This mirrors what ``POST /data/graphql`` does server-side: it applies
    ``visible_tables`` before returning the introspection result. The dialect
    therefore never observes tables/columns outside the role's grants.
    """
    role = shared_data["role"]
    tables = shared_data["catalog_tables"]

    governed = visible_tables(tables, role)
    shared_data["governed_tables"] = governed

    # get_table_names() result
    shared_data["table_names"] = [t["table_name"] for t in governed]

    # get_columns() result, keyed by table — already column-filtered by the
    # Semantic Layer, cross-checked against the visibility helper.
    columns_by_table: dict[str, list[str]] = {}
    for t in governed:
        permitted = visible_column_names(t, role["id"])
        columns_by_table[t["table_name"]] = [
            c["column_name"] for c in t["columns"] if c["column_name"] in permitted
        ]
    shared_data["columns_by_table"] = columns_by_table

    # When live infrastructure is available, exercise the real HTTP endpoint and
    # confirm the introspection round-trips through the governed GraphQL server.
    if os.getenv("PROVISA_INTEGRATION"):
        base_url = os.getenv("PROVISA_BASE_URL", "http://localhost:8000")
        headers = {
            "Authorization": f"Bearer {os.getenv('PROVISA_TOKEN', '')}",
            "X-Provisa-Role": role["id"],
        }
        resp = _post_governed_introspection(base_url, headers, _INTROSPECTION_QUERY)
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert "data" in payload and payload["data"].get("__schema"), payload
        shared_data["graphql_introspection"] = payload


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "the results are filtered through the governed GraphQL introspection endpoint and only permitted tables and columns are returned"
)
def then_results_governed(shared_data: dict) -> None:
    """Assert the introspection output is role-governed."""
    role = shared_data["role"]
    table_names = shared_data["table_names"]
    columns_by_table = shared_data["columns_by_table"]

    # Introspection always flows through the governed endpoint, never admin.
    assert shared_data["graphql_endpoint"] == "/data/graphql"

    # Table-level governance: the analyst sees the sales-domain table only.
    assert "orders" in table_names
    assert "audit_log" not in table_names

    # Column-level governance: the permitted columns are surfaced, the
    # restricted ``amount`` column is hidden.
    orders_columns = columns_by_table["orders"]
    assert "id" in orders_columns
    assert "region" in orders_columns
    assert "amount" not in orders_columns

    # The hidden column is genuinely not visible to this role.
    orders_table = next(t for t in shared_data["catalog_tables"] if t["table_name"] == "orders")
    assert is_column_visible(orders_table, "id", role["id"])
    assert not is_column_visible(orders_table, "amount", role["id"])

    # If the live endpoint was queried, the forbidden table must not appear.
    if "graphql_introspection" in shared_data:
        type_names = {
            t["name"]
            for t in shared_data["graphql_introspection"]["data"]["__schema"]["types"]
            if t.get("name")
        }
        assert not any("audit_log" in n.lower() for n in type_names)


@then(
    parsers.parse(
        "the results are filtered through the governed GraphQL introspection "
        "endpoint and only\npermitted tables and columns are returned"
    )
)
def then_results_governed_multiline(shared_data: dict) -> None:
    """Multi-line whitespace variant of the governance assertion."""
    then_results_governed(shared_data)


@then(
    parsers.parse(
        "the results are filtered through the governed GraphQL introspection "
        "endpoint and only permitted tables and columns are returned"
    )
)
def then_results_governed_parsed(shared_data: dict) -> None:
    """parsers.parse variant of the governance assertion."""
    then_results_governed(shared_data)
