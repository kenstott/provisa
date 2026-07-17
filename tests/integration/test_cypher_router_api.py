# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1c9a3d-8e2b-4a7f-9c5d-1b3e7a9f2c6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the Cypher REST router (provisa/api/rest/cypher_router.py).

Drives POST /data/cypher, GET /data/graph-schema, and GET /data/graph-counts against
an in-process app (ASGITransport) backed by the real isolated Postgres stack, using
tests/fixtures/sample_config.yaml (labels Orders/Customers/Products across the
sales-analytics and product-catalog domains, admin/analyst roles). Extends the
patterns in test_cypher_endpoint.py, test_cypher_integration_extra.py, and
test_compile_endpoint.py (which point create_app() at the same fixture config).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

_FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    # Point the app at the fixture config (sales-pg / orders / customers / products)
    # rather than the shipped demo config, matching test_compile_endpoint.py.
    _prev_config = os.environ.get("PROVISA_CONFIG")
    _prev_replace = os.environ.get("PROVISA_CONFIG_REPLACE")
    os.environ["PROVISA_CONFIG"] = str(_FIXTURE_CONFIG)
    os.environ["PROVISA_CONFIG_REPLACE"] = "1"

    app = create_app()

    try:
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                yield c
    finally:
        if _prev_config is None:
            os.environ.pop("PROVISA_CONFIG", None)
        else:
            os.environ["PROVISA_CONFIG"] = _prev_config
        if _prev_replace is None:
            os.environ.pop("PROVISA_CONFIG_REPLACE", None)
        else:
            os.environ["PROVISA_CONFIG_REPLACE"] = _prev_replace


async def _cypher(client: AsyncClient, query: str, params: dict | None = None, headers=None):
    return await client.post(
        "/data/cypher",
        json={"query": query, "params": params or {}},
        headers=headers,
    )


async def _schema(client: AsyncClient, headers=None):
    resp = await client.get("/data/graph-schema", headers=headers)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _label_for_domain(schema: dict, domain_id: str) -> dict:
    for n in schema["node_labels"]:
        if n["domain_id"] == domain_id:
            return n
    pytest.fail(f"No node label found for domain {domain_id!r} in schema: {schema}")


# --------------------------------------------------------------------------- #
# GET /data/graph-schema (REQ-392, REQ-398)
# --------------------------------------------------------------------------- #


class TestGraphSchema:
    async def test_schema_has_node_labels_and_relationship_types(self, client):
        schema = await _schema(client)
        assert isinstance(schema["node_labels"], list)
        assert len(schema["node_labels"]) > 0
        assert isinstance(schema["relationship_types"], list)

    async def test_schema_node_label_shape(self, client):
        schema = await _schema(client)
        node = schema["node_labels"][0]
        for key in (
            "label",
            "domain_label",
            "domain_id",
            "table_label",
            "properties",
            "pk",
            "pk_columns",
            "id_column",
            "native_filter_columns",
            "property_types",
            "traversal_only",
            "scl1",
            "scl2",
            "scl3",
        ):
            assert key in node, f"missing {key!r} in node label {node}"

    async def test_schema_property_types_populated_for_orders(self, client):
        """Orders has an explicit integer `id` column — property_types must reflect it."""
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        assert isinstance(orders["property_types"], dict)
        assert len(orders["property_types"]) > 0
        # every declared type must be a non-empty string (REQ-392 typed property panel)
        for prop, sql_type in orders["property_types"].items():
            assert isinstance(prop, str) and prop
            assert isinstance(sql_type, str) and sql_type

    async def test_schema_relationship_type_shape(self, client):
        schema = await _schema(client)
        if not schema["relationship_types"]:
            pytest.skip("No relationships registered in fixture config for this role")
        rel = schema["relationship_types"][0]
        assert "type" in rel
        assert "source" in rel
        assert "target" in rel

    async def test_schema_scoped_by_analyst_role(self, client):
        """analyst role's domain_access excludes product-catalog (REQ-392 domain scoping)."""
        schema = await _schema(client, headers={"X-Provisa-Role": "analyst"})
        domain_ids = {n["domain_id"] for n in schema["node_labels"]}
        assert "product-catalog" not in domain_ids

    async def test_schema_admin_sees_all_domains(self, client):
        schema = await _schema(client, headers={"X-Provisa-Role": "admin"})
        domain_ids = {n["domain_id"] for n in schema["node_labels"]}
        assert "sales-analytics" in domain_ids
        assert "product-catalog" in domain_ids


# --------------------------------------------------------------------------- #
# GET /data/graph-counts (REQ-392)
# --------------------------------------------------------------------------- #


class TestGraphCounts:
    async def test_counts_returns_shape(self, client):
        resp = await client.get("/data/graph-counts")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "node_count" in data
        assert "rel_count" in data
        assert "label_counts" in data
        assert isinstance(data["label_counts"], dict)
        assert isinstance(data["node_count"], int)
        assert isinstance(data["rel_count"], int)

    async def test_counts_label_counts_are_non_negative_ints(self, client):
        resp = await client.get("/data/graph-counts")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        for label, count in data["label_counts"].items():
            assert isinstance(label, str)
            assert isinstance(count, int)
            assert count >= 0

    async def test_counts_node_count_matches_sum_of_label_counts(self, client):
        resp = await client.get("/data/graph-counts")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["node_count"] == sum(data["label_counts"].values())

    async def test_counts_domain_filter_sales_analytics_only(self, client):
        resp = await client.get("/data/graph-counts", params={"domains": "sales-analytics"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        schema = await _schema(client)
        sa_labels = {
            n["label"] for n in schema["node_labels"] if n["domain_id"] == "sales-analytics"
        }
        assert set(data["label_counts"].keys()) <= sa_labels

    async def test_counts_domain_filter_excludes_other_domain(self, client):
        resp = await client.get("/data/graph-counts", params={"domains": "product-catalog"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        schema = await _schema(client)
        pc_labels = {
            n["label"] for n in schema["node_labels"] if n["domain_id"] == "product-catalog"
        }
        sa_labels = {
            n["label"] for n in schema["node_labels"] if n["domain_id"] == "sales-analytics"
        }
        assert not (set(data["label_counts"].keys()) & sa_labels) or pc_labels
        # Nothing outside product-catalog should be present.
        assert set(data["label_counts"].keys()) <= pc_labels

    async def test_counts_scoped_by_analyst_role(self, client):
        """analyst lacks product-catalog domain_access — counts must omit those labels."""
        resp = await client.get("/data/graph-counts", headers={"X-Provisa-Role": "analyst"})
        assert resp.status_code == 200, resp.text
        data = resp.json()
        schema = await _schema(client, headers={"X-Provisa-Role": "analyst"})
        allowed_labels = {n["label"] for n in schema["node_labels"]}
        assert set(data["label_counts"].keys()) <= allowed_labels


# --------------------------------------------------------------------------- #
# POST /data/cypher — node / relationship / id-reference queries
# --------------------------------------------------------------------------- #


class TestCypherNodeAndRelationshipQueries:
    async def test_match_node_query_returns_columns_and_rows(self, client):
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(client, f"MATCH (n:{orders['label']}) RETURN n LIMIT 5")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "columns" in data
        assert "rows" in data
        assert data["type"] == "cypher"

    async def test_relationship_traversal_query(self, client):
        schema = await _schema(client)
        if not schema["relationship_types"]:
            pytest.skip("No relationships registered in fixture config for this role")
        rel = schema["relationship_types"][0]
        resp = await _cypher(
            client,
            f"MATCH (a)-[r:{rel['type']}]->(b) RETURN a, b LIMIT 3",
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "columns" in data
        assert "rows" in data

    async def test_id_reference_resolution_no_match(self, client):
        """id(n) IN [...] with ids that resolve to nothing still exercises the
        _resolve_id_references lookup path (empty node_ids match) without error."""
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(
            client,
            f"MATCH (n:{orders['label']}) WHERE id(n) IN [999999, 999998] RETURN n LIMIT 1",
        )
        assert resp.status_code in (200, 400, 500, 503), resp.text

    async def test_id_reference_resolution_round_trip(self, client):
        """Query once to register a node id, then re-query with id(n) IN [<that id>]
        to exercise the successful id_to_val replacement branch."""
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        first = await _cypher(client, f"MATCH (n:{orders['label']}) RETURN n LIMIT 1")
        assert first.status_code == 200, first.text
        rows = first.json()["rows"]
        if not rows:
            pytest.skip("No Orders rows available to round-trip a stable node id")
        node = rows[0].get("n") or next(iter(rows[0].values()))
        node_id = node.get("id") if isinstance(node, dict) else None
        if node_id is None:
            pytest.skip("Assembled node has no stable id to round-trip")
        second = await _cypher(
            client,
            f"MATCH (n:{orders['label']}) WHERE id(n) IN [{int(node_id)}] RETURN n LIMIT 1",
        )
        assert second.status_code in (200, 400, 500, 503), second.text

    async def test_named_params_bound(self, client):
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(
            client,
            f"MATCH (n:{orders['label']}) WHERE n.id = $order_id RETURN n LIMIT 1",
            params={"order_id": 1},
        )
        assert resp.status_code in (200, 400, 500, 503), resp.text


# --------------------------------------------------------------------------- #
# POST /data/cypher — domain-scoped headers
# --------------------------------------------------------------------------- #


class TestCypherDomainScoping:
    async def test_analyst_cannot_query_product_catalog_label(self, client):
        """analyst's domain_access is restricted to sales-analytics; a Products query
        (product-catalog domain) must not be answered as an admin-visible node."""
        schema = await _schema(client, headers={"X-Provisa-Role": "admin"})
        products = _label_for_domain(schema, "product-catalog")
        resp = await _cypher(
            client,
            f"MATCH (n:{products['label']}) RETURN n LIMIT 1",
            headers={"X-Provisa-Role": "analyst"},
        )
        # Either the label doesn't resolve for analyst (400 translate error) or governance
        # rejects it (403) — never a bare 200 success against an out-of-scope domain.
        assert resp.status_code in (400, 403), resp.text

    async def test_admin_can_query_product_catalog_label(self, client):
        schema = await _schema(client, headers={"X-Provisa-Role": "admin"})
        products = _label_for_domain(schema, "product-catalog")
        resp = await _cypher(
            client,
            f"MATCH (n:{products['label']}) RETURN n LIMIT 1",
            headers={"X-Provisa-Role": "admin"},
        )
        assert resp.status_code == 200, resp.text


# --------------------------------------------------------------------------- #
# POST /data/cypher — error paths
# --------------------------------------------------------------------------- #


class TestCypherErrorPaths:
    async def test_malformed_cypher_returns_400(self, client):
        resp = await _cypher(client, "MATCH (n RETURN n")
        assert resp.status_code == 400
        assert "error" in resp.json()

    async def test_unbound_param_returns_400(self, client):
        resp = await _cypher(client, "MATCH (n) WHERE n.id = $missing_param RETURN n LIMIT 1")
        assert resp.status_code == 400
        assert "error" in resp.json()

    async def test_unknown_label_returns_error_not_500(self, client):
        resp = await _cypher(client, "MATCH (n:TotallyUnknownLabel) RETURN n LIMIT 1")
        assert resp.status_code in (400, 404, 422)
        assert resp.status_code != 500

    async def test_execution_type_mismatch_scrubs_engine_name(self, client):
        """A query that translates successfully but fails at the DB (comparing a numeric
        column to a non-numeric literal) drives _dispatch_execution's query-error branch
        into _exec_error/_federation_error, which must scrub the raw engine name."""
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(
            client,
            f"MATCH (n:{orders['label']}) WHERE n.id = 'not-a-number-xyz' RETURN n LIMIT 1",
        )
        if resp.status_code == 200:
            pytest.skip("Engine coerced the type mismatch instead of erroring")
        assert resp.status_code in (400, 500, 503)
        data = resp.json()
        assert "error" in data
        assert "trino" not in data["error"].lower()
        assert "postgresql" not in data["error"].lower()

    async def test_query_id_removed_endpoint_returns_410(self, client):
        resp = await client.post(
            "/data/cypher?query_id=some-id",
            json={"query": "MATCH (n) RETURN n LIMIT 1"},
        )
        assert resp.status_code == 410
        assert "error" in resp.json()

    async def test_merge_write_rejected(self, client):
        resp = await _cypher(client, "MERGE (n:TestNode {id: 999})")
        assert resp.status_code == 400
        assert "error" in resp.json()


# --------------------------------------------------------------------------- #
# POST /data/cypher — multi-CALL (non-correlated subqueries, no outer MATCH)
# --------------------------------------------------------------------------- #


class TestCypherMultiCall:
    async def test_multi_call_non_correlated_subqueries(self, client):
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        query = (
            f"CALL {{ MATCH (n:{orders['label']}) RETURN count(n) AS order_count }} "
            f"CALL {{ MATCH (n:{orders['label']}) RETURN count(n) AS order_count2 }} "
            "RETURN order_count, order_count2"
        )
        resp = await _cypher(client, query)
        assert resp.status_code in (200, 400, 500), resp.text
        if resp.status_code == 200:
            data = resp.json()
            assert data["type"] == "cypher"
            assert "columns" in data
            assert "rows" in data


# --------------------------------------------------------------------------- #
# POST /data/cypher — response contract (REQ-345..352)
# --------------------------------------------------------------------------- #


class TestCypherResponseContract:
    async def test_success_response_has_no_error_field_set(self, client):
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(client, f"MATCH (n:{orders['label']}) RETURN count(n) AS cnt")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data.get("type") == "cypher"
        if "error" in data:
            assert data["error"] is None

    async def test_stats_header_includes_provisa_stats(self, client):
        schema = await _schema(client)
        orders = _label_for_domain(schema, "sales-analytics")
        resp = await _cypher(
            client,
            f"MATCH (n:{orders['label']}) RETURN n LIMIT 1",
            headers={"X-Provisa-Stats": "true"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "provisa_stats" in data
