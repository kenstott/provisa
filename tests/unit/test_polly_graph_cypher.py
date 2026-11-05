# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9e2d71-3c58-4b06-8f19-0d7e5b3a6c82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's Cypher on the Graph page (REQ-1848, REQ-1795), on a model shaped as the weblogs demo's
shop: customers, orders, order items and products, related by registered relationships.

cypher_field_names hands Polly each label as a MATCH writes it -- a domain table's is two labels,
domain then table, never backticked -- and the table's typed, directed relationships, so it does
not invent untyped ones or join on columns. A Cypher statement it would put on the Graph page is
checked first, by the steps the Cypher endpoint runs before executing anything: one that does not
run never reaches the page, and Polly is answered with the real error.
"""

# Requirements: REQ-1848, REQ-1795

from __future__ import annotations

import pytest

from provisa.api.mcp import tools
from provisa.api.mcp.cypher_check import check_graph_read, graph_query
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from tests.role_state import RoleState
from tests.unit.test_group_by import _col

pytestmark = pytest.mark.asyncio


async def cypher_read_problem(state, role, query):
    return (await check_graph_read(state, role, query)).problem


_ROLE = "analyst"
_COLUMNS = {
    1: ("customers", ["id", "name", "region"]),
    2: ("orders", ["id", "customer_id", "status"]),
    3: ("order_items", ["id", "order_id", "product_id", "quantity"]),
    4: ("products", ["id", "name", "category"]),
}
_RELATIONSHIPS = [
    ("order-to-customer", 2, "customer_id", 1, "PLACED_BY"),
    ("item-to-order", 3, "order_id", 2, "ON_ORDER"),
    ("item-to-product", 3, "product_id", 4, "OF_PRODUCT"),
]


def _state() -> RoleState:
    """The shop model, built as the server builds it, as the state a request reads."""
    _naming.configure(gql="apollo_graphql", sql="snake")
    tables = [
        {
            "id": table_id,
            "source_id": "shop-db",
            "domain_id": "shop",
            "schema_name": "shop",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": [_ROLE]} for c in columns],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
        for table_id, (name, columns) in _COLUMNS.items()
    ]
    column_types = {
        table_id: [
            _col(c, "varchar(100)" if c in ("name", "region", "status", "category") else "integer")
            for c in columns
        ]
        for table_id, (_name, columns) in _COLUMNS.items()
    }
    relationships = [
        {
            "id": rel_id,
            "source_table_id": source,
            "target_table_id": target,
            "source_column": column,
            "target_column": "id",
            "cardinality": "many-to-one",
            "alias": alias,
        }
        for rel_id, source, column, target, alias in _RELATIONSHIPS
    ]
    role = {"id": _ROLE, "capabilities": [], "domain_access": ["*"]}
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=[{"id": "shop", "description": "The shop"}],
    )
    return RoleState(
        contexts={_ROLE: build_context(si)},
        rls_contexts={_ROLE: RLSContext.empty()},
        roles={_ROLE: role},
        masking_rules={},
        tables=tables,
        relationships=relationships,
        schema_build_cache={
            "tables": tables,
            "relationships": relationships,
            "column_types": column_types,
        },
        source_catalogs={},
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        cypher_label_maps={},
    )


@pytest.fixture
def state() -> RoleState:
    return _state()


async def test_the_label_tool_gives_the_pattern_and_the_typed_relationships(state):
    orders = await tools.cypher_field_names(state, _ROLE, "shop", "orders")
    assert orders["node_pattern"] == f"(n:{orders['label']})"
    assert ":" in orders["label"] and "`" not in orders["node_pattern"]
    customers = (await tools.cypher_field_names(state, _ROLE, "shop", "customers"))["label"]
    items = (await tools.cypher_field_names(state, _ROLE, "shop", "order_items"))["label"]
    assert f"(:{orders['label']})-[:PLACED_BY]->(:{customers})" in orders["relationships"]
    assert f"(:{items})-[:ON_ORDER]->(:{orders['label']})" in orders["relationships"]
    assert all("-[:" in r for r in orders["relationships"])  # every one typed


async def _labels(state) -> dict[str, str]:
    return {
        table: (await tools.cypher_field_names(state, _ROLE, "shop", table))["label"]
        for table in ("customers", "orders", "order_items", "products")
    }


async def test_a_network_of_sample_orders_items_and_products_written_as_one_path_runs(state):
    """ "Draw a network diagram of a few sample orders, order items and products": one path,
    typed and directed along the registered relationships."""
    label = await _labels(state)
    query = (
        f"MATCH (o:{label['orders']})<-[on:ON_ORDER]-(i:{label['order_items']})"
        f"-[of:OF_PRODUCT]->(p:{label['products']}) RETURN o, on, i, of, p LIMIT 25"
    )
    read = await check_graph_read(state, _ROLE, query)
    assert read.problem is None
    assert read.columns == {"o": "node", "on": "edge", "i": "node", "of": "edge", "p": "node"}


async def test_a_path_returning_only_its_nodes_is_refused(state):
    """ "Show this analysis in a network graph", then "group by theme": a path whose
    relationships are not returned draws no edges, so the page cannot connect or group it."""
    label = await _labels(state)
    query = (
        f"MATCH (o:{label['orders']})<-[:ON_ORDER]-(i:{label['order_items']})"
        f"-[:OF_PRODUCT]->(p:{label['products']}) RETURN o, i, p LIMIT 25"
    )
    problem = await cypher_read_problem(state, _ROLE, query)
    assert problem is not None
    assert "OF_PRODUCT, ON_ORDER" in problem and "return it" in problem


async def test_a_bound_path_returned_whole_draws_its_relationships(state):
    label = await _labels(state)
    query = f"MATCH p = (o:{label['orders']})-[:PLACED_BY]->(c:{label['customers']}) RETURN p"
    read = await check_graph_read(state, _ROLE, query)
    assert read.problem is None and read.columns == {"p": "path"}


@pytest.mark.parametrize(
    "shape",
    [
        # Comma-separated patterns do not parse here: a path, or one MATCH per path.
        "MATCH (i:{items})-[:ON_ORDER]->(o:{orders}), (i)-[:OF_PRODUCT]->(p:{products}) "
        "RETURN o, i, p",
        # A bare `--` binds no relationship, so the page has none to draw.
        "MATCH (o:{orders})--(c:{customers}) RETURN o, c",
    ],
    ids=["comma_patterns", "bare_dashes"],
)
async def test_the_shapes_that_do_not_run_here_are_refused_with_the_real_error(state, shape):
    label = await _labels(state)
    query = shape.format(
        items=label["order_items"],
        orders=label["orders"],
        products=label["products"],
        customers=label["customers"],
    )
    problem = await cypher_read_problem(state, _ROLE, query)
    assert problem is not None and problem.strip(), query


async def test_a_backticked_label_reads_as_the_same_two_labels(state):
    """Not what failed: the translation reads `Shop:Orders` in backticks as Shop:Orders."""
    label = await _labels(state)
    assert (
        await cypher_read_problem(state, _ROLE, f"MATCH (o:`{label['orders']}`) RETURN o") is None
    )


async def test_a_relationship_type_no_relationship_registers_is_refused(state):
    label = await _labels(state)
    query = f"MATCH (o:{label['orders']})-[:BOUGHT]->(p:{label['products']}) RETURN o, p"
    assert await cypher_read_problem(state, _ROLE, query) is not None


async def test_a_statement_that_does_not_parse_is_refused(state):
    assert await cypher_read_problem(state, _ROLE, "MATCH (o RETURN o") is not None


def test_only_a_graph_navigate_with_a_query_is_checked():
    def call(route, state=None):
        return {"id": "c", "name": "navigate", "input": {"route": route, "state": state}}

    assert graph_query(call("/graph", {"query": "MATCH (n) RETURN n"})) == "MATCH (n) RETURN n"
    assert graph_query(call("/graph", {"view": {"groupBy": "region"}})) is None
    assert graph_query(call("/sql", {"sql": "SELECT 1"})) is None
    assert graph_query({"id": "c", "name": "present_choice", "input": {}}) is None
