# Copyright (c) 2026 Kenneth Stott
# Canary: 5939eb88-80c9-40f3-a801-abfe9fd9d0a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationships govern how tables are related, however it is written (REQ-603): rows of two
registered tables are matched only by a registered relationship's column equality -- a join of
any spelling, through a CTE or a derived table, IN / NOT IN, EXISTS / NOT EXISTS, a correlated
subquery, LATERAL. Anything else relating them is refused."""

# Requirements: REQ-603, REQ-693

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext

_REPO = Path(__file__).resolve().parents[2]
_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}
_COLUMNS = {
    1: ["id", "region", "customer_id"],  # orders
    2: ["id", "name", "region"],  # customers
    3: ["id", "customer_id"],  # visits
    4: ["id", "table_name"],  # a meta table
}


def _meta(table_id: int, name: str, domain: str = "sales") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id=domain,
        source_type="postgresql",
    )


def _model() -> tuple[CompilationContext, GovernanceContext]:
    """orders, customers and visits; ONE relationship: customers.id = visits.customer_id."""
    orders, customers, visits = _meta(1, "orders"), _meta(2, "customers"), _meta(3, "visits")
    ctx = CompilationContext()
    ctx.tables = {"orders": orders, "customers": customers, "visits": visits}
    ctx.joins = {
        ("Customers", "visits"): JoinMeta(
            source_column="id",
            target_column="customer_id",
            source_column_type="integer",
            target_column_type="integer",
            target=visits,
            cardinality="one-to-many",
        )
    }
    gov = GovernanceContext()
    gov.table_map = {"orders": 1, "customers": 2, "visits": 3}
    gov.all_columns = {tid: [(c, "integer") for c in cols] for tid, cols in _COLUMNS.items()}
    return ctx, gov


def _v002(sql: str, **kw) -> list[str]:
    ctx, gov = _model()
    return [v.message for v in validate_sql(sql, ctx, gov, _ROLE, [], **kw) if v.code == "V002"]


_OUTSIDE = {
    "explicit_on": "SELECT o.id FROM orders o JOIN customers c ON o.region = c.region",
    "left_join": "SELECT o.id FROM orders o LEFT JOIN customers c ON o.region = c.region",
    "comma_where": "SELECT o.id FROM orders o, customers c WHERE o.region = c.region",
    "cross_join_where": "SELECT o.id FROM orders o CROSS JOIN customers c WHERE o.region = c.region",
    "on_true_where": "SELECT o.id FROM orders o JOIN customers c ON TRUE WHERE o.region = c.region",
    "on_range": (
        "SELECT o.id FROM orders o JOIN customers c ON o.region >= c.region AND o.region <= c.region"
    ),
    "on_expression": "SELECT o.id FROM orders o JOIN customers c ON LOWER(o.region) = c.region",
    "cte": (
        "WITH w AS (SELECT * FROM orders) "
        "SELECT w.id FROM w JOIN customers c ON w.region = c.region"
    ),
    "cte_renamed": (
        "WITH w AS (SELECT id, region AS r FROM orders) "
        "SELECT w.id FROM w JOIN customers c ON w.r = c.region"
    ),
    "two_ctes": (
        "WITH a AS (SELECT * FROM orders), b AS (SELECT * FROM customers) "
        "SELECT a.id FROM a JOIN b ON a.region = b.region"
    ),
    "derived_table": (
        "SELECT w.id FROM (SELECT * FROM orders) w JOIN customers c ON w.region = c.region"
    ),
    "in_subquery": "SELECT o.id FROM orders o WHERE o.region IN (SELECT region FROM customers)",
    "exists": (
        "SELECT o.id FROM orders o WHERE EXISTS "
        "(SELECT 1 FROM customers c WHERE c.region = o.region)"
    ),
    "scalar_subquery": (
        "SELECT o.id, (SELECT MAX(c.name) FROM customers c WHERE c.region = o.region) FROM orders o"
    ),
    "lateral": (
        "SELECT o.id, x.name FROM orders o, "
        "LATERAL (SELECT c.name FROM customers c WHERE c.region = o.region) x"
    ),
    "using": "SELECT o.id FROM orders o JOIN customers c USING (region)",
    "natural": "SELECT o.id FROM orders o NATURAL JOIN customers c",
    "no_condition": "SELECT o.id FROM orders o JOIN customers c ON TRUE",
    "not_in_subquery": (
        "SELECT o.id FROM orders o WHERE o.region NOT IN (SELECT region FROM customers)"
    ),
    "not_exists": (
        "SELECT o.id FROM orders o WHERE NOT EXISTS "
        "(SELECT 1 FROM customers c WHERE c.region = o.region)"
    ),
    "in_subquery_of_an_expression": (
        "SELECT o.id FROM orders o WHERE o.region IN (SELECT LOWER(region) FROM customers)"
    ),
    "equality_on_a_computed_cte_column": (
        "WITH w AS (SELECT id, LOWER(name) AS key FROM customers) "
        "SELECT w.id FROM w JOIN visits v ON w.key = v.customer_id"
    ),
    # Along the registered relationship, and ALSO matched by something else.
    "extra_inequality": (
        "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id AND v.id > c.id"
    ),
    "extra_equality_in_where": (
        "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id WHERE c.id = v.id"
    ),
    "or_of_two_equalities": (
        "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id OR c.id = v.id"
    ),
    # Three tables, one hop of the two not registered.
    "second_hop_unregistered": (
        "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id "
        "JOIN orders o ON o.customer_id = c.id"
    ),
    # The registered relationship's tables, on other columns than its own.
    "wrong_columns_in_where": "SELECT c.id FROM customers c JOIN visits v ON TRUE WHERE c.id = v.id",
    "self_join": "SELECT a.id FROM orders a JOIN orders b ON TRUE WHERE a.region = b.region",
}


@pytest.mark.parametrize("shape", list(_OUTSIDE))
def test_two_tables_combined_outside_a_relationship_are_refused(shape):
    said = _v002(_OUTSIDE[shape])
    assert said, shape


@pytest.mark.parametrize("shape", list(_OUTSIDE))
def test_a_role_that_may_ignore_relationships_is_not_held_to_them(shape):
    assert _v002(_OUTSIDE[shape], bypass_relationship_guard=True) == []


def test_the_refusal_says_how_the_tables_are_related_outside_a_relationship():
    unregistered = (
        "Invalid JOIN: orders.region = customers.region — no approved relationship exists between "
        "these tables on these columns"
    )
    for shape in (
        "explicit_on",
        "comma_where",
        "on_true_where",
        "cte",
        "cte_renamed",
        "derived_table",
        "in_subquery",
        "not_in_subquery",
        "exists",
        "not_exists",
        "scalar_subquery",
        "lateral",
        "using",
    ):
        assert _v002(_OUTSIDE[shape]) == [unregistered], shape
    # NATURAL pairs every column the two tables share: id and region, neither a relationship.
    assert sorted(_v002(_OUTSIDE["natural"])) == sorted(
        [unregistered, unregistered.replace("region", "id")]
    )
    other = (
        "Invalid JOIN: orders and customers are matched by something other than an equality of "
        "their columns — tables are related only along an approved relationship, on its columns"
    )
    for shape in ("on_range", "on_expression", "in_subquery_of_an_expression"):
        assert _v002(_OUTSIDE[shape]) == [other], shape
    assert _v002(_OUTSIDE["no_condition"]) == [
        "Invalid JOIN: orders and customers are combined with no condition relating them — cross "
        "joins are not permitted"
    ]
    (said,) = _v002(_OUTSIDE["extra_equality_in_where"])
    assert "customers.id = visits.id" in said


_ALONG = {
    "explicit_on": "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id",
    "reversed": "SELECT c.name FROM visits v JOIN customers c ON v.customer_id = c.id",
    # Any spelling of the join, judged by the columns it pairs.
    "comma_where": "SELECT c.name FROM customers c, visits v WHERE c.id = v.customer_id",
    "cross_join_where": (
        "SELECT c.name FROM customers c CROSS JOIN visits v WHERE v.customer_id = c.id"
    ),
    "on_true_where": (
        "SELECT c.name FROM customers c JOIN visits v ON TRUE WHERE c.id = v.customer_id"
    ),
    "left_join": "SELECT c.name FROM customers c LEFT JOIN visits v ON c.id = v.customer_id",
    "single_table_conditions": (
        "SELECT c.name FROM customers c JOIN visits v ON c.id = v.customer_id AND v.id > 5 "
        "WHERE c.region = 'east' OR c.name = 'ann'"
    ),
    "not_in_subquery": (
        "SELECT c.name FROM customers c WHERE c.id NOT IN (SELECT customer_id FROM visits)"
    ),
    "not_exists": (
        "SELECT c.name FROM customers c WHERE NOT EXISTS "
        "(SELECT 1 FROM visits v WHERE v.customer_id = c.id)"
    ),
    "lateral": (
        "SELECT c.name, x.id FROM customers c, "
        "LATERAL (SELECT v.id FROM visits v WHERE v.customer_id = c.id) x"
    ),
    "cte": (
        "WITH w AS (SELECT * FROM customers) "
        "SELECT w.name FROM w JOIN visits v ON w.id = v.customer_id"
    ),
    "cte_renamed": (
        "WITH w AS (SELECT id AS cid, name FROM customers) "
        "SELECT w.name FROM w JOIN visits v ON w.cid = v.customer_id"
    ),
    "aggregated_derived_table": (
        "SELECT c.name, n.visits FROM customers c JOIN "
        "(SELECT customer_id, COUNT(*) AS visits FROM visits GROUP BY customer_id) n "
        "ON c.id = n.customer_id"
    ),
    "in_subquery": "SELECT c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM visits)",
    "exists": (
        "SELECT c.name FROM customers c WHERE EXISTS "
        "(SELECT 1 FROM visits v WHERE v.customer_id = c.id)"
    ),
    "scalar_subquery": (
        "SELECT c.name, (SELECT COUNT(*) FROM visits v WHERE v.customer_id = c.id) FROM customers c"
    ),
}


@pytest.mark.parametrize("shape", list(_ALONG))
def test_tables_combined_along_a_relationship_pass(shape):
    assert _v002(_ALONG[shape]) == [], shape


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM orders",
        # The branches of a union are alternatives, never combined with each other.
        "SELECT id FROM orders UNION ALL SELECT id FROM customers",
        "SELECT u.id FROM (SELECT id FROM orders UNION ALL SELECT id FROM customers) u",
        # A subquery that reaches out to nothing around it relates no rows to it.
        "SELECT o.id FROM orders o WHERE o.id > (SELECT MAX(id) FROM customers)",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM customers)",
        "WITH w AS (SELECT id FROM customers) SELECT id FROM orders",
    ],
)
def test_tables_a_statement_does_not_combine_need_no_relationship(sql):
    assert _v002(sql) == [], sql


def test_a_union_read_beside_a_table_is_held_branch_by_branch():
    sql = (
        "SELECT v.id FROM visits v JOIN "
        "(SELECT id FROM customers UNION ALL SELECT id FROM orders) u ON u.id = v.customer_id"
    )
    (said,) = _v002(sql)  # customers.id = visits.customer_id is registered; orders.id is not
    assert "orders.id" in said and "visits.customer_id" in said, said


def test_a_meta_table_is_read_beside_any_table():
    ctx, gov = _model()
    ctx.tables["registered_tables_meta"] = _meta(4, "registered_tables_meta", domain="meta")
    gov.table_map["registered_tables_meta"] = 4
    sql = "SELECT o.id FROM orders o JOIN registered_tables_meta m ON TRUE WHERE m.id = o.id"
    assert [v for v in validate_sql(sql, ctx, gov, _ROLE, []) if v.code == "V002"] == []


def test_one_rule_decides_who_may_ignore_relationships_for_every_statement():
    """REQ-264, REQ-693: the right, or a cleared guard with the statement's opt-out, lets a role
    relate tables freely -- decided by one function for raw and compiled statements alike -- and
    never in high-security mode, whatever the role was granted."""
    from types import SimpleNamespace

    from provisa.pgwire._pipeline import relationship_guard_bypassed

    normal, high = SimpleNamespace(security_high=False), SimpleNamespace(security_high=True)
    holder = {"id": "free", "capabilities": ["ignore_relationships"], "domain_access": ["*"]}
    cleared = {"id": "loose", "capabilities": [], "relationship_guard": False}
    bound = {"id": "bound", "capabilities": []}

    assert relationship_guard_bypassed(holder, normal, statement_opts_out=False)
    assert relationship_guard_bypassed(cleared, normal, statement_opts_out=True)
    assert not relationship_guard_bypassed(cleared, normal, statement_opts_out=False)
    assert not relationship_guard_bypassed(bound, normal, statement_opts_out=True)
    for role in (holder, cleared, bound):
        assert not relationship_guard_bypassed(role, high, statement_opts_out=True)


def test_only_a_graphql_built_statement_is_exempt_and_its_caller_says_so():
    """REQ-603: the compiled stage checks what a statement relates unless the GraphQL SDL defined
    its joins; every caller states which (``sdl_joins`` has no default), and no Cypher surface
    states the exemption or switches the guard off."""
    import inspect
    import re

    from provisa.pgwire import _pipeline

    for entry in (
        _pipeline._govern_and_route_compiled,
        _pipeline._govern_and_route_compiled_planned,
        _pipeline._govern_compiled,
    ):
        assert inspect.signature(entry).parameters["sdl_joins"].default is inspect.Parameter.empty
    # The compiled stage validates the statement as the raw stage does, the relationship guard
    # skipped only for the SDL's joins or by the one bypass rule.
    governing = inspect.getsource(_pipeline._govern_compiled)
    assert "validate_sql(" in governing and "relationship_guard_bypassed(" in governing
    assert "bypass_relationship_guard=sdl_joins" in governing
    package = _REPO / "provisa"
    for surface in (
        "bolt/session.py",
        "api/rest/cypher_router.py",
        "api/rest/cypher_exec.py",
        "api/rest/neo4j_compat_router.py",
    ):
        text = (package / surface).read_text()
        assert "sdl_joins=True" not in text and "bypass_relationship_guard=True" not in text
        assert "sdl_joins=False" in text, surface
    # Flight carries both: its Cypher ticket is checked, its GraphQL ticket is the SDL's.
    flight = (package / "api/flight/server.py").read_text()
    graphql = flight[flight.index("def _do_get_graphql(") :]
    assert "sdl_joins=True" in graphql and "sdl_joins=False" in flight[: flight.index(graphql)]
    # No blanket skip is left anywhere: the GraphQL endpoint states sdl_joins like every caller.
    skipping = [
        str(path.relative_to(package))
        for path in package.rglob("*.py")
        if re.search(r"bypass_relationship_guard=True", path.read_text())
    ]
    assert skipping == [], skipping


def test_usings_and_naturals_are_judged_by_the_columns_they_pair():
    """USING and NATURAL pair the columns they name: along a registered relationship on a
    column of one name they pass, on any other column they are refused."""
    ctx, gov = _model()
    # A relationship on a column both tables name the same: customers.region = orders.region.
    orders, customers = ctx.tables["orders"], ctx.tables["customers"]
    ctx.joins[("Customers", "orders")] = JoinMeta(
        source_column="region",
        target_column="region",
        source_column_type="varchar",
        target_column_type="varchar",
        target=orders,
        cardinality="one-to-many",
    )
    del customers

    def v002(sql: str) -> list[str]:
        return [v.message for v in validate_sql(sql, ctx, gov, _ROLE, []) if v.code == "V002"]

    assert v002("SELECT o.id FROM orders o JOIN customers c USING (region)") == []
    (said,) = v002("SELECT o.id FROM orders o JOIN customers c USING (id)")
    assert "orders.id = customers.id" in said
    # NATURAL pairs every column the two share: id is not a relationship of theirs.
    said = v002("SELECT o.id FROM orders o NATURAL JOIN customers c")
    assert any("orders.id = customers.id" in m for m in said), said


# -- a registered relationship whose edge is computed -------------------------------------------


def _computed_model():
    """orders, customers and regions. Two registered relationships with computed edges:
    LOWER(orders.customer_code) = customers.code, and every customer -> the region whose id is
    the constant 1."""
    from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping

    columns = {
        1: ["id", "customer_code"],
        2: ["id", "code", "name"],
        3: ["id", "name"],
    }
    orders, customers, regions = _meta(1, "orders"), _meta(2, "customers"), _meta(3, "regions")
    ctx = CompilationContext()
    ctx.tables = {"orders": orders, "customers": customers, "regions": regions}
    ctx.joins = {
        ("Orders", "customer"): JoinMeta(
            source_column="customer_code",
            target_column="code",
            source_column_type="varchar",
            target_column_type="varchar",
            target=customers,
            cardinality="many-to-one",
            source_expr='LOWER({alias}."customer_code")',
        ),
        ("Customers", "home"): JoinMeta(
            source_column="id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=regions,
            cardinality="many-to-one",
            source_constant=1,
        ),
    }
    gov = GovernanceContext()
    gov.table_map = {
        "orders": 1,
        "customers": 2,
        "regions": 3,
        "public.orders": 1,
        "public.customers": 2,
        "public.regions": 3,
    }
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in columns.items()}

    def node(label: str, table_id: int, table: str) -> NodeMapping:
        return NodeMapping(
            label=label,
            type_name=label,
            domain_label=None,
            table_label=label,
            table_id=table_id,
            source_id="pg",
            id_column="id",
            pk_columns=[],
            catalog_name="pg",
            schema_name="public",
            table_name=table,
            properties={c: c for c in columns[table_id]},
        )

    placed_by = RelationshipMapping(
        rel_type="PLACED_BY",
        source_label="Orders",
        target_label="Customers",
        join_source_column="customer_code",
        join_target_column="code",
        field_name="customer",
        source_expr='LOWER({alias}."customer_code")',
    )
    home = RelationshipMapping(
        rel_type="HOME",
        source_label="Customers",
        target_label="Regions",
        join_source_column="id",
        join_target_column="id",
        field_name="home",
        source_constant=1,
    )
    label_map = CypherLabelMap(
        nodes={
            "Orders": node("Orders", 1, "orders"),
            "Customers": node("Customers", 2, "customers"),
            "Regions": node("Regions", 3, "regions"),
        },
        relationships={"PLACED_BY": placed_by, "HOME": home},
        aliases={"PLACED_BY": [placed_by], "HOME": [home]},
    )
    return ctx, gov, label_map


def _guard(sql_tree, ctx, gov) -> list[str]:
    from provisa.compiler.sql_validator import approved_joins, tables_outside_relationships

    from provisa.compiler.sql_validator import computed_joins

    computed, constants = computed_joins(ctx)
    return [
        v.message
        for v in tables_outside_relationships(
            sql_tree,
            gov,
            approved_joins(ctx),
            {m.table_id: m for m in ctx.tables.values()},
            computed=computed,
            constants=constants,
        )
    ]


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (o:Orders)-[:PLACED_BY]->(c:Customers) RETURN o.id, c.name",
        "MATCH (c:Customers)-[:HOME]->(r:Regions) RETURN c.name, r.name",
    ],
)
def test_a_pattern_over_a_computed_registered_relationship_passes(cypher):
    """A relationship registered with a computed edge -- an expression, a constant -- is a
    registered relationship: the join a pattern over it lowers to IS its own condition."""
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql

    ctx, gov, label_map = _computed_model()
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), label_map, {})
    assert _guard(sql_tree, ctx, gov) == [], sql_tree.sql(dialect="postgres")


def test_sql_written_as_the_computed_relationship_is_registered_passes_and_any_other_does_not():
    import sqlglot

    ctx, gov, _ = _computed_model()

    def said(sql: str) -> list[str]:
        return _guard(sqlglot.parse_one(sql, read="postgres"), ctx, gov)

    assert (
        said('SELECT o.id FROM orders o JOIN customers c ON LOWER(o."customer_code") = c."code"')
        == []
    )
    assert (
        said("SELECT o.id FROM orders o, customers c WHERE c.code = LOWER(o.customer_code)") == []
    )
    assert said("SELECT c.name FROM customers c JOIN regions r ON r.id = 1") == []
    # Another expression, another constant, the plain column: none is the relationship.
    assert said("SELECT o.id FROM orders o JOIN customers c ON UPPER(o.customer_code) = c.code")
    assert said("SELECT o.id FROM orders o JOIN customers c ON o.customer_code = c.code")
    assert said("SELECT c.name FROM customers c JOIN regions r ON r.id = 2")
    assert said("SELECT c.name FROM customers c JOIN regions r ON TRUE")


def test_a_pattern_whose_type_does_not_connect_its_labels_is_refused_saying_so():
    """A registered relationship type named between two labels it does not connect lowers to a
    join that is never true; the refusal says no relationship of that type connects them."""
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql

    ctx, gov, label_map = _computed_model()
    cypher = "MATCH (o:Orders)-[:HOME]->(r:Regions) RETURN o.id, r.name"
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), label_map, {})
    assert _guard(sql_tree, ctx, gov) == [
        "Invalid JOIN: no approved relationship of the type named connects orders and regions"
    ], sql_tree.sql(dialect="postgres")


def test_a_lowering_that_is_not_the_relationships_own_condition_is_refused():
    """The backward traversal of a relationship registered with an expression on its source side
    is lowered on the two plain columns (#148), which is not the registered condition: the guard
    holds the statement to the relationship as registered, whoever wrote the SQL."""
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql

    ctx, gov, label_map = _computed_model()
    cypher = "MATCH (c:Customers)<-[:PLACED_BY]-(o:Orders) RETURN o.id, c.name"
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), label_map, {})
    lowered = sql_tree.sql(dialect="postgres")
    if "LOWER(" in lowered:
        assert _guard(sql_tree, ctx, gov) == [], lowered  # the translator defect is fixed
    else:
        (said,) = _guard(sql_tree, ctx, gov)
        assert "orders.customer_code = customers.code" in said, said
