# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3ddf07-2a42-471a-ac21-0bce4734b11d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-778: a mistake in the user's Cypher is refused at translation, in the user's own terms --
the clause or expression, what is wrong, and the fix -- under its own code, and no engine is
asked. The engine's own text is said only for what the engine refused, as the engine's."""

# Requirements: REQ-778

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)
from tests.unit.test_cypher_cache_before_route import graph  # noqa: F401


@pytest.fixture
def refused(graph, monkeypatch):  # noqa: F811
    """Sends a statement to /data/cypher and answers its body, failing if anything reached the
    pipeline or an engine."""
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query
    from provisa.pgwire import _pipeline

    async def _never(*_a, **_k):
        raise AssertionError("the statement reached the pipeline")

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _never)

    async def send(query: str) -> dict:
        request = MagicMock()
        request.state.role = "analyst"
        request.headers = {}
        response = await cypher_query(
            CypherRequest(query=query, params={}), request, query_id=None, x_provisa_stats=None
        )
        assert response.status_code == 400, response.body
        assert graph.state.federation_engine.statements == []
        return json.loads(bytes(response.body))

    return send


async def test_a_literal_pattern_no_engine_takes_is_refused_in_the_users_terms(refused):
    body = await refused('MATCH (o:Orders) WHERE o.id =~ "*pet*" RETURN o.id AS id')
    assert body["code"] == "cypher.invalid_regex"
    assert body["detail"] == (
        'The pattern "*pet*" in o.id =~ "*pet*" is not a valid regular expression: "*" must '
        'follow something to repeat. =~ takes a regular expression, e.g. ".*pet.*", or use '
        'CONTAINS "pet".'
    )


async def test_an_unknown_label_is_refused_with_the_labels_like_it(refused):
    body = await refused("MATCH (o:Ordrs) RETURN o.id AS id")
    assert body["code"] == "cypher.unknown_label"
    assert (
        body["detail"]
        == "There is no label :Ordrs. Registered labels: :Orders, :Sales:Orders, :Sales."
    )


async def test_an_unknown_relationship_type_is_a_400_naming_it(refused):
    body = await refused("MATCH (o:Orders)-[:BOUGHT]->(x:Orders) RETURN o.id AS id")
    assert body["code"] == "cypher.no_relationship_types"
    assert body["detail"] == (
        "There is no relationship type :BOUGHT: this role can traverse no relationship."
    )


async def test_a_property_compared_with_a_literal_of_another_type_is_refused(refused):
    body = await refused('MATCH (o:Orders) WHERE o.id = "ten" RETURN o.id AS id')
    assert body["code"] == "cypher.type_mismatch"
    assert body["detail"] == (
        'In o.id = "ten", "ten" cannot be compared with o.id, which is of type integer. '
        "Compare o.id to a number, e.g. o.id = 10."
    )


async def test_a_number_written_as_text_still_compares():
    from provisa.gql.expr_ast import Literal, Property, Variable
    from provisa.gql.mistakes import type_mismatch

    prop = Property(obj=Variable(name="o"), name="id")
    assert type_mismatch("=", prop, "integer", Literal(value="10", kind="string"), True) is None
    assert type_mismatch("<", prop, "timestamp", Literal(value="x", kind="string"), True) is None


async def test_an_aggregate_in_where_is_refused(refused):
    body = await refused("MATCH (o:Orders) WHERE count(o) > 1 RETURN o.id AS id")
    assert body["code"] == "cypher.aggregate_in_where"
    assert body["detail"] == (
        "WHERE cannot use the aggregate count(o): WHERE filters rows before they are grouped. "
        "Aggregate in a WITH and filter after it."
    )


async def test_an_aggregate_of_an_aggregate_is_refused(refused):
    body = await refused("MATCH (o:Orders) RETURN count(sum(o.id)) AS n")
    assert body["code"] == "cypher.nested_aggregate"
    assert body["detail"] == (
        "count(sum(o.id)) aggregates the aggregate sum(o.id): an aggregate cannot be taken of "
        "another. Aggregate in a WITH first."
    )


async def test_a_variable_nothing_binds_is_refused_naming_its_clause(refused):
    body = await refused("MATCH (o:Orders) RETURN x.id AS id")
    assert body["code"] == "cypher.undefined_variable"
    assert body["detail"] == (
        "RETURN reads x, which nothing in the statement binds: a variable is bound by a MATCH "
        "pattern, WITH, UNWIND or CALL. Bound here: o."
    )


@pytest.mark.parametrize(
    "tail, code, detail",
    [
        (
            "LIMIT 2.5",
            "cypher.invalid_limit",
            "LIMIT 2.5 is not a whole number of rows: LIMIT takes a count of 0 or more, "
            "e.g. LIMIT 10.",
        ),
        (
            "SKIP -1",
            "cypher.invalid_skip",
            "SKIP -1 is not a whole number of rows: SKIP takes a count of 0 or more, e.g. SKIP 10.",
        ),
    ],
)
async def test_a_row_count_that_is_no_whole_number_is_refused(refused, tail, code, detail):
    body = await refused(f"MATCH (o:Orders) RETURN o.id AS id {tail}")
    assert (body["code"], body["detail"]) == (code, detail)


async def test_what_the_engine_refuses_is_said_as_the_engines(monkeypatch):
    from types import SimpleNamespace

    from provisa.api.rest import cypher_router
    from provisa.pgwire import _pipeline

    async def _refuse(plan, state):
        raise RuntimeError("Binder Error: no such function")

    monkeypatch.setattr(_pipeline, "_execute_plan", _refuse)
    monkeypatch.setattr(_pipeline, "require_governed_plan", lambda plan: None)
    state = SimpleNamespace(federation_engine=SimpleNamespace(classify_error=lambda exc: "query"))
    plan = SimpleNamespace(physical_sql="SELECT 1", exec_sql=None)
    response = await cypher_router._run_plan(plan, state)
    body = json.loads(bytes(response.body))
    assert response.status_code == 400
    assert body["code"] == "cypher.engine_refused"
    assert body["detail"] == "The engine refused the query: Binder Error: no such function"
    assert body["sql"] == "SELECT 1"


def _pets_label_map():
    from provisa.gql.label_map import CypherLabelMap, NodeMapping

    def node(label: str, table_id: int, props: list[str]) -> NodeMapping:
        return NodeMapping(
            label=label,
            type_name=label,
            domain_label=None,
            table_label=label,
            table_id=table_id,
            source_id="pets",
            id_column="id",
            pk_columns=["id"],
            catalog_name="pets",
            schema_name="pets",
            table_name=label.lower(),
            properties={p: p for p in props},
        )

    nodes = {
        "Pets": node("Pets", 1, ["id", "name", "breedName"]),
        "Owners": node("Owners", 2, ["id", "name"]),
    }
    return CypherLabelMap(
        nodes=nodes,
        relationships={},
        domains={},
        nodes_by_table={"Pets": ["Pets"], "Owners": ["Owners"]},
    )


def test_an_unbound_name_that_is_a_bound_variables_property_is_offered_as_it():
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator_types import CypherMistake

    with pytest.raises(CypherMistake) as refused:
        parse_cypher("MATCH (a:Pets) RETURN a.breedName, COUNT(id) AS pet_count", _pets_label_map())
    assert refused.value.code == "cypher.undefined_variable_property"
    assert str(refused.value) == (
        "RETURN reads id, which nothing in the statement binds: a variable is bound by a MATCH "
        "pattern, WITH, UNWIND or CALL. Bound here: a. Did you mean a.id? a is bound to :Pets, "
        "which has a property id."
    )


def test_each_bound_variable_with_the_property_is_offered():
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator_types import CypherMistake

    with pytest.raises(CypherMistake) as refused:
        parse_cypher("MATCH (a:Pets) MATCH (o:Owners) RETURN name", _pets_label_map())
    assert refused.value.code == "cypher.undefined_variable_properties"
    assert str(refused.value).endswith(
        "Did you mean a.name or o.name? Each is bound to a label with a property name."
    )
