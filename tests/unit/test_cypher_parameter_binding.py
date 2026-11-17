# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e19a4-58b2-4f0d-9e61-a2d84b7c5f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher binds a parameterized table's parameter by its property; a write onto one is refused.

The pet-store shape (REQ-1986): get_pet_by_id is read by its path parameter ``petId``, held as
the column ``_nf_petId`` and published by graph-schema as ``petId``. A Cypher property naming the
parameter -- by its name, its column or another spelling of its name -- reads that column, so the
statement's one binding step assigns it by equality or IN and refuses any other condition by
name, as for SQL and GraphQL. A relationship joined on a parameter is not written: a parameter is
bound to a value for the source's call, never a column a row holds.
"""

# Requirements: REQ-1986, REQ-665

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from provisa.compiler.nf_extractor import bind_parameters
from provisa.core.parameter_binding import (
    ParameterBindingRefused,
    ParameterByEqualityOnly,
    ParameterNotWritten,
)
from provisa.cypher.parser import parse_cypher
from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write, written
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
)
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_graph_schema_parameter_id import graph  # noqa: F401  (the compiled pet-store map)

_TYPES = {"_nf_petId": "integer"}


def _sql(graph: CypherLabelMap, cypher: str) -> str:  # noqa: F811
    tree, _, _ = cypher_to_sql(parse_cypher(cypher), graph, {})
    return tree.sql(dialect="postgres")


@pytest.mark.parametrize("name", ["petId", "_nf_petId", "pet_id", "_petId"])
def test_a_parameters_property_reads_its_column(graph, name):  # noqa: F811
    sql = _sql(graph, f"MATCH (b:PetById) WHERE b.{name} = 3 RETURN b.name AS name")
    assert 'b."_nf_petId" = 3' in sql
    assert "NULL" not in sql


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (b:PetById) WHERE b.petId = 3 RETURN b.name AS name",
        "MATCH (b:PetById {petId: 3}) RETURN b.name AS name",
    ],
)
def test_equality_assigns_the_parameter_for_the_call(graph, cypher):  # noqa: F811
    bound = bind_parameters(_sql(graph, cypher), [], _TYPES).bound({})
    assert bound.nf_args == {"petId": 3}
    assert "_nf_petId" not in bound.sql  # the assignment is taken out: rows are not filtered by it


def test_in_assigns_each_value_one_call_each(graph):  # noqa: F811
    sql = _sql(graph, "MATCH (b:PetById) WHERE b.petId IN [1, 2] RETURN b.name AS name")
    bound = bind_parameters(sql, [], _TYPES).bound({})
    assert sorted(args["petId"] for _table, args in bound.reads.values()) == [1, 2]


def test_returning_the_parameter_repeats_the_value_asked(graph):  # noqa: F811
    sql = _sql(graph, "MATCH (b:PetById {petId: 3}) RETURN b.petId AS asked")
    assert 'b."_nf_petId" AS asked' in sql
    bound = bind_parameters(sql, [], _TYPES).bound({})
    assert bound.nf_args == {"petId": 3}
    assert 'b."_nf_petId" AS asked' in bound.sql  # the call's rows carry the value asked


@pytest.mark.parametrize(
    "condition, form",
    [
        ("NOT b.petId = 3", "NOT (=)"),
        ("b.petId <> 3", "<>"),
        ("NOT b.petId IN [1, 2]", "NOT IN"),
        ("b.petId > 3", ">"),
        ("b.petId IS NULL", "IS NULL"),
    ],
)
def test_a_condition_that_assigns_nothing_is_refused_naming_the_parameter(
    graph,  # noqa: F811
    condition,
    form,
):
    sql = _sql(graph, f"MATCH (b:PetById) WHERE {condition} RETURN b.name AS name")
    with pytest.raises(ParameterByEqualityOnly) as refused:
        bind_parameters(sql, [], _TYPES).bound({})
    assert refused.value.code == "data.parameter_by_equality_only"
    assert refused.value.params == {"parameter": "_nf_petId", "form": form}


def test_a_wrongly_typed_value_binds_as_the_sql_front_binds_it(graph):  # noqa: F811
    """The value goes to the binding step as given, which states it as the parameter's type, as a
    SQL statement's ``"_nf_petId" = 'x'`` does: one rule for every front."""
    cypher = _sql(graph, "MATCH (b:PetById) WHERE b.petId = 'x' RETURN b.name AS name")
    sql = cypher.replace('b."_nf_petId"', '"b"."_nf_petId"')
    assert "b.\"_nf_petId\" = 'x'" in cypher
    assert (
        bind_parameters(cypher, [], _TYPES).bound({}).nf_args
        == bind_parameters(sql, [], _TYPES).bound({}).nf_args
        == {"petId": "x"}
    )


# --- Writes -----------------------------------------------------------------------------------


def _node(label: str, table: str, parameters: dict[str, str] | None = None) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=1,
        source_id="petstore",
        id_column="id",
        pk_columns=["id"],
        catalog_name="",
        schema_name="public",
        table_name=table,
        properties={"id": "id", "name": "name"},
        native_filter_columns=parameters or {},
    )


def _write_map() -> CypherLabelMap:
    # config/provisa.yaml's find-pets-by-status-to-get-pet-by-id: many-to-one onto the parameter.
    detail = RelationshipMapping(
        rel_type="DETAIL",
        source_label="PetByStatus",
        target_label="PetById",
        join_source_column="id",
        join_target_column="_nf_petId",
        field_name="detail",
    )
    # A junction whose row would name the parameter as the pet it points at.
    adopted = RelationshipMapping(
        rel_type="ADOPTED",
        source_label="Owner",
        target_label="PetById",
        join_source_column="id",
        join_target_column="_nf_petId",
        field_name="adoptions",
        via=JunctionMapping(
            catalog_name="",
            schema_name="public",
            table_name="adoptions",
            type_name="adoptions",
            source_columns=("owner_id",),
            target_columns=("pet_id",),
            attributes={"since": "since"},
        ),
    )
    return CypherLabelMap(
        nodes={
            "PetByStatus": _node("PetByStatus", "pet_by_status"),
            "PetById": _node("PetById", "pet_by_id", {"_nf_petId": "integer"}),
            "Owner": _node("Owner", "owner"),
        },
        relationships={"DETAIL": detail, "ADOPTED": adopted},
        aliases={"DETAIL": [detail], "ADOPTED": [adopted]},
    )


class _Surface:
    """A write's read and write: neither may be reached by a refused write."""

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def read(self, cypher: str, params: dict):
        self.calls.append(cypher)
        return ["_id"], [["1"]]

    async def write(self, sql: str, bound: list):
        self.calls.append(sql)
        return [], [], 1


_WRITES = [
    "MATCH (a:PetByStatus {id: 1}), (b:PetById {id: 1}) CREATE (a)-[:DETAIL]->(b)",
    "MATCH (o:Owner {id: 1}), (b:PetById {id: 1}) CREATE (o)-[:ADOPTED]->(b)",
    "MATCH (a:PetByStatus {id: 1}), (b:PetById {id: 1}) MERGE (a)-[:DETAIL]->(b)",
    "MATCH (o:Owner {id: 1}), (b:PetById {id: 1}) MERGE (o)-[:ADOPTED]->(b)",
]


@pytest.mark.parametrize("query", _WRITES)
def test_a_relationship_joined_on_a_parameter_is_not_written(query):
    surface = _Surface()
    with pytest.raises(ParameterNotWritten) as refused:
        asyncio.run(
            written(
                parse_cypher_write(query),
                WriteTranslator(_write_map()),
                {},
                surface.read,
                surface.write,
            )
        )
    rel_type = "DETAIL" if "DETAIL" in query else "ADOPTED"
    assert refused.value.code == "data.parameter_not_written"
    assert refused.value.params == {"parameter": "petId", "relationship": rel_type}
    assert surface.calls == []


async def test_http_answers_the_write_refusal_with_its_code(monkeypatch):
    import provisa.api.app as appmod
    import provisa.api.rest.cypher_router as cr

    monkeypatch.setattr(cr, "_resolve_role_id", lambda *_: "admin")
    monkeypatch.setattr(cr, "_build_label_map", lambda *_: _write_map())
    monkeypatch.setattr(appmod.state, "contexts", {"admin": object()})
    request = MagicMock()
    request.state.role = "admin"
    request.headers = {}
    body = cr.CypherRequest(query=_WRITES[0], params={})

    # The route's answer to the statement, past the graph session it is bound in.
    response = await cr._answer(body, request, None, internal=True)

    assert response.status_code == 400, response.body
    answer = json.loads(bytes(response.body))
    assert answer["code"] == "data.parameter_not_written"
    assert answer["params"]["parameter"] == "petId"
    assert answer["params"]["relationship"] == "DETAIL"


async def test_bolt_refuses_the_write_as_a_statement_error(monkeypatch):
    import provisa.bolt.session as bolt_session
    from provisa.bolt.run_failure import STATEMENT_REFUSED, client_failure

    monkeypatch.setattr(bolt_session, "_bolt_label_map", lambda *_: _write_map())
    with pytest.raises(ParameterBindingRefused) as refused:
        await bolt_session._execute_write_cypher(
            _WRITES[1], {}, "admin", SimpleNamespace(), False, SimpleNamespace()
        )
    assert refused.value.code == "data.parameter_not_written"
    assert client_failure(refused.value, SimpleNamespace()) == (
        STATEMENT_REFUSED,
        str(refused.value),
    )
