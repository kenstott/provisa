# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b1d72-6a0e-4c58-b2d4-8e17a5c06f93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameter is bound under its own name and under its alias alike, on every front (REQ-1986).

The pet-store shapes. get_pet_by_id is read by its path parameter ``petId``, held as the column
``_nf_petId`` with the alias Register Table gives it by default (``_nf_pet_id``, its snake-case
spelling). get_owner_by_id's parameter column is ``_nf_owner_id``, aliased ``owner_id``. A
statement that names either parameter by its column, its own name or its alias gives it its
value; SQL, GraphQL, Cypher and SPARQL all resolve the name through the one map the compilation
context holds (``CompilationContext.parameter_names``). A negation through the alias is refused as
through the column. A value the parameter's type does not take is refused, naming the parameter,
before the source is called.
"""

# Requirements: REQ-1986

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.nf_extractor import bind_parameters, name_parameters_by_column
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.core.parameter_binding import (
    ParameterByEqualityOnly,
    ParameterRequired,
    ParameterWrongType,
    check_value,
)
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql

ROLE = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
PET, OWNER = 2, 3


def _columns(*names: str) -> list[dict]:
    return [{"column_name": n, "visible_to": ["admin"]} for n in names]


def _parameter(column: str, alias: str) -> dict:
    return {
        "column_name": column,
        "alias": alias,
        "visible_to": ["admin"],
        "data_type": "integer",
        "native_filter_type": "path_param",
        "native_filter_required": True,
    }


def _table(table_id: int, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "source_id": "petstore",
        "domain_id": "pet-store",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


TABLES = [
    _table(1, "pet_by_status", _columns("id", "name", "status")),
    _table(PET, "get_pet_by_id", [_parameter("_nf_petId", "_nf_pet_id"), *_columns("id", "name")]),
    _table(
        OWNER, "get_owner_by_id", [_parameter("_nf_owner_id", "owner_id"), *_columns("id", "name")]
    ),
]


def _meta(*names: tuple[str, str]) -> list[ColumnMetadata]:
    return [ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t in names]


def _input() -> SchemaInput:
    real = _meta(("id", "integer"), ("name", "varchar"), ("status", "varchar"))
    return SchemaInput(
        tables=TABLES,
        relationships=[],
        column_types={1: real, PET: real, OWNER: real},
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "pet-store", "description": "Pet store"}],
    )


@pytest.fixture(scope="module")
def compiled():
    si = _input()
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


# --- The one map --------------------------------------------------------------------------------


def test_each_parameter_is_held_under_its_column_its_own_name_and_its_alias(compiled):
    _, ctx = compiled
    pet = ctx.parameter_names[PET]
    for name in ("_nf_petid", "petid", "_nf_pet_id", "pet_id"):
        assert pet[name] == "_nf_petId", name
    owner = ctx.parameter_names[OWNER]
    for name in ("_nf_owner_id", "owner_id"):
        assert owner[name] == "_nf_owner_id", name


# --- SQL ----------------------------------------------------------------------------------------


def _sql_bound(ctx, table: str, condition: str, select: str = "name"):
    tree = sqlglot.parse_one(f"SELECT {select} FROM {table} WHERE {condition}", read="postgres")
    name_parameters_by_column(tree, ctx)
    sql = tree.sql(dialect="postgres")
    table_id = PET if table == "get_pet_by_id" else OWNER
    return sql, bind_parameters(sql, [], ctx.native_filter_columns[table_id]).bound({})


@pytest.mark.parametrize(
    "table, condition, args",
    [
        ("get_pet_by_id", '"_nf_petId" = 3', {"petId": 3}),
        ("get_pet_by_id", '"_nf_pet_id" = 3', {"petId": 3}),
        ("get_pet_by_id", "pet_id = 3", {"petId": 3}),
        ("get_pet_by_id", '"petId" = 3', {"petId": 3}),
        ("get_pet_by_id", "pet_id IN (3)", {"petId": 3}),
        ("get_owner_by_id", "owner_id = 3", {"owner_id": 3}),
        ("get_owner_by_id", '"_nf_owner_id" = 3', {"owner_id": 3}),
    ],
)
def test_sql_binds_a_parameter_under_any_of_its_names(compiled, table, condition, args):
    _, ctx = compiled
    _sql, bound = _sql_bound(ctx, table, condition)
    assert bound.nf_args == args


def test_sql_a_parameter_selected_by_its_alias_keeps_that_name(compiled):
    _, ctx = compiled
    sql, bound = _sql_bound(ctx, "get_owner_by_id", "owner_id = 3", select="owner_id")
    assert '"_nf_owner_id" AS owner_id' in sql
    assert bound.nf_args == {"owner_id": 3}


@pytest.mark.parametrize("condition", ["NOT pet_id = 3", "pet_id <> 3", "pet_id NOT IN (3, 4)"])
def test_sql_a_negation_through_the_alias_is_refused_naming_the_parameter(compiled, condition):
    _, ctx = compiled
    with pytest.raises(ParameterByEqualityOnly) as refused:
        _sql_bound(ctx, "get_pet_by_id", condition)
    assert refused.value.params["parameter"] == "_nf_petId"


def test_sql_a_qualified_alias_binds(compiled):
    _, ctx = compiled
    tree = sqlglot.parse_one("SELECT p.name FROM get_pet_by_id AS p WHERE p.pet_id = 3")
    assert name_parameters_by_column(tree, ctx)
    sql = tree.sql(dialect="postgres")
    bound = bind_parameters(sql, [], ctx.native_filter_columns[PET]).bound({})
    assert bound.nf_args == {"petId": 3}


# --- GraphQL ------------------------------------------------------------------------------------


def _field(schema, table: str) -> str:
    assert schema.query_type is not None
    (name,) = [f for f in schema.query_type.fields if f.lower() == table.replace("_", "")]
    return name


def test_graphql_names_the_argument_by_the_parameters_alias(compiled):
    schema, _ = compiled
    assert schema.query_type is not None
    pet = schema.query_type.fields[_field(schema, "get_pet_by_id")].args
    owner = schema.query_type.fields[_field(schema, "get_owner_by_id")].args
    assert str(pet["pet_id"].type) == "String!"
    assert str(owner["owner_id"].type) == "String!"


@pytest.mark.parametrize(
    "table, argument, args",
    [
        ("get_pet_by_id", "pet_id", {"petId": "3"}),
        ("get_owner_by_id", "owner_id", {"owner_id": "3"}),
    ],
)
def test_graphql_binds_a_parameter_by_its_alias(compiled, table, argument, args):
    schema, ctx = compiled
    # The argument is the type its column metadata names; this one has none, so a string.
    query = f'{{ {_field(schema, table)}({argument}: "3") {{ name }} }}'
    (one,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    assert one.api_args == args
    table_id = PET if table == "get_pet_by_id" else OWNER
    bound = bind_parameters(one.sql, [], ctx.native_filter_columns[table_id]).bound({})
    assert bound.nf_args == args


# --- Cypher -------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def graph(compiled) -> CypherLabelMap:
    _, ctx = compiled
    return CypherLabelMap.from_schema(ctx, domain_access=["*"], all_tables=TABLES)


def _label(graph: CypherLabelMap, table: str) -> str:
    (label,) = [nm.label for nm in graph.nodes.values() if nm.table_name.endswith(table)]
    return label.split(":")[-1]


@pytest.mark.parametrize(
    "table, prop, args",
    [
        ("get_pet_by_id", "petId", {"petId": 3}),
        ("get_pet_by_id", "_nf_pet_id", {"petId": 3}),
        ("get_pet_by_id", "pet_id", {"petId": 3}),
        ("get_owner_by_id", "owner_id", {"owner_id": 3}),
        ("get_owner_by_id", "_nf_owner_id", {"owner_id": 3}),
    ],
)
def test_cypher_binds_a_parameter_under_any_of_its_names(compiled, graph, table, prop, args):
    _, ctx = compiled
    cypher = f"MATCH (b:{_label(graph, table)}) WHERE b.{prop} = 3 RETURN b.name AS name"
    tree, _, _ = cypher_to_sql(parse_cypher(cypher), graph, {})
    sql = tree.sql(dialect="postgres")
    table_id = PET if table == "get_pet_by_id" else OWNER
    bound = bind_parameters(sql, [], ctx.native_filter_columns[table_id]).bound({})
    assert bound.nf_args == args


def test_cypher_a_negation_through_the_alias_is_refused(compiled, graph):
    _, ctx = compiled
    cypher = f"MATCH (b:{_label(graph, 'get_pet_by_id')}) WHERE b.pet_id <> 3 RETURN b.name AS n"
    tree, _, _ = cypher_to_sql(parse_cypher(cypher), graph, {})
    with pytest.raises(ParameterByEqualityOnly):
        bind_parameters(tree.sql(dialect="postgres"), [], ctx.native_filter_columns[PET]).bound({})


# --- SPARQL -------------------------------------------------------------------------------------


@pytest.fixture
def vocab(graph, monkeypatch):
    from provisa.sparql_query.vocabulary import Grounding, build_vocabulary

    base = "https://data.example.com"
    monkeypatch.setenv("PROVISA_PUBLIC_URL", base)
    return build_vocabulary(graph, base, {}, Grounding({}, {}))


@pytest.mark.parametrize(
    "table, name, args",
    [
        ("get_pet_by_id", "petId", {"petId": 3}),
        ("get_pet_by_id", "pet_id", {"petId": 3}),
        ("get_owner_by_id", "owner_id", {"owner_id": 3}),
    ],
)
def test_sparql_binds_a_parameter_by_its_own_name_and_its_alias(
    compiled, graph, vocab, table, name, args
):
    from provisa.sparql_query.compiler import compile_sparql
    from provisa.sparql_query.vocabulary import column_iri

    _, ctx = compiled
    nm = next(n for n in graph.nodes.values() if n.table_name.endswith(table))
    parameter = column_iri(vocab.base, "pet-store", nm.model_name, name)
    title = column_iri(vocab.base, "pet-store", nm.model_name, "name")
    text = f"SELECT ?n WHERE {{ ?p <{parameter}> 3 . ?p <{title}> ?n }}"
    sparql = compile_sparql(text, vocab, graph)
    assert sparql.ast is not None
    tree, _, _ = cypher_to_sql(sparql.ast, graph, {})
    table_id = PET if table == "get_pet_by_id" else OWNER
    bound = bind_parameters(tree.sql(dialect="postgres"), [], ctx.native_filter_columns[table_id])
    assert bound.bound({}).nf_args == args


# --- Wrong type and missing parameter -----------------------------------------------------------


def test_a_value_its_type_does_not_take_is_refused_before_the_call(compiled):
    _, ctx = compiled
    with pytest.raises(ParameterWrongType) as refused:
        _sql_bound(ctx, "get_pet_by_id", "\"_nf_petId\" = 'two'")
    assert refused.value.code == "data.parameter_wrong_type"
    assert refused.value.params == {"parameter": "_nf_petId", "type": "integer", "value": "two"}


@pytest.mark.parametrize("condition", ["pet_id = 'two'", "pet_id IN (3, 'two')"])
def test_a_wrongly_typed_value_through_the_alias_is_refused(compiled, condition):
    _, ctx = compiled
    with pytest.raises(ParameterWrongType):
        _sql_bound(ctx, "get_pet_by_id", condition)


def test_graphql_a_wrongly_typed_argument_is_refused_before_the_call(compiled):
    schema, ctx = compiled
    query = f'{{ {_field(schema, "get_pet_by_id")}(pet_id: "two") {{ name }} }}'
    (one,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    with pytest.raises(ParameterWrongType):
        bind_parameters(one.sql, [], ctx.native_filter_columns[PET]).bound({})


@pytest.mark.parametrize(
    "data_type, value",
    [
        ("integer", 3),
        ("integer", "3"),
        ("bigint", " -7 "),
        ("numeric(10,2)", "2.5"),
        ("boolean", True),
        ("boolean", "false"),
        ("text", "anything"),
        ("integer", None),
    ],
)
def test_a_value_its_type_takes_is_given(data_type, value):
    check_value("_nf_x", data_type, value)


@pytest.mark.parametrize(
    "data_type, value",
    [("integer", "two"), ("integer", 2.5), ("integer", True), ("double", "x"), ("boolean", "yes")],
)
def test_a_value_its_type_does_not_take_is_refused(data_type, value):
    with pytest.raises(ParameterWrongType):
        check_value("_nf_x", data_type, value)


def test_a_missing_parameter_is_refused_under_its_catalogued_code():
    refused = ParameterRequired("get_pet_by_id", ["petId"])
    assert refused.code == "data.parameter_required"
    assert refused.params == {"table": "get_pet_by_id", "parameters": "petId"}
    assert "requires the parameter(s) ['petId']" in str(refused)
