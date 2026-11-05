# Copyright (c) 2026 Kenneth Stott
# Canary: b5377dfc-778c-4ff8-b40b-8c87d8da25a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameterized table's aggregate and group-by take its parameters (REQ-1986, REQ-1991).

The ``_aggregate`` and ``_group_by`` fields of a table with parameters take the same arguments as
its base field, and compile them the same way: ``_nf_<name> = <value>``, the predicate the read
takes back out and sends to the source. A parameter whose name is also one of the field's own is
offered as ``_<name>`` and still fills ``_nf_<name>`` (#247).
"""

# Requirements: REQ-1986, REQ-1991

from __future__ import annotations

import pytest

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query


def _schema():
    def _param(name: str, *, required: bool) -> dict:
        return {
            "column_name": f"_nf_{name}",
            "visible_to": [],
            "native_filter_type": "query_param",
            "native_filter_required": required,
        }

    data = [{"column_name": c, "visible_to": ["admin"]} for c in ("id", "status", "total")]
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "api",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    *data,
                    _param("region", required=True),
                    # The same name as a data column: offered as _status.
                    _param("status", required=False),
                ],
                "enable_aggregates": True,
                "enable_group_by": True,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="status", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="total", data_type="double", is_nullable=True),
                ColumnMetadata(column_name="_nf_region", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="_nf_status", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


def _compiled(query: str):
    schema, ctx = _schema()
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    return compiled


@pytest.mark.parametrize("field", ["orders", "ordersAggregate", "ordersGroupBy"])
def test_every_read_of_the_table_takes_its_parameters(field):
    schema, _ = _schema()
    assert schema.query_type is not None
    args = {k: str(v.type) for k, v in schema.query_type.fields[field].args.items()}
    assert args["region"] == "String!"
    assert args["_status"] == "String"


def test_an_aggregate_sends_its_parameters():
    compiled = _compiled('{ ordersAggregate(region: "EU") { aggregate { count } } }')
    assert compiled.sql.endswith("WHERE \"_nf_region\" = 'EU'")
    assert compiled.api_args == {"region": "EU"}


def test_an_aggregate_sends_its_parameters_beside_its_filter():
    compiled = _compiled(
        '{ ordersAggregate(region: "EU", where: {total: {gt: 5}}) { aggregate { count } } }'
    )
    assert 'WHERE "total" > $1 AND "_nf_region" = \'EU\'' in compiled.sql


def test_a_group_by_sends_its_parameters_and_its_rows_read_them_too():
    compiled = _compiled(
        '{ ordersGroupBy(by: [status], region: "EU") '
        "{ groupKey aggregate { count } nodes { id } } }"
    )
    # One statement: the groups' rows are read under the same parameters, in it.
    assert 'WHERE "_nf_region" = \'EU\' GROUP BY "status"' in compiled.sql
    assert compiled.has_nodes and "json_agg(json_object(KEY 'id' VALUE \"id\"))" in compiled.sql
    assert compiled.api_args == {"region": "EU"}


def test_a_read_s_parameters_are_in_its_where_before_its_order_and_limit():
    """They were appended after ORDER BY and LIMIT, a statement no parser reads (the reviews
    demo's example query)."""
    import sqlglot

    compiled = _compiled('{ orders(region: "EU", order_by: [{total: asc}], limit: 5) { id } }')
    assert 'WHERE "_nf_region" = \'EU\' ORDER BY "total" ASC LIMIT $1' in compiled.sql
    sqlglot.parse_one(compiled.sql, read="postgres")
    compiled = _compiled(
        '{ orders(region: "EU", where: {total: {gt: 5}}, order_by: [{total: asc}]) { id } }'
    )
    assert 'WHERE "total" > $1 AND "_nf_region" = \'EU\' ORDER BY "total" ASC' in compiled.sql


def test_a_renamed_argument_fills_its_parameter():
    """#247: the argument is _status because status is a column; the parameter is still status."""
    compiled = _compiled('{ orders(region: "EU", _status: "open") { id } }')
    assert "\"_nf_status\" = 'open'" in compiled.sql
    assert "_nf__status" not in compiled.sql
    assert compiled.api_args == {"region": "EU", "status": "open"}


def _array_schema(required: bool):
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "atlas",
                "domain_id": "sales",
                "schema_name": "films",
                "table_name": "similar_movies",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {
                        "column_name": "_nf_query_vector",
                        "visible_to": [],
                        "native_filter_type": "query_arg",
                        "native_filter_required": required,
                    },
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(
                    column_name="_nf_query_vector", data_type="array(double)", is_nullable=True
                ),
            ]
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


@pytest.mark.parametrize("required, type_", [(True, "[Float!]!"), (False, "[Float!]")])
def test_an_array_parameter_takes_a_list(required, type_):
    """A query vector is a list: a required one built a NonNull of NonNull and the schema
    failed to build (CI, test_mongodb_pipeline_tables_e2e)."""
    schema, ctx = _array_schema(required)
    assert schema.query_type is not None
    field = schema.query_type.fields["similarMovies"]
    assert str(field.args["query_vector"].type) == type_
    query = "{ similarMovies(query_vector: [0.1, 0.2]) { id } }"
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    assert compiled.api_args == {"query_vector": [0.1, 0.2]}
