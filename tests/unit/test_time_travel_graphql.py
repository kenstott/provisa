# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f1d2e-6a4c-4e9b-8f7d-1c5a9e2b4d60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1163 (amended 2026-10-25): in GraphQL, time travel is an argument of the field whose
relation has versions -- a bitemporal table's or view's field takes as_of and as_recorded, an
Iceberg or Delta table's as_recorded alone, a related bitemporal relation's field too -- and the
compiler writes each field's time on its reference in the SQL call form the pipeline lifts per
reference, so two aliases read one relation at two times."""

from __future__ import annotations

import pytest
from graphql import parse, validate

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.mv.time_travel import lift


def _col(name: str, data_type: str) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(tid: int, name: str, source: str, columns: list[str]) -> dict:
    return {
        "id": tid,
        "source_id": source,
        "domain_id": "bank",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


_KINDS = {"eod_totals": "bitemporal", "events": "snapshot", "accounts": "plain"}


@pytest.fixture
def schema_and_ctx():
    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=[
            _table(1, "accounts", "ledger", ["id"]),
            _table(2, "eod_totals", "ledger", ["id", "account_id", "total"]),
            _table(3, "events", "lake", ["id"]),
        ],
        relationships=[
            {
                "id": "acct-eod",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "account_id",
                "cardinality": "one-to-many",
            }
        ],
        column_types={
            1: [_col("id", "integer")],
            2: [_col("id", "integer"), _col("account_id", "integer"), _col("total", "integer")],
            3: [_col("id", "integer")],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "bank", "description": "Bank"}],
        time_travel={2: "bitemporal", 3: "snapshot"},
    )
    return generate_schema(si), build_context(si)


def _compiled(schema, ctx, query: str) -> list:
    document = parse(query)
    assert not validate(schema, document)
    return compile_query(document, ctx)


def test_each_relations_field_takes_the_arguments_its_kind_of_versions_does(schema_and_ctx):
    schema, _ = schema_and_ctx
    fields = schema.query_type.fields
    assert {"as_of", "as_recorded"} <= set(fields["eod_totals"].args)
    assert "as_recorded" in fields["events"].args and "as_of" not in fields["events"].args
    assert not {"as_of", "as_recorded"} & set(fields["accounts"].args)
    related = schema.get_type("Accounts").fields["eod_totals"]
    assert {"as_of", "as_recorded"} <= set(related.args)


def test_two_aliases_read_one_view_at_two_times(schema_and_ctx):
    schema, ctx = schema_and_ctx
    compiled = _compiled(
        schema,
        ctx,
        '{ before: eod_totals(as_recorded: "2026-10-01T17:00:00") { id total } '
        'after: eod_totals(as_recorded: "2026-10-05T17:00:00") { id total } }',
    )
    times = []
    for query in compiled:
        sql, travel = lift(query.sql, lambda t: _KINDS.get(t.name))
        assert "=>" not in sql
        [at] = travel.by_alias.values()
        times.append(at)
    assert times == [
        "@TIMESTAMP '2026-10-01 17:00:00.000000'",
        "@TIMESTAMP '2026-10-05 17:00:00.000000'",
    ]


def test_a_related_bitemporal_field_is_read_at_its_own_time(schema_and_ctx):
    schema, ctx = schema_and_ctx
    [query] = _compiled(
        schema, ctx, '{ accounts { id eod_totals(as_of: "2026-09-30") { total } } }'
    )
    _, travel = lift(query.sql, lambda t: _KINDS.get(t.name))
    assert list(travel.by_alias.values()) == ["TIMESTAMP '2026-09-30 00:00:00.000000'"]


def test_a_snapshot_field_has_no_valid_time(schema_and_ctx):
    schema, _ = schema_and_ctx
    errors = validate(schema, parse('{ events(as_of: "2026-10-01") { id } }'))
    assert errors and "as_of" in errors[0].message


def test_a_malformed_time_is_refused_naming_the_argument(schema_and_ctx):
    schema, ctx = schema_and_ctx
    with pytest.raises(ValueError):
        _compiled(schema, ctx, '{ eod_totals(as_recorded: "last tuesday") { id } }')


def test_rest_passes_the_times_as_the_fields_arguments(schema_and_ctx):
    """REST and MCP offer what the GraphQL field offers (REQ-1929): ?as_recorded= is the field's
    argument, refused by name on a relation that keeps no versions."""
    from provisa.api._query_helpers import RequestRefused, build_graphql_query, request_variables

    schema, ctx = schema_and_ctx
    variables = request_variables(
        schema, "eod_totals", {}, None, {"as_recorded": "2026-10-01T17:00:00"}
    )
    query = build_graphql_query("eod_totals", ["id"], variables, [], None, None)
    document = parse(query)
    assert not validate(schema, document)
    [compiled] = compile_query(document, ctx, variables.values)
    _, travel = lift(compiled.sql, lambda t: _KINDS.get(t.name))
    assert list(travel.by_alias.values()) == ["@TIMESTAMP '2026-10-01 17:00:00.000000'"]
    with pytest.raises(RequestRefused, match="accounts takes no as_of"):
        request_variables(schema, "accounts", {}, None, {"as_of": "2026-10-01"})
    with pytest.raises(RequestRefused, match="invalid as_recorded"):
        request_variables(schema, "eod_totals", {}, None, {"as_recorded": "soon"})
