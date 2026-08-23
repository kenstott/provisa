# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9d4e-1b83-4c56-9e7a-2d5f8c0b1a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1359 — JSON:API aggregate/group-by protocol parity.

Mirrors tests/unit/test_group_by.py's schema-fixture pattern: real GraphQL
schema built via generate_schema, real compile_query — no mocked pipeline.
"""

from graphql import parse, validate

from provisa.api.jsonapi.generator import (
    _build_agg_selection,
    _build_group_by_graphql_query,
    _build_group_by_node_selection,
    _get_agg_fields_type,
    _get_relationship_fields,
    _parse_aggregate_param,
    _parse_group_by_param,
    _relationship_scalars,
    _resolve_query_field,
)
from provisa.api.jsonapi.naming import relationship_name_maps, relationship_scalar_maps
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.executor.serialize import serialize_aggregate, serialize_group_by


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(enable_aggregates: bool = True, enable_group_by: bool = True):
    """A single ``orders`` table, modeled after pet_store's ``inquiries``
    (config/provisa-install.yaml:823-824), which has both flags on."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_aggregates": enable_aggregates,
            "enable_group_by": enable_group_by,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
        ],
    }
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestParseAggregateParam:
    def test_absent_means_not_requested(self):
        assert _parse_aggregate_param(None) is None

    def test_true_means_all_functions(self):
        assert _parse_aggregate_param("true") == []
        assert _parse_aggregate_param("1") == []
        assert _parse_aggregate_param("") == []

    def test_comma_separated_functions(self):
        assert _parse_aggregate_param("count,sum") == ["count", "sum"]

    def test_strips_whitespace(self):
        assert _parse_aggregate_param(" count , sum ") == ["count", "sum"]


class TestParseGroupByParam:
    def test_absent_means_empty(self):
        assert _parse_group_by_param(None) == []
        assert _parse_group_by_param("") == []

    def test_comma_separated_columns(self):
        assert _parse_group_by_param("region,status") == ["region", "status"]


class TestResolveQueryField:
    def test_resolves_aggregate_field_when_enabled(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=True, enable_group_by=False)
        field = _resolve_query_field(schema.query_type, "orders", "_aggregate", "Aggregate")
        assert field == "orders_aggregate"

    def test_resolves_group_by_field_when_enabled(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=True)
        field = _resolve_query_field(schema.query_type, "orders", "_group_by", "GroupBy")
        assert field == "orders_group_by"

    def test_returns_none_when_aggregate_disabled(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=True)
        field = _resolve_query_field(schema.query_type, "orders", "_aggregate", "Aggregate")
        assert field is None

    def test_returns_none_when_group_by_disabled(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=True, enable_group_by=False)
        field = _resolve_query_field(schema.query_type, "orders", "_group_by", "GroupBy")
        assert field is None

    def test_returns_none_when_both_disabled(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=False)
        assert _resolve_query_field(schema.query_type, "orders", "_aggregate", "Aggregate") is None
        assert _resolve_query_field(schema.query_type, "orders", "_group_by", "GroupBy") is None


class TestGetAggFieldsType:
    def test_from_aggregate_root_field(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=True, enable_group_by=False)
        root = schema.query_type.fields["orders_aggregate"]
        agg_fields_type = _get_agg_fields_type(root)
        assert agg_fields_type is not None
        assert "count" in agg_fields_type.fields
        assert "sum" in agg_fields_type.fields
        assert "avg" in agg_fields_type.fields

    def test_from_group_by_root_field(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=True)
        root = schema.query_type.fields["orders_group_by"]
        agg_fields_type = _get_agg_fields_type(root)
        assert agg_fields_type is not None
        assert "count" in agg_fields_type.fields


class TestBuildAggSelection:
    def test_all_functions_when_filter_is_none(self):
        schema, _ = _build_schema_and_ctx()
        agg_fields_type = _get_agg_fields_type(schema.query_type.fields["orders_aggregate"])
        selection = _build_agg_selection(agg_fields_type, None)
        assert "count" in selection
        assert "sum {" in selection and "amount" in selection
        assert "avg {" in selection and "amount" in selection
        assert "min {" in selection
        assert "max {" in selection

    def test_only_requested_functions(self):
        schema, _ = _build_schema_and_ctx()
        agg_fields_type = _get_agg_fields_type(schema.query_type.fields["orders_aggregate"])
        selection = _build_agg_selection(agg_fields_type, ["count", "sum"])
        assert selection.startswith("count sum {")
        assert "amount" in selection
        assert "avg" not in selection

    def test_scalar_field_has_no_subselection(self):
        schema, _ = _build_schema_and_ctx()
        agg_fields_type = _get_agg_fields_type(schema.query_type.fields["orders_aggregate"])
        selection = _build_agg_selection(agg_fields_type, ["count"])
        assert selection == "count"


class TestBuildGroupByGraphqlQuery:
    def test_basic_by_arg(self):
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count", {}, [], None, None
        )
        assert q == "{ orders_group_by(by: [region]) { groupKey aggregate { count } } }"

    def test_multiple_by_columns(self):
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region", "status"], "count", {}, [], None, None
        )
        assert "by: [region, status]" in q

    def test_with_where_filter(self):
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count", {"status": {"eq": "active"}}, [], None, None
        )
        assert 'where: {status: {eq: "active"}}' in q

    def test_with_limit_offset(self):
        q = _build_group_by_graphql_query("orders_group_by", ["region"], "count", {}, [], 10, 5)
        assert "limit: 10" in q
        assert "offset: 5" in q

    def test_with_sort(self):
        q = _build_group_by_graphql_query(
            "orders_group_by",
            ["region"],
            "count",
            {},
            [{"field": "region", "dir": "asc"}],
            None,
            None,
        )
        assert "order_by: {region: asc}" in q

    def test_by_columns_translated_to_gql_convention(self):
        """REQ-1361: ?groupBy= takes API-native (physical) column names; the
        synthesized GraphQL text must use the schema's GQL-convention spelling."""
        _naming.configure(gql="apollo_graphql")
        try:
            q = _build_group_by_graphql_query(
                "orders_group_by", ["user_id"], "count", {}, [], None, None
            )
        finally:
            _naming.configure(gql="snake")
        assert "by: [userId]" in q
        assert "user_id" not in q

    def test_produced_query_parses_and_validates(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count sum { amount }", {}, [], None, None
        )
        doc = parse(q)
        assert validate(schema, doc) == []

    def test_node_selection_adds_nodes_field(self):
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count", {}, [], None, None, "id region amount"
        )
        assert "nodes { id region amount }" in q

    def test_no_node_selection_omits_nodes_field(self):
        q = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count", {}, [], None, None
        )
        assert "nodes" not in q


class TestAggregateResponseShape:
    """REQ-1359: aggregate results shape as {"data": null, "meta": {"aggregate": {...}}}."""

    def test_serialize_aggregate_matches_jsonapi_meta_shape(self):
        schema, ctx = _build_schema_and_ctx(enable_aggregates=True, enable_group_by=False)
        doc = parse("""
            query {
                orders_aggregate {
                    aggregate { count sum { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)[0]

        # Column order follows compiled.columns; count then sum.amount per the query above.
        fake_row = (5, 250.0)
        shaped = serialize_aggregate(
            [fake_row],
            compiled.columns,
            None,
            None,
            compiled.root_field,
            agg_alias=compiled.agg_alias,
        )
        agg_payload = shaped["data"][compiled.root_field][compiled.agg_alias]

        # This is exactly what the JSON:API endpoint wraps as {"data": None, "meta": {"aggregate": ...}}.
        jsonapi_doc = {"data": None, "meta": {"aggregate": agg_payload}}
        assert jsonapi_doc["data"] is None
        assert jsonapi_doc["meta"]["aggregate"]["count"] == 5
        assert jsonapi_doc["meta"]["aggregate"]["sum"]["amount"] == 250.0


class TestGroupByResponseShape:
    """REQ-1359: group-by rows shape as typed-but-resourceless JSON:API rows."""

    def test_serialize_group_by_matches_jsonapi_row_shape(self):
        schema, ctx = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=True)
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)[0]
        assert compiled.is_group_by is True

        fake_rows = [("east", 3), ("west", 7)]
        shaped = serialize_group_by(fake_rows, compiled.columns, None, None, compiled.root_field)
        group_rows = shaped["data"][compiled.root_field]

        data = [
            {"type": "ordersGroupBy", "id": str(idx), "attributes": row}
            for idx, row in enumerate(group_rows)
        ]

        assert data[0]["type"] == "ordersGroupBy"
        assert data[0]["id"] == "0"
        assert data[0]["attributes"]["groupKey"]["region"] == "east"
        assert data[0]["attributes"]["aggregate"]["count"] == 3
        assert data[1]["attributes"]["groupKey"]["region"] == "west"
        assert data[1]["attributes"]["aggregate"]["count"] == 7


class TestDisabledTableRejected:
    """REQ-1359: a table without enable_aggregates/enable_group_by has no such root field —
    _resolve_query_field returns None, which the endpoint maps to a 400, never a 500."""

    def test_aggregate_field_absent_when_flag_off(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=False)
        assert "orders_aggregate" not in schema.query_type.fields
        assert _resolve_query_field(schema.query_type, "orders", "_aggregate", "Aggregate") is None

    def test_group_by_field_absent_when_flag_off(self):
        schema, _ = _build_schema_and_ctx(enable_aggregates=False, enable_group_by=False)
        assert "orders_group_by" not in schema.query_type.fields
        assert _resolve_query_field(schema.query_type, "orders", "_group_by", "GroupBy") is None


def _build_schema_with_relationship():
    """``orders`` group-by joined to a ``customers`` relationship, for include dot-path tests."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]
    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
        ],
        2: [_col("id", "integer"), _col("name", "varchar(100)"), _col("email", "varchar(200)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


def _node_selection(schema, ctx, base_scalars, include_param):
    """Call the helper the way the handler does — with the physical → GQL maps it builds first."""
    gql_rels = list(_get_relationship_fields(schema, "orders").values())
    _, rel_physical_to_gql = relationship_name_maps(gql_rels)
    rel_scalars = {rel: _relationship_scalars(schema, "orders", rel) for rel in gql_rels}
    _, rel_scalar_physical_to_gql = relationship_scalar_maps(
        ctx, ctx.tables["orders"].type_name, rel_scalars
    )
    return _build_group_by_node_selection(
        schema,
        "orders",
        base_scalars,
        include_param,
        rel_physical_to_gql,
        rel_scalar_physical_to_gql,
    )


class TestGroupByNodeSelection:
    """REQ-1408/REQ-1417: ?include= takes the same rel/rel.col projection gRPC and REST take,
    named physically on both segments."""

    def test_base_scalars_only_without_include(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id", "amount"], None)
        assert err is None
        assert selection == "id amount"

    def test_bare_relationship_selects_every_related_scalar(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id"], "customer")
        assert err is None
        assert selection == "id customer { id name email }"

    def test_dot_path_selects_only_that_column(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id"], "customer.email")
        assert err is None
        assert selection == "id customer { email }"

    def test_dot_paths_on_one_relationship_merge(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id"], "customer.email,customer.name")
        assert err is None
        assert selection == "id customer { email name }"

    def test_selection_parses_as_graphql(self):
        schema, ctx = _build_schema_with_relationship()
        selection, _ = _node_selection(schema, ctx, ["id"], "customer.email")
        query = _build_group_by_graphql_query(
            "orders_group_by", ["region"], "count", {}, [], None, None, selection
        )
        assert not validate(schema, parse(query))

    def test_unknown_relationship_is_an_error(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id"], "nope.email")
        assert selection == ""
        assert err == "Unknown relationship 'nope'"

    def test_unknown_column_on_relationship_is_an_error(self):
        schema, ctx = _build_schema_with_relationship()
        selection, err = _node_selection(schema, ctx, ["id"], "customer.nope")
        assert selection == ""
        assert err == "Unknown field 'nope' on relationship 'customer'"
