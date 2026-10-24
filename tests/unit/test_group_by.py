# Copyright (c) 2026 Kenneth Stott
# Canary: a3f1b8e2-5c94-4d7f-8e21-6b9a3d0f1c42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for REQ-653, REQ-654, REQ-655: group_by root field, GroupByRow type, FILTER/HAVING."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.context import build_context


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(enable_group_by: bool = True, enable_aggregates: bool = False):
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
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
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


def _build_schema_and_ctx_with_rel():
    """orders (table 1) → customers (table 2), many-to-one via customer_id."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_group_by": True,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
        ],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    relationships = [
        {
            "id": "r1",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    ]
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


@pytest.fixture
def schema_and_ctx():
    return _build_schema_and_ctx()


@pytest.fixture
def schema_and_ctx_rel():
    return _build_schema_and_ctx_with_rel()


class TestGroupBySchema:
    """REQ-653/654: Schema structure for _group_by field."""

    def test_group_by_field_present_when_enabled(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        assert "orders_group_by" in schema.query_type.fields

    def test_group_by_field_absent_when_disabled(self):
        schema, _ = _build_schema_and_ctx(enable_group_by=False)
        assert "orders_group_by" not in schema.query_type.fields

    def test_group_by_field_absent_by_default(self):
        _naming.configure(gql="snake")
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": ["delete", "insert", "update"],
                "write_returns_rows": True,
                "write_refused_forms": [],
            },
        ]
        column_types = {1: [_col("id", "integer")]}
        role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=column_types,
            naming_rules=[],
            role=role,
            domains=[{"id": "sales", "description": "Sales"}],
        )
        schema = generate_schema(si)
        assert "orders_group_by" not in schema.query_type.fields

    def test_group_by_return_type_is_list(self, schema_and_ctx):
        from graphql import GraphQLList, GraphQLNonNull

        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        # NonNull(List(NonNull(GroupByRow)))
        assert isinstance(field.type, GraphQLNonNull)
        assert isinstance(field.type.of_type, GraphQLList)

    def test_group_by_row_has_group_key(self, schema_and_ctx):
        from graphql import GraphQLNonNull

        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "groupKey" in row_type.fields
        gk = row_type.fields["groupKey"]
        assert isinstance(gk.type, GraphQLNonNull)

    def test_group_by_row_has_aggregate(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "aggregate" in row_type.fields

    def test_group_by_row_has_nodes(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "nodes" in row_type.fields

    def test_aggregate_field_has_where_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        agg_field = row_type.fields["aggregate"]
        assert "where" in agg_field.args

    def test_group_by_root_has_by_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "by" in field.args

    def test_group_by_root_has_where_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "where" in field.args

    def test_group_by_root_has_having_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "having" in field.args

    def test_group_by_root_has_limit_offset(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "limit" in field.args
        assert "offset" in field.args

    def test_group_by_root_has_order_by_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "order_by" in field.args

    def test_enable_aggregates_independent(self):
        # enable_group_by=True, enable_aggregates=False → group_by present, _aggregate absent
        schema, _ = _build_schema_and_ctx(enable_group_by=True, enable_aggregates=False)
        assert "orders_group_by" in schema.query_type.fields
        assert "orders_aggregate" not in schema.query_type.fields

    def test_both_flags_independent(self):
        schema, _ = _build_schema_and_ctx(enable_group_by=True, enable_aggregates=True)
        assert "orders_group_by" in schema.query_type.fields
        assert "orders_aggregate" in schema.query_type.fields

    def test_group_by_query_validates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate {
                        count
                        sum { amount }
                    }
                }
            }
        """)
        errors = validate(schema, doc)
        assert errors == []

    def test_group_by_query_with_having_validates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    having: { count: { gt: 5 } }
                ) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        errors = validate(schema, doc)
        assert errors == []


class TestGroupBySQLGen:
    """REQ-654: SQL generation for _group_by field."""

    def test_group_by_generates_group_by_clause(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert len(compiled) == 1
        sql = compiled[0].sql
        assert 'GROUP BY "region"' in sql

    def test_group_by_selects_group_columns(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        group_key_refs = [c for c in col_refs if c.nested_in == "groupKey"]
        assert any(c.field_name == "region" for c in group_key_refs)

    def test_group_by_count_nested_in_aggregates(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        agg_refs = [c for c in col_refs if c.nested_in == "aggregate"]
        assert any(c.field_name == "count" for c in agg_refs)

    def test_group_by_sum_nested_in_aggregates_sum(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { sum { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        sum_refs = [c for c in col_refs if c.nested_in == "aggregate.sum"]
        assert any(c.field_name == "amount" for c in sum_refs)

    def test_group_by_sum_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { sum { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'SUM("amount")' in sql

    def test_group_by_multiple_columns(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region, status]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'GROUP BY "region", "status"' in sql
        assert compiled[0].is_group_by is True
        assert compiled[0].group_by_columns == ["region", "status"]

    def test_group_by_where_clause(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], where: { status: { eq: "active" } }) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "WHERE" in sql
        assert "$1" in sql
        assert compiled[0].params == ["active"]

    def test_group_by_limit_offset(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], limit: 10, offset: 5) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "LIMIT $1" in sql
        assert "OFFSET $2" in sql
        assert compiled[0].params == [10, 5]

    def test_group_by_is_group_by_flag(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert compiled[0].is_group_by is True

    def test_group_by_group_by_columns_field(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert compiled[0].group_by_columns == ["region"]

    def test_group_by_avg_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { avg { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'AVG("amount")' in sql

    def test_group_by_min_max_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { min { amount } max { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'MIN("amount")' in sql
        assert 'MAX("amount")' in sql


class TestGroupByFilterHaving:
    """REQ-655: FILTER (WHERE ...) on aggregates and HAVING clause."""

    def test_filter_where_on_aggregates(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "FILTER (WHERE" in sql
        assert "$1" in sql
        assert compiled[0].params[0] == "active"

    def test_filter_where_applied_to_count(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "COUNT(*) FILTER (WHERE" in sql

    def test_filter_where_applied_to_sum(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate(where: { status: { eq: "active" } }) {
                        sum { amount }
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'SUM("amount") FILTER (WHERE' in sql

    def test_having_count_gt(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], having: { count: { gt: 5 } }) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" in sql
        assert "COUNT(*) > $1" in sql
        assert compiled[0].params == [5]

    def test_having_sum_gte(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], having: { sum: { amount: { gte: 1000 } } }) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" in sql
        assert 'SUM("amount") >= $1' in sql
        assert compiled[0].params == [1000]

    def test_having_not_emitted_when_empty(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" not in sql

    def test_filter_and_root_where_both_present(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    where: { status: { eq: "active" } }
                ) {
                    groupKey
                    aggregate(where: { region: { eq: "US" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "FILTER (WHERE" in sql
        assert "WHERE" in sql
        assert len(compiled[0].params) == 2

    def test_having_with_filter_param_ordering(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    having: { count: { gt: 3 } }
                ) {
                    groupKey
                    aggregate(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        params = compiled[0].params
        # FILTER params come first (status="active" $1), then HAVING ($2)
        assert "FILTER (WHERE" in sql
        assert "HAVING" in sql
        assert len(params) == 2


class TestGroupByNodes:
    """Nodes subquery on group_by rows."""

    def test_nodes_field_present_in_schema(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "nodes" in row_type.fields

    def test_nodes_query_validates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                    nodes { id amount }
                }
            }
        """)
        errors = validate(schema, doc)
        assert errors == []

    _NODES = """
        query {
            orders_group_by(by: [region]%s) {
                groupKey
                aggregate { count }
                nodes { id amount }
            }
        }
    """

    def _compiled(self, ctx, where: str = ""):
        return compile_query(parse(self._NODES % where), ctx, variables=None)[0]

    def test_a_group_s_nodes_are_a_column_of_its_one_statement(self, schema_and_ctx):
        # REQ-196: one statement, each group's rows aggregated in it -- never a second statement
        # joined back to the groups on their key.
        _, ctx = schema_and_ctx
        compiled = self._compiled(ctx)
        assert compiled.has_nodes
        assert compiled.sql == (
            'SELECT "region", COUNT(*), COALESCE(json_agg(json_object(KEY \'id\' VALUE "id", '
            "KEY 'amount' VALUE \"amount\")), CAST('[]' AS JSON)) AS \"nodes\" "
            'FROM "public"."orders" GROUP BY "region"'
        )
        nodes = [c for c in compiled.columns if c.nested_in is None]
        assert [(c.field_name, c.is_agg, c.cardinality) for c in nodes] == [
            ("nodes", True, "one-to-many")
        ]

    def test_the_nodes_read_the_group_s_own_where(self, schema_and_ctx):
        _, ctx = schema_and_ctx
        compiled = self._compiled(ctx, ', where: { status: { eq: "active" } }')
        assert '"status" = $1 GROUP BY "region"' in compiled.sql
        assert compiled.params == ["active"]

    def test_nodes_absent_when_not_requested(self, schema_and_ctx):
        _, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregate { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)[0]
        assert not compiled.has_nodes and "json_agg" not in compiled.sql


class TestSerializeGroupBy:
    """A group's row carries its nodes as a JSON list column."""

    def _columns(self):
        from provisa.compiler.sql_gen import ColumnRef

        return [
            ColumnRef(alias=None, column="region", field_name="region", nested_in="groupKey"),
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
            ColumnRef(
                alias=None,
                column="nodes",
                field_name="nodes",
                nested_in=None,
                cardinality="one-to-many",
                is_agg=True,
            ),
        ]

    def test_nodes_attached_per_group(self):
        from provisa.executor.serialize import serialize_rows

        rows = [("US", 2, '[{"id": 1}, {"id": 2}]'), ("EU", 1, '[{"id": 3}]')]
        data = serialize_rows(rows, self._columns(), "orders_group_by")["data"]["orders_group_by"]
        assert data == [
            {
                "groupKey": {"region": "US"},
                "aggregate": {"count": 2},
                "nodes": [{"id": 1}, {"id": 2}],
            },
            {"groupKey": {"region": "EU"}, "aggregate": {"count": 1}, "nodes": [{"id": 3}]},
        ]

    def test_a_relationship_s_value_is_parsed_into_a_node(self):
        from provisa.executor.serialize import serialize_rows

        rows = [("US", 1, '[{"id": 1, "customer": {"name": "Acme"}}]')]
        data = serialize_rows(rows, self._columns(), "orders_group_by")["data"]["orders_group_by"]
        assert data[0]["nodes"] == [{"id": 1, "customer": {"name": "Acme"}}]


class TestGroupByNodesRelationship:
    """REQ-654: nested relationship fields inside group_by nodes (regression)."""

    def _unwrap(self, t):
        while hasattr(t, "of_type"):
            t = t.of_type
        return t

    def _rel_field(self, schema):
        row_type = self._unwrap(schema.query_type.fields["orders_group_by"].type)
        nodes_type = self._unwrap(row_type.fields["nodes"].type)
        # the relationship field is the only one whose type is itself an object with fields
        return next(
            n for n, f in nodes_type.fields.items() if hasattr(self._unwrap(f.type), "fields")
        )

    def test_relationship_is_a_correlated_json_subquery_in_each_node(self, schema_and_ctx_rel):
        schema, ctx = schema_and_ctx_rel
        rel = self._rel_field(schema)
        doc = parse(f"""
            query {{
                orders_group_by(by: [region]) {{
                    groupKey
                    aggregate {{ count }}
                    nodes {{ id {rel} {{ name }} }}
                }}
            }}
        """)
        assert not validate(schema, doc)
        sql = compile_query(doc, ctx, variables=None)[0].sql
        # The table is read under the root alias the relationship's subquery correlates to.
        assert f"KEY 'id' VALUE \"t0\".\"id\", KEY '{rel}' VALUE (SELECT json_object(" in sql
        assert 'FROM "public"."customers" "t1" WHERE "t1"."id" = "t0"."customer_id"' in sql
        assert 'FROM "public"."orders" "t0" GROUP BY "region"' in sql
        assert "JOIN" not in sql.upper()
