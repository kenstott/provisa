# Copyright (c) 2026 Kenneth Stott
# Canary: cf9539d4-b269-42ad-a38d-88f18037ce92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for cursor pagination — helpers, serialization, SQL compilation (REQ-218)."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    build_context,
    compile_query,
)


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "customer_id", "visible_to": ["admin", "analyst"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables, relationships=[], column_types=column_types,
        naming_rules=[], role=role, domains=domains, relay_pagination=True,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


@pytest.fixture
def schema_and_ctx():
    return _build_schema_and_ctx()


class TestCursorPagination:
    """Cursor pagination via _connection fields (REQ-218)."""

    def test_connection_field_exists_in_schema(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        assert "orders_connection" in schema.query_type.fields

    def test_connection_type_structure(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        conn_field = schema.query_type.fields["orders_connection"]
        conn_type = getattr(conn_field.type, "of_type", conn_field.type)
        assert "edges" in conn_type.fields
        assert "pageInfo" in conn_type.fields
        edges_type = conn_type.fields["edges"].type
        edge_type = edges_type.of_type.of_type.of_type
        assert "cursor" in edge_type.fields
        assert "node" in edge_type.fields
        pi_type = conn_type.fields["pageInfo"].type.of_type
        for f in ("hasNextPage", "hasPreviousPage", "startCursor", "endCursor"):
            assert f in pi_type.fields

    def test_connection_first_compiles(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_connection(first: 10) {
                edges { cursor node { id amount } }
                pageInfo { hasNextPage endCursor }
            } }
        """)
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        q = results[0]
        assert q.is_connection and not q.is_backward
        assert q.page_size == 10
        assert "LIMIT $1" in q.sql
        assert q.params == [11]
        assert 'ORDER BY "id" ASC' in q.sql

    def test_connection_first_after_compiles(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        from provisa.compiler.cursor import encode_cursor
        cursor = encode_cursor([42])
        doc = parse(f"""
            {{ orders_connection(first: 5, after: "{cursor}") {{
                edges {{ cursor node {{ id }} }}
                pageInfo {{ hasNextPage hasPreviousPage }}
            }} }}
        """)
        assert not validate(schema, doc)
        q = compile_query(doc, ctx)[0]
        assert q.has_cursor
        assert 'WHERE "id" > $1' in q.sql
        assert "LIMIT $2" in q.sql
        assert q.params == [42, 6]

    def test_connection_last_before_compiles(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        from provisa.compiler.cursor import encode_cursor
        cursor = encode_cursor([100])
        doc = parse(f"""
            {{ orders_connection(last: 5, before: "{cursor}") {{
                edges {{ cursor node {{ id }} }}
                pageInfo {{ hasPreviousPage }}
            }} }}
        """)
        assert not validate(schema, doc)
        q = compile_query(doc, ctx)[0]
        assert q.is_backward and q.has_cursor
        assert '"id" < $1' in q.sql
        assert "LIMIT $2" in q.sql
        assert q.params == [100, 6]
        assert 'ORDER BY "id" DESC' in q.sql

    def test_connection_with_where(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_connection(first: 10, where: { region: { eq: "us" } }) {
                edges { cursor node { id region } }
                pageInfo { hasNextPage }
            } }
        """)
        assert not validate(schema, doc)
        q = compile_query(doc, ctx)[0]
        assert '"region" = $1' in q.sql
        assert "LIMIT $2" in q.sql
        assert q.params == ["us", 11]

    def test_connection_with_order_by(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_connection(first: 10, order_by: [{ amount: desc }]) {
                edges { cursor node { id amount } }
                pageInfo { hasNextPage }
            } }
        """)
        assert not validate(schema, doc)
        q = compile_query(doc, ctx)[0]
        assert '"amount" DESC' in q.sql
        assert q.sort_columns == ["amount"]

    def test_connection_sort_column_auto_added(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_connection(first: 5, order_by: [{ amount: asc }]) {
                edges { cursor node { id } }
                pageInfo { hasNextPage }
            } }
        """)
        q = compile_query(doc, ctx)[0]
        assert '"amount"' in q.sql
        assert any(c.field_name == "amount" for c in q.columns)

    def test_connection_backward_reverses_order(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_connection(last: 5, order_by: [{ amount: asc }]) {
                edges { cursor node { id amount } }
                pageInfo { hasPreviousPage }
            } }
        """)
        q = compile_query(doc, ctx)[0]
        assert q.is_backward
        assert '"amount" DESC' in q.sql


class TestCursorHelpers:
    """Unit tests for cursor encode/decode and WHERE generation."""

    def test_encode_decode_roundtrip(self):
        from provisa.compiler.cursor import encode_cursor, decode_cursor
        assert decode_cursor(encode_cursor([42, "hello"])) == [42, "hello"]

    def test_decode_invalid_cursor(self):
        from provisa.compiler.cursor import decode_cursor
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor("not-valid-base64!!!")

    def test_cursor_where_single_column(self):
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector
        c = ParamCollector()
        assert cursor_where_clause(["id"], [42], "forward", c, None) == '"id" > $1'
        assert c.params == [42]

    def test_cursor_where_multi_column(self):
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector
        c = ParamCollector()
        result = cursor_where_clause(
            ["created_at", "id"], ["2024-01-01", 5], "forward", c, None,
        )
        assert result == '("created_at", "id") > ($1, $2)'

    def test_cursor_where_backward(self):
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector
        c = ParamCollector()
        assert cursor_where_clause(["id"], [100], "backward", c, None) == '"id" < $1'

    def test_cursor_where_with_alias(self):
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector
        c = ParamCollector()
        assert cursor_where_clause(["id"], [42], "forward", c, "t0") == '"t0"."id" > $1'

    def test_cursor_mismatch_raises(self):
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector
        with pytest.raises(ValueError, match="Cursor has 1 values but sort key has 2"):
            cursor_where_clause(["a", "b"], [1], "forward", ParamCollector(), None)

    def test_reverse_order(self):
        from provisa.compiler.cursor import reverse_order
        assert reverse_order('"id" ASC') == '"id" DESC'
        assert reverse_order('"id" DESC') == '"id" ASC'
        assert reverse_order('"a" ASC NULLS FIRST') == '"a" DESC NULLS LAST'
        assert reverse_order('"a" DESC NULLS LAST') == '"a" ASC NULLS FIRST'

    def test_first_and_last_raises(self):
        from provisa.compiler.cursor import apply_cursor_pagination
        from provisa.compiler.params import ParamCollector
        with pytest.raises(ValueError, match="Cannot use both"):
            apply_cursor_pagination(
                {"first": 10, "last": 5}, ["id"], ParamCollector(), None,
            )


class TestConnectionSerialization:
    """Unit tests for serialize_connection."""

    def test_serialize_forward_pagination(self):
        from provisa.compiler.cursor import encode_cursor
        from provisa.executor.serialize import serialize_connection
        compiled = CompiledQuery(
            sql="SELECT ...", params=[], root_field="orders_connection",
            columns=[ColumnRef(None, "id", "id", None), ColumnRef(None, "amount", "amount", None)],
            sources={"pg"}, is_connection=True, is_backward=False,
            sort_columns=["id"], page_size=2, has_cursor=False,
        )
        rows = [(1, 100), (2, 200), (3, 300)]
        result = serialize_connection(rows, compiled)
        data = result["data"]["orders_connection"]
        assert len(data["edges"]) == 2
        assert data["edges"][0]["node"] == {"id": 1, "amount": 100}
        assert data["pageInfo"]["hasNextPage"] is True
        assert data["pageInfo"]["hasPreviousPage"] is False
        assert data["pageInfo"]["startCursor"] == encode_cursor([1])
        assert data["pageInfo"]["endCursor"] == encode_cursor([2])

    def test_serialize_no_more_pages(self):
        from provisa.executor.serialize import serialize_connection
        compiled = CompiledQuery(
            sql="SELECT ...", params=[], root_field="orders_connection",
            columns=[ColumnRef(None, "id", "id", None)], sources={"pg"},
            is_connection=True, is_backward=False, sort_columns=["id"],
            page_size=5, has_cursor=False,
        )
        result = serialize_connection([(1,), (2,)], compiled)
        assert result["data"]["orders_connection"]["pageInfo"]["hasNextPage"] is False

    def test_serialize_backward_reverses(self):
        from provisa.executor.serialize import serialize_connection
        compiled = CompiledQuery(
            sql="SELECT ...", params=[100], root_field="orders_connection",
            columns=[ColumnRef(None, "id", "id", None)], sources={"pg"},
            is_connection=True, is_backward=True, sort_columns=["id"],
            page_size=2, has_cursor=True,
        )
        rows = [(99,), (98,), (97,)]
        result = serialize_connection(rows, compiled)
        data = result["data"]["orders_connection"]
        assert len(data["edges"]) == 2
        assert data["edges"][0]["node"] == {"id": 98}
        assert data["edges"][1]["node"] == {"id": 99}
        assert data["pageInfo"]["hasPreviousPage"] is True
        assert data["pageInfo"]["hasNextPage"] is True

    def test_serialize_empty_result(self):
        from provisa.executor.serialize import serialize_connection
        compiled = CompiledQuery(
            sql="SELECT ...", params=[], root_field="orders_connection",
            columns=[ColumnRef(None, "id", "id", None)], sources={"pg"},
            is_connection=True, is_backward=False, sort_columns=["id"],
            page_size=10, has_cursor=False,
        )
        result = serialize_connection([], compiled)
        data = result["data"]["orders_connection"]
        assert data["edges"] == []
        assert data["pageInfo"]["startCursor"] is None
        assert data["pageInfo"]["hasNextPage"] is False


def _base_tables():
    return [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
    ]


def _base_column_types():
    return {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
        ],
    }


def _base_role():
    return {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}


def _base_domains():
    return [{"id": "sales", "description": "Sales"}]


class TestRelayPaginationOptIn:
    """relay_pagination=False (default) suppresses _connection fields."""

    def test_no_connection_field_by_default(self):
        si = SchemaInput(
            tables=_base_tables(), relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            # relay_pagination defaults to False
        )
        schema = generate_schema(si)
        assert "orders_connection" not in schema.query_type.fields

    def test_no_connection_field_when_false(self):
        si = SchemaInput(
            tables=_base_tables(), relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            relay_pagination=False,
        )
        schema = generate_schema(si)
        assert "orders_connection" not in schema.query_type.fields

    def test_connection_field_present_when_true(self):
        si = SchemaInput(
            tables=_base_tables(), relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            relay_pagination=True,
        )
        schema = generate_schema(si)
        assert "orders_connection" in schema.query_type.fields

    def test_table_level_relay_overrides_global_false(self):
        tables = _base_tables()
        tables[0]["relay_pagination"] = True  # table-level override
        si = SchemaInput(
            tables=tables, relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            relay_pagination=False,  # global is False
        )
        schema = generate_schema(si)
        assert "orders_connection" in schema.query_type.fields

    def test_table_level_relay_false_overrides_global_true(self):
        tables = _base_tables()
        tables[0]["relay_pagination"] = False  # table-level opt-out
        si = SchemaInput(
            tables=tables, relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            relay_pagination=True,  # global is True
        )
        schema = generate_schema(si)
        assert "orders_connection" not in schema.query_type.fields

    def test_regular_fields_present_regardless_of_relay(self):
        si = SchemaInput(
            tables=_base_tables(), relationships=[], column_types=_base_column_types(),
            naming_rules=[], role=_base_role(), domains=_base_domains(),
            relay_pagination=False,
        )
        schema = generate_schema(si)
        assert "orders" in schema.query_type.fields
        assert "orders_aggregate" in schema.query_type.fields
