# Copyright (c) 2026 Kenneth Stott
# Canary: af870f23-c0e4-4042-b071-c6474037eec2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for mutation SQL generation."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import compile_mutation
from provisa.compiler.write_admission import WriteNotAdmitted
from tests.write_governance import admitted, write_governance
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
        },
    ]
    col_types = {
        1: [_col("id", "integer"), _col("amount", "decimal(10,2)"), _col("region", "varchar(50)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestInsertMutation:
    def test_basic_insert(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { insertOrders(input: { amount: 42.0, region: "us-east" }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 1
        m = results[0]
        assert m.mutation_type == "insert"
        assert "INSERT INTO" in m.sql
        assert '"amount"' in m.sql
        assert '"region"' in m.sql
        assert "$1" in m.sql
        assert m.params == [42.0, "us-east"]

    def test_insert_source_id(self):
        schema, ctx = _build()
        doc = parse('mutation { insertOrders(input: { region: "x" }) { affected_rows } }')
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert results[0].source_id == "sales-pg"


class TestUpdateMutation:
    def test_basic_update(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { updateOrders(set: { amount: 99.0 }, where: { id: { eq: 1 } }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.mutation_type == "update"
        assert "UPDATE" in m.sql
        assert "SET" in m.sql
        assert "WHERE" in m.sql
        assert m.params == [99.0, 1]


class TestDeleteMutation:
    def test_basic_delete(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { deleteOrders(where: { id: { eq: 5 } }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.mutation_type == "delete"
        assert "DELETE FROM" in m.sql
        assert "WHERE" in m.sql
        assert m.params == [5]


class TestNoSQLRejection:
    def test_nosql_source_rejected(self):
        # A table whose source takes no writes (a MongoDB collection, executor/write_capability.py)
        # is refused by the one write admission, naming the table and the operation.
        from provisa.compiler.write_admission import WriteNotSupported

        _schema, ctx = _build()
        doc = parse('mutation { insertOrders(input: { region: "x" }) { affected_rows } }')
        m = compile_mutation(doc, ctx, {"sales-pg": "mongodb"})[0]
        with pytest.raises(WriteNotSupported, match="does not take INSERT"):
            admitted(m.sql, write_governance(_ORDERS, write_ops={1: set()}), m.params)


_ORDERS = {"public.orders": (1, ["id", "region", "amount", "status"])}
_US = {1: "region = 'us'"}


class TestRLSOnMutation:
    """The role's row filter on a compiled mutation is applied by the one write admission
    (compiler/write_admission.py) in the governance stage every surface's write passes."""

    def _compiled(self, mutation: str):
        _schema, ctx = _build()
        return compile_mutation(parse(mutation), ctx, {"sales-pg": "postgresql"})[0]

    def test_rls_injected_into_update(self):
        m = self._compiled(
            "mutation { updateOrders(set: { amount: 1.0 }, where: { id: { eq: 1 } }) "
            "{ affected_rows } }"
        )
        governed = admitted(m.sql, write_governance(_ORDERS, rls=_US), m.params)
        assert "\"region\" = 'us'" in governed
        assert " AND " in governed

    def test_rls_injected_into_delete(self):
        m = self._compiled("mutation { deleteOrders(where: { id: { eq: 1 } }) { affected_rows } }")
        governed = admitted(m.sql, write_governance(_ORDERS, rls=_US), m.params)
        assert "\"region\" = 'us'" in governed

    def test_an_insert_is_checked_against_the_filter_not_narrowed_by_it(self):
        outside = self._compiled(
            'mutation { insertOrders(input: { region: "x" }) { affected_rows } }'
        )
        with pytest.raises(WriteNotAdmitted, match="outside role"):
            admitted(outside.sql, write_governance(_ORDERS, rls=_US), outside.params)
        inside = self._compiled(
            'mutation { insertOrders(input: { region: "us" }) { affected_rows } }'
        )
        # INSERT has no WHERE: admitted as compiled
        assert admitted(inside.sql, write_governance(_ORDERS, rls=_US), inside.params) == inside.sql

    def test_no_rls_when_no_rule(self):
        m = self._compiled("mutation { deleteOrders(where: { id: { eq: 1 } }) { affected_rows } }")
        governed = admitted(m.sql, write_governance(_ORDERS), m.params)
        # No row filter: only its RETURNING is governed, the star read as the served columns.
        statement, returned = governed.split(" RETURNING ")
        assert statement == m.sql.split(" RETURNING ")[0]
        assert "*" not in returned and '"orders"."id"' in returned


class TestUpsertMutation:
    def test_basic_upsert(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsertOrders(
                    input: { id: 1, amount: 42.0, region: "us-east" }
                    on_conflict: [id]
                ) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 1
        m = results[0]
        assert m.mutation_type == "upsert"
        assert "INSERT INTO" in m.sql
        assert "ON CONFLICT" in m.sql
        assert '"id"' in m.sql
        assert "EXCLUDED" in m.sql
        assert "RETURNING" in m.sql

    def test_upsert_do_nothing_when_all_conflict(self):
        """When all input columns are conflict columns, emit DO NOTHING."""
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsertOrders(
                    input: { id: 1 }
                    on_conflict: [id]
                ) { affected_rows }
            }
        """)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert "DO NOTHING" in m.sql

    def test_upsert_source_id(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsertOrders(
                    input: { id: 1, region: "x" }
                    on_conflict: [id]
                ) { affected_rows }
            }
        """)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert results[0].source_id == "sales-pg"

    def test_upsert_params(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsertOrders(
                    input: { id: 5, amount: 99.9, region: "eu" }
                    on_conflict: [id]
                ) { affected_rows }
            }
        """)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.params == [5, 99.9, "eu"]
