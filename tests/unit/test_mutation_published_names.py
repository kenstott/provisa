# Copyright (c) 2026 Kenneth Stott
# Canary: 660ea9f8-bd00-436d-9ad6-9ed0d703e35d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's GraphQL speaks one set of names: its mutations name each column as its reads do
(REQ-032, REQ-212, REQ-2074).

A column's published name is its alias, else the naming convention's name. Reads select it by
that name; so do an insert's input, an update's set, an upsert's conflict columns and a
mutation's where. The SQL a mutation compiles to names the physical columns, which is what the
write grant states and the source holds."""

# Requirements: REQ-032, REQ-033, REQ-035, REQ-212, REQ-2074

from __future__ import annotations

import pytest
from graphql import parse, validate

from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import compile_mutation
from provisa.compiler.schema_gen import SchemaInput, generate_schema

_COLUMNS = ["id", "first_name", "total_cents"]


@pytest.fixture
def built():
    naming.configure(gql="apollo_graphql", sql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "store",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "first_name", "alias": "givenName", "visible_to": ["admin"]},
                {"column_name": "total_cents", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
            "write_grants": [
                {
                    "role": "admin",
                    "operation": op,
                    "columns": [] if op == "delete" else _COLUMNS,
                    "rows": None,
                    "check": None,
                    "presets": [],
                }
                for op in ("insert", "update", "delete")
            ],
        }
    ]
    types = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="first_name", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="total_cents", data_type="integer", is_nullable=True),
    ]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types={1: types},
        naming_rules=[],
        role={"id": "admin", "capabilities": ["write"], "domain_access": ["*"]},
        domains=[{"id": "store", "description": "Store"}],
        source_types={"pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _compiled(built, text):
    schema, ctx = built
    doc = parse(text)
    assert not validate(schema, doc), validate(schema, doc)
    (result,) = compile_mutation(doc, ctx, {"pg": "postgresql"})
    return result


def test_the_inputs_offer_the_names_reads_select(built):
    schema, _ = built
    for type_name in ("CustomersInsertInput", "CustomersSetInput"):
        assert set(schema.type_map[type_name].fields) == {"id", "givenName", "totalCents"}
    assert set(schema.type_map["CustomersConflictColumn"].values) == {
        "id",
        "givenName",
        "totalCents",
    }


def test_a_physical_name_is_not_an_input(built):
    schema, _ = built
    doc = parse('mutation { insertCustomers(input: {id: 1, first_name: "Ana"}) { affected_rows } }')
    assert validate(schema, doc)


def test_an_insert_writes_the_physical_columns(built):
    m = _compiled(
        built,
        'mutation { insertCustomers(input: {id: 1, givenName: "Ana", totalCents: 5}) { affected_rows } }',
    )
    assert '("id", "first_name", "total_cents")' in m.sql
    assert m.params == [1, "Ana", 5]


def test_an_update_sets_and_filters_by_the_physical_columns(built):
    m = _compiled(
        built,
        'mutation { updateCustomers(set: {givenName: "Anita"}, where: {totalCents: {eq: 5}}) '
        "{ affected_rows } }",
    )
    assert 'SET "first_name" = ' in m.sql and 'WHERE "total_cents" = ' in m.sql
    assert "givenName" not in m.sql and "totalCents" not in m.sql


def test_an_upsert_names_its_conflict_column_as_reads_do(built):
    m = _compiled(
        built,
        'mutation { upsertCustomers(input: {id: 1, givenName: "Ana"}, on_conflict: [id]) '
        "{ affected_rows } }",
    )
    assert 'ON CONFLICT ("id") DO UPDATE SET "first_name" = EXCLUDED."first_name"' in m.sql


def test_a_delete_filters_by_the_physical_column(built):
    m = _compiled(
        built, 'mutation { deleteCustomers(where: {givenName: {eq: "Ana"}}) { affected_rows } }'
    )
    assert 'WHERE "first_name" = ' in m.sql


@pytest.mark.parametrize(
    ("where", "condition", "params"),
    [
        ('{givenName: {like: "An%"}}', '"first_name" LIKE %s', ["An%"]),
        ('{givenName: {neq: "Ana"}}', '"first_name" != %s', ["Ana"]),
        ("{totalCents: {gt: 5}}", '"total_cents" > %s', [5]),
        ("{totalCents: {gte: 5}}", '"total_cents" >= %s', [5]),
        ("{totalCents: {lt: 5}}", '"total_cents" < %s', [5]),
        ("{totalCents: {lte: 5}}", '"total_cents" <= %s', [5]),
        ("{totalCents: {in: [1, 2]}}", '"total_cents" IN (%s, %s)', [1, 2]),
        ("{givenName: {is_null: true}}", '"first_name" IS NULL', []),
        (
            '{totalCents: {gt: 1}, givenName: {like: "A%"}}',
            '"total_cents" > %s AND "first_name" LIKE %s',
            [1, "A%"],
        ),
        (
            '{_or: [{totalCents: {eq: 1}}, {givenName: {eq: "Ana"}}]}',
            '("total_cents" = %s OR "first_name" = %s)',
            [1, "Ana"],
        ),
    ],
)
@pytest.mark.parametrize("verb", ["update", "delete"])
def test_a_writes_where_takes_every_operator_a_read_does(built, verb, where, condition, params):
    """Each operator the where input offers is the condition written; none is dropped, so a
    write never reaches rows its where did not select."""
    set_arg = 'set: {givenName: "Anita"}, ' if verb == "update" else ""
    m = _compiled(
        built, f"mutation {{ {verb}Customers({set_arg}where: {where}) {{ affected_rows }} }}"
    )
    placeholders = condition.replace("%s", "{}").format(
        *[f"${i}" for i in range(2 if verb == "update" else 1, len(params) + 3)]
    )
    assert f"WHERE {placeholders} RETURNING" in m.sql, m.sql
    assert m.params == (["Anita"] if verb == "update" else []) + params


@pytest.mark.parametrize(
    "sql",
    [
        'INSERT INTO "store"."customers" ("givenName", total_cents) VALUES (\'Ana\', 5)',
        'UPDATE "store"."customers" SET "givenName" = \'Ana\' WHERE "givenName" = \'A\'',
        'DELETE FROM "store"."customers" WHERE "givenName" = \'Ana\'',
        'INSERT INTO "store"."customers" (id, "givenName") VALUES (1, \'Ana\') '
        'ON CONFLICT (id) DO UPDATE SET "givenName" = EXCLUDED."givenName"',
    ],
)
def test_a_sql_write_naming_published_columns_is_lowered_to_the_physical_ones(built, sql):
    """SQL speaks its own published names (here the alias; the snake convention keeps
    total_cents): a write naming a column by its published name writes the physical column, as a
    read does."""
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical

    _, ctx = built
    lowered = lower_to_catalog_physical(sql, ctx)
    assert '"first_name"' in lowered and "givenName" not in lowered, lowered


def _writer(writable):
    from tests.write_governance import write_governance

    gov = write_governance(
        {"public.customers": (1, ["id", "first_name", "total_cents"])}, writable={1: writable}
    )
    gov.published = {1: {"givenname": "first_name"}}
    return gov


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO public.customers (id, \"givenName\") VALUES (1, 'Ana')",
        "UPDATE public.customers SET \"givenName\" = 'Ana' WHERE id = 1",
    ],
)
def test_a_write_naming_a_published_column_is_admitted_by_its_physical_grant(sql):
    from tests.write_governance import admitted

    governed = admitted(sql, _writer(["id", "first_name"]))
    assert '"first_name"' in governed and "givenName" not in governed, governed


def test_a_published_name_does_not_widen_the_grant():
    from provisa.compiler.write_admission import ColumnNotWritable
    from tests.write_governance import admitted

    with pytest.raises(ColumnNotWritable, match="first_name"):
        admitted("UPDATE public.customers SET \"givenName\" = 'Ana' WHERE id = 1", _writer(["id"]))


def test_a_returning_column_keeps_its_published_name():
    from tests.write_governance import admitted

    governed = admitted(
        'UPDATE public.customers SET "givenName" = \'Ana\' WHERE id = 1 RETURNING "givenName"',
        _writer(["id", "first_name"]),
    )
    assert 'RETURNING "first_name" AS "givenName"' in governed, governed


def test_a_cypher_create_names_the_columns_published_name_and_writes_the_physical_column(built):
    """Cypher names a column by its property -- the same key its MATCH reads -- and the write it
    translates to names the published column, admitted and lowered to the physical one."""
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write
    from tests.write_governance import admitted

    _, ctx = built
    label_map = CypherLabelMap.from_schema(ctx, ["*"])
    (node,) = label_map.nodes.values()
    # The column's published name is its property, in MATCH and CREATE alike.
    assert node.properties["givenName"] == "givenName"
    assert "firstName" not in node.properties
    assert node.property_of("first_name") == "givenName"
    sql = WriteTranslator(label_map).translate(
        parse_cypher_write("CREATE (c:Customers {id: 1, givenName: 'Ana'})")
    )
    gov = _writer(["id", "first_name"])
    gov.table_map = {"store.customers": 1, "public.customers": 1, "customers": 1}
    assert '"first_name"' in admitted(sql, gov)
    assert '"first_name"' in lower_to_catalog_physical(sql, ctx)


def test_a_cypher_match_returns_a_column_under_its_published_name(built):
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql

    _, ctx = built
    label_map = CypherLabelMap.from_schema(ctx, ["*"])
    select, _, _ = cypher_to_sql(
        parse_cypher("MATCH (c:Customers) WHERE c.givenName = 'Ana' RETURN c.givenName"),
        label_map,
        {},
    )
    sql = select.sql(dialect="postgres")
    assert "givenName" in sql and "firstName" not in sql, sql
