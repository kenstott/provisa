# Copyright (c) 2026 Kenneth Stott
# Canary: 7b4e2c90-5a18-4d36-8f72-1c9d0e6a3b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Nothing is defined through a query protocol.

A statement that creates, alters or drops a relation is refused with one message, decided in
one place — the semantic layer of the one pipeline, where a statement has been parsed and is
not yet governed or routed. Data writes are not definitions and pass. A surface that must tell
a text's language before it can parse it (Cypher, Flight) recognises a definition from its
opening words and raises the same refusal."""

from __future__ import annotations

import inspect

import pytest
import sqlglot

from provisa.compiler.definitions import (
    DefinitionNotAvailable,
    definition_kind,
    definition_refusal,
    refuse_definition,
    refuse_definition_text,
)

_SQL_DEFINITIONS = [
    ("CREATE TABLE t AS SELECT 1", "CREATE TABLE"),
    ("CREATE TABLE t (id INT)", "CREATE TABLE"),
    ("CREATE TEMP TABLE t (id INT)", "CREATE TABLE"),
    ("CREATE VIEW v AS SELECT 1", "CREATE VIEW"),
    ("CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE VIEW"),
    ("CREATE MATERIALIZED VIEW m AS SELECT 1", "CREATE VIEW"),
    ("CREATE INDEX i ON t (a)", "CREATE INDEX"),
    ("CREATE UNIQUE INDEX i ON t (a)", "CREATE INDEX"),
    ("CREATE SCHEMA s", "CREATE SCHEMA"),
    ("CREATE SEQUENCE q", "CREATE SEQUENCE"),
    ("ALTER TABLE t ADD COLUMN a INT", "ALTER TABLE"),
    ("ALTER VIEW v RENAME TO w", "ALTER VIEW"),
    ("ALTER SEQUENCE q RESTART", "ALTER SEQUENCE"),  # kept by the parser as a raw command
    ("DROP TABLE t", "DROP TABLE"),
    ("DROP VIEW v", "DROP VIEW"),
    ("DROP INDEX i", "DROP INDEX"),
    ("DROP SCHEMA s", "DROP SCHEMA"),
]
_SQL_DATA = [
    "SELECT 1",
    "INSERT INTO t VALUES (1)",
    "UPDATE t SET a = 1",
    "DELETE FROM t",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1",
    "TRUNCATE TABLE t",
]
_HOW = (
    "is not available here: create it in the model (admin pages, admin API or config), or "
    "create it in the data source and admit it into the model."
)


@pytest.mark.parametrize("sql,kind", _SQL_DEFINITIONS)
def test_a_parsed_definition_is_refused_with_the_one_message(sql, kind):
    tree = sqlglot.parse_one(sql, read="postgres")
    assert definition_kind(tree) == kind
    with pytest.raises(DefinitionNotAvailable) as raised:
        refuse_definition(tree)
    assert str(raised.value) == f"{kind} {_HOW}"
    assert raised.value.kind == kind


@pytest.mark.parametrize("sql", _SQL_DATA)
def test_a_read_or_a_data_write_is_not_a_definition(sql):
    tree = sqlglot.parse_one(sql, read="postgres")
    assert definition_kind(tree) is None
    refuse_definition(tree)  # does not raise
    refuse_definition_text(sql)  # nor from its text


@pytest.mark.parametrize("sql,kind", [d for d in _SQL_DEFINITIONS if d[1] != "ALTER SEQUENCE"])
def test_the_opening_words_of_a_sql_definition_say_the_same(sql, kind):
    """Flight tells a ticket's language from its text before anything parses it: a definition
    is refused there with the same kind the parsed tree gives."""
    with pytest.raises(DefinitionNotAvailable) as raised:
        refuse_definition_text(f"-- nightly\n  {sql}")
    assert raised.value.kind == kind


@pytest.mark.parametrize(
    "cypher,kind",
    [
        ("CREATE INDEX person_name FOR (n:Person) ON (n.name)", "CREATE INDEX"),
        ("CREATE RANGE INDEX i FOR (n:Person) ON (n.age)", "CREATE INDEX"),
        ("create constraint c for (n:Person) require n.id is unique", "CREATE CONSTRAINT"),
        ("DROP INDEX person_name", "DROP INDEX"),
        ("DROP CONSTRAINT c", "DROP CONSTRAINT"),
        ("// structure\nCREATE OR REPLACE DATABASE x", "CREATE DATABASE"),
    ],
)
def test_a_cypher_structure_statement_is_refused_before_it_is_lowered(cypher, kind):
    """An index or a constraint lowers to no SQL, so it never reaches the pipeline's guard: the
    Cypher front end raises the same refusal, on the read parser and on the write parser."""
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.write_translator import parse_cypher_write

    for parse in (parse_cypher, parse_cypher_write):
        with pytest.raises(DefinitionNotAvailable) as raised:
            parse(cypher)
        assert str(raised.value) == f"{kind} {_HOW}"


@pytest.mark.parametrize(
    "cypher",
    [
        "CREATE (n:Person {name: 'a'})",
        "CREATE(n:Person {name: 'a'})",
        "CREATE p = (a:Person)-[:KNOWS]->(b:Person)",  # a path variable, not an object kind
        "MATCH (n:Person) SET n.name = 'b'",
        "MATCH (n:Person) DELETE n",
        "MATCH (n:Index) RETURN n",
    ],
)
def test_a_cypher_data_write_is_not_taken_for_a_definition(cypher):
    """Told apart by what follows the verb: a data CREATE is followed by a pattern, ``(``, or by
    a path variable being assigned; a definition by a word naming what is defined."""
    refuse_definition_text(cypher)


def test_the_refusal_is_found_under_whatever_wrapped_it():
    try:
        try:
            raise DefinitionNotAvailable("DROP TABLE")
        except DefinitionNotAvailable as inner:
            raise RuntimeError(str(inner)) from inner
    except RuntimeError as wrapped:
        found = definition_refusal(wrapped)
    assert found is not None and found.kind == "DROP TABLE"
    assert definition_refusal(ValueError("SQL parse error")) is None
    assert definition_refusal(None) is None


def test_the_guard_sits_at_both_parse_points_of_the_one_pipeline_before_governance():
    """Raw SQL is parsed in ``prepare_front_end``; compiled statements (Flight, gRPC, Cypher,
    GraphQL, REST) in ``_govern_compiled``. Each refuses right after its parse, before any
    governance or routing is built."""
    from provisa.compiler import prepared
    from provisa.pgwire import _pipeline

    front = inspect.getsource(prepared.prepare_front_end)
    assert front.index("sqlglot.parse_one(raw_sql") < front.index("refuse_definition(parsed_input)")
    assert front.index("refuse_definition(parsed_input)") < front.index("_shape_digest_from_tree")

    compiled = inspect.getsource(_pipeline._govern_compiled)
    assert compiled.index("parse_one(sql") < compiled.index("refuse_definition(_compiled_tree)")
    assert compiled.index("refuse_definition(_compiled_tree)") < compiled.index(
        "_reject_view_writes(_compiled_tree, state)"
    )
    assert compiled.index("refuse_definition(_compiled_tree)") < compiled.index("gov_ctx = ")

    governed = inspect.getsource(_pipeline.govern_statement)
    assert "except DefinitionNotAvailable:" in governed  # not reported as a parse error


def test_the_refusal_takes_the_statement_not_the_role():
    from provisa.compiler import definitions

    source = inspect.getsource(definitions)
    assert "capabilit" not in source and "role_id" not in source


def test_airport_definition_actions_answer_the_same_refusal():
    from provisa.api.airport import server

    assert server._DEFINITION_ACTIONS["create_table"] == "CREATE TABLE"
    assert server._DEFINITION_ACTIONS["create_schema"] == "CREATE SCHEMA"
    assert server._DEFINITION_ACTIONS["drop_schema"] == "DROP SCHEMA"
    assert server._DEFINITION_ACTIONS["drop_table"] == "DROP TABLE"
    source = inspect.getsource(server)
    assert "DefinitionNotAvailable(definition)" in source
    assert "_do_create_table" not in source and "_do_create_schema" not in source
