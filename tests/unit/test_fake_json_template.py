# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f0b9a-2e4c-4a73-b85e-1c9f7a3d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JSON column's template fake (REQ-2279): the declaration read and checked field by field, the
object built from the fields' fakes as one JSON text on every engine's form, and a template
proposed from the keys and value types the latest profile run kept."""

from __future__ import annotations

import json
import re

import duckdb
import pytest
import sqlglot.expressions as exp

from provisa.fakes import digest as digest_mod
from provisa.fakes.checks import DeclaredColumn, check_table
from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import (
    FakeRefused,
    Method,
    Template,
    TemplateArray,
    TemplateField,
    TemplateLiteral,
    TemplateObject,
    Uniform,
    parse,
)
from provisa.fakes.projection import faked_projection
from provisa.fakes.propose import ProfiledColumnFacts, propose
from provisa.fakes.read_sql import Column
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_KEY = b"t" * 32
CONTACT = '{"email": email(), "phone": phone_number()}'


def _check(fake: str, data_type: str = "json", stable: bool = False):
    return check_table("people", [DeclaredColumn("contact", data_type, fake, stable)], {})


# -- the declaration -----------------------------------------------------------------------------


def test_a_template_reads_fields_nested_objects_arrays_and_literals():
    t = parse(
        '{"email": email(), "age": uniform(min=18, max=90)::integer, '
        '"address": {"city": city()}, "tags": [word(), word()], "kind": "contact", '
        '"n": 3, "ok": true, "none": null}'
    )
    assert isinstance(t, Template)
    assert t.root == TemplateObject(
        (
            ("email", TemplateField(Method("email"), "varchar")),
            ("age", TemplateField(Uniform(18.0, 90.0), "integer")),
            ("address", TemplateObject((("city", TemplateField(Method("city"), "varchar")),))),
            (
                "tags",
                TemplateArray(
                    (
                        TemplateField(Method("word"), "varchar"),
                        TemplateField(Method("word"), "varchar"),
                    )
                ),
            ),
            ("kind", TemplateLiteral("contact")),
            ("n", TemplateLiteral(3)),
            ("ok", TemplateLiteral(True)),
            ("none", TemplateLiteral(None)),
        )
    )


def test_a_field_without_a_type_takes_the_type_its_fake_makes():
    t = parse(
        '{"a": pyint(), "b": pyfloat(), "c": date_object(), "d": normal(mean=1, sd=1), '
        '"e": poisson(mean=3), "f": bool(0.5), "g": categories((x, y)), "h": hash()}'
    )
    assert isinstance(t, Template)
    types = {k: v.data_type for k, v in t.root.fields if isinstance(v, TemplateField)}
    assert types == {
        "a": "bigint",
        "b": "double",
        "c": "date",
        "d": "double",
        "e": "bigint",
        "f": "boolean",
        "g": "varchar",
        "h": "varchar",
    }


@pytest.mark.parametrize(
    ("declaration", "refusal"),
    [
        ('{"a": email()', "expected '}'"),
        ('{"a": email(), "a": city()}', "names the field 'a' twice"),
        ('{"a": email}', "write a fake as email()"),
        ('{"a": nosuch()}', "field 'a'"),
        ('{"a": email()} x', "text after its closing brace"),
        ("{a: email()}", "double-quoted name"),
    ],
)
def test_a_template_that_cannot_describe_an_object_is_refused_by_name(declaration, refusal):
    with pytest.raises(FakeRefused, match=re.escape(refusal)):
        _check(declaration)


# -- the checks ----------------------------------------------------------------------------------


def test_a_template_fakes_a_json_column_only():
    _check(CONTACT, "jsonb")
    with pytest.raises(FakeRefused, match="people.contact: a JSON template fakes a json column"):
        _check(CONTACT, "varchar")


@pytest.mark.parametrize(
    ("declaration", "refusal"),
    [
        # Checked against the field's type, as a column's fake is against the column's.
        ('{"a": email()::integer}', "field 'a': email() makes text; a whole-number column takes"),
        ('{"a": poisson(mean=2)::varchar}', "field 'a': poisson() fakes a integer column"),
        (
            '{"a": {"b": [pyint(), email()::boolean]}}',
            "field 'a.b[1]': email() makes text; a true/false column takes",
        ),
        ('{"a": email()::json}', "field 'a': a field is written as one of"),
        # A field computes from the column's value and its own declaration alone.
        ('{"a": categories()}', "field 'a': categories() cannot fake a field"),
        ('{"a": bool()}', "field 'a': bool() cannot fake a field"),
        ('{"a": profile()}', "field 'a': profile() cannot fake a field"),
        ('{"a": pattern()}', "field 'a': pattern() cannot fake a field"),
        ('{"a": prefix(3)}', "field 'a': prefix() cannot fake a field"),
        ('{"a": after(created)}', "field 'a': after() cannot fake a field"),
        ('{"a": sql(1)}', "field 'a': sql() cannot fake a field"),
        # Structured Faker methods stay refused (REQ-2279).
        ('{"a": pydict()}', "field 'a': pydict() makes values no column can hold"),
        ('{"a": profile()}', "cannot fake a field"),
        ('{"a": simple_profile()}', "field 'a': simple_profile() makes values no column"),
    ],
)
def test_each_field_is_checked_as_a_column_s_fake_would_be(declaration, refusal):
    with pytest.raises(FakeRefused, match=re.escape(refusal)):
        _check(declaration)


def test_a_stable_template_s_fields_are_each_stable():
    _check(CONTACT, stable=True)
    _check('{"tier": categories((a, b), (0.5, 0.5)), "kind": "x"}', stable=True)
    with pytest.raises(FakeRefused, match=r"field 'b': date_object\(\) cannot be stable"):
        _check('{"a": email(), "b": date_object()}', stable=True)


# -- the build on each engine's form ------------------------------------------------------------


@pytest.fixture
def engine():
    previous = digest_mod._key
    digest_mod.set_key(_KEY)
    con = duckdb.connect()
    register(con)
    con.execute(
        "CREATE TABLE people AS SELECT * FROM (VALUES "
        """(1, '{"id":1}'::JSON), (2, '{"id":2}'::JSON), (3, '{"id":1}'::JSON), (4, NULL::JSON)"""
        ") t(id, contact)"
    )
    yield con
    con.close()
    digest_mod._key = previous


def _governed(declaration: str, stable: bool = False) -> str:
    """The faked read of people.contact under ``declaration``, as the governed statement says it."""
    col = Column(
        "contact", "json", "other", parse(declaration), stable, None, 1 if stable else None
    )
    projection = faked_projection(
        exp.table_("people"),
        "t",
        [("id", "integer", "integer"), ("contact", "json", "other")],
        {"contact": col},
        digest_mod.fingerprint(_KEY),
        None,
    )
    return exp.select("*").from_(projection.subquery("p")).order_by("id").sql(dialect="postgres")


def _rows(con, declaration: str, stable: bool = False) -> list[tuple[int, object]]:
    sql = transpile_to_duckdb(_governed(declaration, stable), generated=True)
    return [(i, None if v is None else json.loads(v)) for i, v in con.execute(sql).fetchall()]


def test_duckdb_builds_the_object_from_its_fields(engine):
    rows = _rows(
        engine,
        '{"email": email(), "phone": phone_number(), "age": uniform(min=18, max=90)::integer, '
        '"address": {"city": city()}, "tags": [word(), word()], "kind": "contact", '
        '"vip": bool(0.5), "score": normal(mean=1, sd=0.5), "joined": date_object(), '
        '"seen": date_time(), "q": "say \\"hi\\"\\n"}',
    )
    (one, a), (two, b), (three, c), (four, d) = rows
    assert d is None  # NULL stays NULL
    assert a == c and a != b  # one value, one object; another value, another
    assert isinstance(a, dict)
    assert list(a) == [
        "email",
        "phone",
        "age",
        "address",
        "tags",
        "kind",
        "vip",
        "score",
        "joined",
        "seen",
        "q",
    ]
    assert re.fullmatch(r"[^@\s]+@[^@\s]+", a["email"])
    assert re.search(r"\d", a["phone"])
    assert isinstance(a["age"], int) and 18 <= a["age"] <= 90
    assert isinstance(a["address"]["city"], str) and a["address"]["city"]
    assert len(a["tags"]) == 2 and all(isinstance(w, str) for w in a["tags"])
    assert a["kind"] == "contact" and a["q"] == 'say "hi"\n'
    assert isinstance(a["vip"], bool) and isinstance(a["score"], float)
    assert re.fullmatch(r"\d{4}-\d\d-\d\d", a["joined"])
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", a["seen"])


def test_two_fields_of_one_fake_are_seeded_apart(engine):
    rows = _rows(engine, '{"a": [word(), word(), word(), word()], "b": email(), "c": email()}')
    objects = [o for _, o in rows if o is not None]
    assert any(len(set(o["a"])) > 1 for o in objects)
    assert all(o["b"] != o["c"] for o in objects)


def test_a_stable_field_is_the_portable_definition(engine):
    """The stable form names provisa_stable_fake for each field, which Trino computes alike."""
    governed = _governed(CONTACT, stable=True)
    assert governed.count("PROVISA_STABLE_FAKE") == 2 and "PROVISA_FAKE_METHOD" not in governed
    rows = _rows(engine, CONTACT, stable=True)
    assert rows[0][1] == rows[2][1] and rows[0][1] != rows[1][1]


def test_each_engine_s_form_reads_the_text_as_json_and_digests_json_as_text():
    governed = _governed(CONTACT, stable=True)
    trino = transpile_to_trino(governed, generated=True)
    # Read as JSON by json_parse (a cast of text to JSON is a JSON string there); digested as
    # its canonical text (REQ-2279), with no cast to JSON in the statement.
    assert "JSON_PARSE((" in trino
    assert 'PROVISA_JSON_TEXT("t"."contact")' in trino and "AS JSON)" not in trino
    duck = transpile_to_duckdb(governed, generated=True)
    assert "ELSE CAST((" in duck and "JSON_PARSE" not in duck
    pg = transpile(governed, "postgres", generated=True)
    assert "CAST((" in pg and "AS JSON)" in pg and 'PROVISA_JSON_TEXT("t"."contact")' in pg


def test_the_pg_engine_holds_provisa_json_text_for_the_digest():
    from provisa.fakes.pg_functions import FUNCTIONS_SQL

    for arg in ("text", "json", "jsonb"):
        assert f"FUNCTION provisa_json_text(value {arg}) RETURNS text" in FUNCTIONS_SQL
    assert "json_format" not in FUNCTIONS_SQL


# -- proposals -----------------------------------------------------------------------------------


def _facts(name: str, data_type: str, values: tuple[str, ...]) -> ProfiledColumnFacts:
    return ProfiledColumnFacts(name, "other", None, "unknown", 0.0, (), data_type, values)


def test_a_json_column_is_proposed_a_template_from_the_keys_and_types_the_run_kept():
    kept = (
        '{"email": "a@x.io", "phone": "555-0101", "age": 31, "vip": true, '
        '"address": {"city": "Oslo"}, "tags": ["a", "b"], "score": 1.5, "note": null}',
        '{"email": "b@x.io", "phone": "555-0102", "age": 45, "vip": false, '
        '"address": {"city": "Rome"}, "tags": ["c", "d"], "score": 2.5, "nick": "bo"}',
        "not json",
    )
    out = propose("r9", [_facts("contact", "json", kept)], set(), set())
    template = out.columns["contact"]["fake"]
    assert template == (
        '{"email": email(), "phone": phone_number(), "age": uniform(min=31, max=45)::integer, '
        '"vip": bool(0.50), "address": {"city": city()}, "tags": [word(), word()], '
        '"score": uniform(min=1.5, max=2.5), "note": null, "nick": word()}'
    )
    _check(template)  # what is proposed is a declaration the save accepts


def test_a_json_column_whose_kept_values_hold_no_object_is_proposed_nothing_from_them():
    out = propose("r9", [_facts("payload", "json", ("[1, 2]", '"x"'))], set(), set())
    assert "payload" not in out.columns
