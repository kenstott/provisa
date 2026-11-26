# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5f2a-9c41-4d07-a6e3-7f1d2c9b0e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JSON column's digest text is identical on every engine (REQ-2279): the value is digested as
its canonical text (digest.canonical_json) -- minified, keys sorted by code point, numbers in plain
digits -- by provisa_json_text, which DuckDB, PostgreSQL and the Trino plugin each hold, so stored
JSON differing only in spacing, key order or number spelling gives one fake."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
import sqlglot.expressions as exp

from provisa.fakes import digest as digest_mod
from provisa.fakes.digest import canonical_json
from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import parse
from provisa.fakes.projection import faked_projection
from provisa.fakes.read_sql import Column
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_KEY = b"j" * 32
CONTACT = '{"email": email(), "phone": phone_number()}'

#: The vectors the Trino plugin's JsonTextTest checks JsonText.canonical against.
_VECTORS = (
    Path(__file__).resolve().parents[2] / "trino-functions/src/test/resources/json_canonical.json"
)

#: One value, stored three ways: minified and sorted, spaced, and reordered with numbers spelled
#: apart.
_MINIFIED = '{"age":30,"email":"ann@example.com","tags":["a","b"]}'
_SPACED = '{ "age" : 30 ,\n  "email" : "ann@example.com", "tags" : [ "a", "b" ] }'
_REORDERED = '{"tags": ["a", "b"], "email": "ann@example.com", "age": 3.0e1}'


@pytest.mark.parametrize(
    ("given", "canonical"), [tuple(pair) for pair in json.loads(_VECTORS.read_text())]
)
def test_canonical_text_is_the_definition_the_trino_plugin_shares(given, canonical):
    assert canonical_json(given) == canonical


def test_spaced_or_reordered_json_has_one_canonical_text():
    assert canonical_json(_SPACED) == canonical_json(_REORDERED) == _MINIFIED


@pytest.mark.parametrize("given", ['{"a": NaN}', '{"a": Infinity}', "1 2", "{", "not json"])
def test_text_that_is_not_json_is_refused(given):
    with pytest.raises(ValueError):
        canonical_json(given)


def test_numbers_keep_every_digit():
    assert canonical_json("123456789012345678901234567890.50") == "123456789012345678901234567890.5"
    assert canonical_json("[-0.0, 1E-7, 2.50e3]") == "[0,0.0000001,2500]"


def _governed(declaration: str = CONTACT, stable: bool = True) -> str:
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


def test_each_engine_s_form_digests_the_canonical_text():
    governed = _governed()
    trino = transpile_to_trino(governed, generated=True)
    assert "PROVISA_DIGEST('" in trino and 'PROVISA_JSON_TEXT("t"."contact")' in trino
    assert "JSON_FORMAT" not in trino
    duck = transpile_to_duckdb(governed, generated=True)
    assert 'PROVISA_JSON_TEXT("t"."contact")' in duck
    pg = transpile(governed, "postgres", generated=True)
    assert 'PROVISA_JSON_TEXT("t"."contact")' in pg and "JSON_FORMAT" not in pg


@pytest.fixture
def engine():
    previous = digest_mod._key
    digest_mod.set_key(_KEY)
    con = duckdb.connect()
    register(con)
    rows = ", ".join(
        f"({i}, '{text}'::JSON)" for i, text in enumerate((_MINIFIED, _SPACED, _REORDERED), 1)
    )
    con.execute(
        f"CREATE TABLE people AS SELECT * FROM (VALUES {rows}, (4, NULL::JSON)) t(id, contact)"
    )
    yield con
    con.close()
    digest_mod._key = previous


def test_duckdb_digests_spaced_and_reordered_json_as_the_minified_text(engine):
    fp = digest_mod.fingerprint(_KEY)
    got = engine.execute(
        f"SELECT id, provisa_digest('{fp}', provisa_json_text(contact)) FROM people ORDER BY id"
    ).fetchall()
    want = digest_mod.digest(_KEY, _MINIFIED)
    assert got == [(1, want), (2, want), (3, want), (4, None)]


@pytest.mark.parametrize("stable", [True, False])
def test_duckdb_fakes_spaced_and_reordered_json_as_one_object(engine, stable):
    sql = transpile_to_duckdb(_governed(stable=stable), generated=True)
    rows = engine.execute(sql).fetchall()
    objects = [None if v is None else json.loads(v) for _, v in rows]
    assert objects[0] is not None and objects[0] == objects[1] == objects[2]
    assert objects[3] is None
