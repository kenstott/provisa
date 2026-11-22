# Copyright (c) 2026 Kenneth Stott
# Canary: 223f5226-7675-4e26-bb74-28b81bfa2d44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The first Cypher translation in a process does the work a later one does: it imports only the
modules its own statement needs and parses no more than a later translation parses. Measured by
counting, in a fresh interpreter, never by seconds. And the keyword table the translator quotes by
is the one the pinned sqlglot reads (REQ-347)."""

# Requirements: REQ-347

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.errors import SqlglotError

from provisa.gql.translator import SQL_KEYWORDS

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]

#: Run in a fresh interpreter: import the translator and the transpiler (a server's start), then
#: translate and transpile two statements, counting the modules each imports and the statements
#: sqlglot parses for each.
_PROBE = """
import json, sys
import sqlglot.parser
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb

parses = [0]
parse = sqlglot.parser.Parser.parse


def counted(self, *args, **kwargs):
    parses[0] += 1
    return parse(self, *args, **kwargs)


sqlglot.parser.Parser.parse = counted
empty = CypherLabelMap(nodes={}, relationships={})


def translated(query):
    before, parses[0] = set(sys.modules), 0
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), empty, {})
    transpile_to_duckdb(sql_ast.sql(dialect="postgres"))
    return {"modules": sorted(set(sys.modules) - before), "parses": parses[0]}


first = translated("WITH 1 AS like RETURN like + 1 AS x")
later = translated("WITH 2 AS like RETURN like + 2 AS y")
print(json.dumps({"first": first, "later": later}))
"""


@pytest.fixture(scope="module")
def counts() -> dict:
    ran = subprocess.run(
        [sys.executable, "-c", _PROBE], cwd=_REPO, capture_output=True, text=True, check=True
    )
    return json.loads(ran.stdout.strip().splitlines()[-1])


def test_the_first_translation_parses_no_more_than_a_later_one(counts):
    # main at 0a5f889512 parsed 525 statements in the first: a probe of every Postgres keyword.
    assert counts["first"]["parses"] == counts["later"]["parses"]


def test_the_first_translation_imports_only_what_its_statement_needs(counts):
    imported = counts["first"]["modules"]
    # The settings catalog (and pydantic under it) for a span that records nothing, and DuckDB for
    # a rewrite of sqlglot trees, were imported here: 113 modules at 0a5f889512.
    for unneeded in ("provisa.core.settings_catalog", "pydantic", "duckdb"):
        assert unneeded not in imported
    assert len(imported) <= 15, imported
    assert counts["later"]["modules"] == []


def _probed_keywords() -> frozenset[str]:
    """The Postgres tokenizer's keywords sqlglot reads back as other than a column when its own
    SQL text writes them bare."""
    found: set[str] = set()
    for keyword in Postgres.Tokenizer.KEYWORDS:
        word = keyword.lower()
        if not word.isidentifier() or word in found:
            continue
        for probe in (f"SELECT {word} FROM t", f"SELECT f({word}, 1), {word}.x FROM t AS {word}"):
            try:
                tree = sqlglot.parse_one(probe, read="postgres")
            except SqlglotError:
                found.add(word)
                break
            names = {n for c in tree.find_all(exp.Column) for n in (c.name, c.table)}
            if word not in {n.lower() for n in names}:
                found.add(word)
                break
    return frozenset(found)


#: Keywords the pinned sqlglot reads back as a column but writes bare, which the engines reject
#: (IN is reserved in Postgres and DuckDB): the table quotes them too.
_WRITTEN_BARE_RESERVED = frozenset({"in"})


def test_the_keyword_table_is_the_pinned_sqlglots():
    assert SQL_KEYWORDS == _probed_keywords() | _WRITTEN_BARE_RESERVED


def test_a_reserved_word_sqlglot_writes_bare_is_still_bare():
    """Each extra word of the table is one sqlglot still writes bare; when it quotes it, drop it."""
    for word in _WRITTEN_BARE_RESERVED:
        assert exp.column("x", table=word).sql(dialect="postgres") == f"{word}.x"
