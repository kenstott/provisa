# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e4f71-3c6a-4d58-8e0f-2a7c5d1b6e93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Db2 and SAP HANA refusals and limitations are rows of the polyfill grid (REQ-2236, THE
REFUSAL LIVES IN THE GRID; amended 2026-11-21 for Db2 and SAP HANA): each dialect raises the row's
refusal, and each engine's page lists them."""

# Requirements: REQ-2236

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import DIALECTS, GRID, Status, docs, exact_engines
from provisa.transpiler.transpile import transpile

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "scripts" / "conformance"))

import polyfill_provenance  # noqa: E402

_PROVENANCE = polyfill_provenance.read(GRID)
_HINT_PROVENANCE = polyfill_provenance.read_hints(GRID)
_DIALECT_SOURCES = {
    "db2": _ROOT / "provisa/db2/dialect.py",
    "saphana": _ROOT / "provisa/saphana/dialect.py",
}
#: A construct a dialect raises by its literal name: refused("...") or a reason _cast_refusal returns.
_LITERAL = re.compile(r'(?:refused\(|return )"([^"]+)"')


def _constructs(dialect: str) -> set[str]:
    """Every construct ``dialect``'s source names a refusal by."""
    source = _DIALECT_SOURCES[dialect].read_text()
    return {name for name in _LITERAL.findall(source) if any(c.islower() for c in name)}


def _row_constructs(dialect: str) -> set[str]:
    return {
        limitation.refusal.message.removesuffix(exact_engines._message("", dialect))
        for feature in exact_engines.FEATURES
        for limitation in feature.read.limitations.get(dialect, ())
        if limitation.refusal is not None
    }


@pytest.mark.parametrize("dialect", ["db2", "saphana"])
def test_db2_and_hana_are_columns_of_the_grid(dialect: str):
    assert dialect in DIALECTS
    assert f"{docs.DIRECTORY}/{dialect}.md" in docs.pages(GRID, _PROVENANCE, _HINT_PROVENANCE)


@pytest.mark.parametrize("dialect", ["db2", "saphana"])
def test_every_refusal_the_dialect_names_is_a_row(dialect: str):
    """No refusal is the dialect's own: each construct it names is a row's, on its column."""
    named = _constructs(dialect)
    assert named, dialect
    assert named <= _row_constructs(dialect), sorted(named - _row_constructs(dialect))


@pytest.mark.parametrize(
    ("dialect", "sql", "message"),
    [
        (
            "db2",
            'SELECT AVG("a") FROM "t"',
            "avg has no form on Db2 that gives PostgreSQL's answer",
        ),
        (
            "db2",
            'SELECT EXTRACT(DOW FROM "d") FROM "t"',
            "extract of dow has no form on Db2 that gives PostgreSQL's answer",
        ),
        (
            "db2",
            'SELECT CAST("a" AS VARCHAR(9000)) FROM "t"',
            "varchar longer than 8168 has no form on Db2 that gives PostgreSQL's answer",
        ),
        (
            "saphana",
            'SELECT "a" / "b" FROM "t"',
            "/ of operands not known to be integers has no form on SAP HANA that gives "
            "PostgreSQL's answer",
        ),
        (
            "saphana",
            'SELECT "a" FROM "t" INTERSECT ALL SELECT "a" FROM "u"',
            "intersect all has no form on SAP HANA that gives PostgreSQL's answer",
        ),
        (
            "saphana",
            'INSERT INTO "t" ("a") VALUES (1) ON CONFLICT DO NOTHING',
            "onconflict has no form on SAP HANA that gives PostgreSQL's answer",
        ),
    ],
)
def test_the_refusal_is_the_rows_sqlstate_and_message(dialect: str, sql: str, message: str):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, dialect, generated=False)
    assert str(refused.value) == message
    assert getattr(refused.value, "sqlstate") == "0A000"  # the row's, set by the dialect
    assert refused.value.params["dialect"] == dialect


@pytest.mark.parametrize(
    ("dialect", "construct", "named", "message"),
    [
        ("db2", "greatest", {}, "greatest has no form on Db2 that gives PostgreSQL's answer"),
        (
            "saphana",
            "the type {type}",
            {"type": "uuid"},
            "the type uuid has no form on SAP HANA that gives PostgreSQL's answer",
        ),
        # A construct no row names is refused by the row of a construct with no exact form.
        ("db2", "xmlelement", {}, "xmlelement has no form on Db2 that gives PostgreSQL's answer"),
    ],
)
def test_refusal_reads_the_row(dialect: str, construct: str, named: dict, message: str):
    refusal = exact_engines.refusal(dialect, construct, **named)
    assert (refusal.sqlstate, refusal.message) == ("0A000", message)


def test_db2_division_is_no_refusal_of_the_grid():
    """Db2's quotient of two integers truncates as PostgreSQL's: only HANA's row refuses /."""
    division = "/ of operands not known to be integers"
    assert not {c for c in _row_constructs("db2") if c.startswith("/")}
    assert division in _row_constructs("saphana")


@pytest.mark.parametrize(
    ("dialect", "phrase"),
    [
        ("db2", "8168"),
        ("db2", "trailing blanks"),
        ("db2", "a cast to an integer type of a value that may have a fraction"),
        ("saphana", "NVARCHAR(5000)"),
        ("saphana", "binary order"),
        ("saphana", "SUM over bigint can overflow"),
        ("saphana", "7 fractional digits"),
        ("saphana", "LENGTH may count"),
        ("saphana", "a cast to an integer type of a value that may have a fraction"),
        ("saphana", "where the compiler supplies the column types"),
    ],
)
def test_each_stated_limitation_is_on_the_engines_page(dialect: str, phrase: str):
    page = docs.pages(GRID, _PROVENANCE, _HINT_PROVENANCE)[f"{docs.DIRECTORY}/{dialect}.md"]
    assert phrase in page.replace("&#x27;", "'")


@pytest.mark.parametrize("feature", exact_engines.FEATURES, ids=lambda f: f.name)
def test_each_row_differs_only_on_db2_and_hana(feature):
    for dialect in DIALECTS:
        status = GRID.status(feature, dialect)
        if dialect in feature.read.limitations:
            assert status is Status.DIFFERS, (feature.name, dialect)
        else:
            assert status is Status.NATIVE, (feature.name, dialect)
