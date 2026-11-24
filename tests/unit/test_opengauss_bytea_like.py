# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7f9c3a-8b41-4e65-a0d3-6f1b5e8c9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""bytea LIKE is written as rawlike / rawnlike for the openGauss engine kind only (REQ-2236,
amended 2026-11-24); every other engine's SQL is unchanged."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest

from provisa.federation import engine as engine_mod
from provisa.federation.engine import build_engine
from provisa.transpiler.transpile import transpile

_LIT = r"'\xdeadbeef'::bytea"
_RAW = r"HEXTORAW(ENCODE(CAST('\xdeadbeef' AS BYTEA), 'hex'))"

# Each bytea LIKE form, and the openGauss function it is written as.
_FORMS = [
    (f'SELECT pg_catalog."like"({_LIT}, {_LIT}) AS v', "RAWLIKE"),
    (f'SELECT pg_catalog."notlike"({_LIT}, {_LIT}) AS v', "RAWNLIKE"),
    (f'SELECT pg_catalog."bytealike"({_LIT}, {_LIT}) AS v', "RAWLIKE"),
    (f'SELECT pg_catalog."byteanlike"({_LIT}, {_LIT}) AS v', "RAWNLIKE"),
    (f"SELECT {_LIT} ~~ {_LIT} AS v", "RAWLIKE"),
    (f"SELECT {_LIT} !~~ {_LIT} AS v", "RAWNLIKE"),
    (f"SELECT {_LIT} LIKE {_LIT} AS v", "RAWLIKE"),
    (f"SELECT {_LIT} NOT LIKE {_LIT} AS v", "RAWNLIKE"),
]


@pytest.mark.parametrize(("statement", "function"), _FORMS)
def test_bytea_like_is_rawlike_on_opengauss(statement: str, function: str) -> None:
    written = transpile(statement, "postgres", engine_kind="opengauss")
    assert written == f"SELECT {function}({_RAW}, {_RAW}) AS v"


@pytest.mark.parametrize("statement", [s for s, _ in _FORMS])
@pytest.mark.parametrize("kind", [None, "postgres", "greenplum", "yugabytedb", "cockroachdb"])
def test_bytea_like_unchanged_off_opengauss(statement: str, kind: str | None) -> None:
    written = transpile(statement, "postgres", engine_kind=kind)
    assert written == transpile(statement, "postgres")
    assert "raw" not in written.lower()
    assert "encode" not in written.lower()


def test_bytea_like_in_a_predicate_on_opengauss() -> None:
    written = transpile(
        "SELECT id FROM t WHERE CAST(b AS bytea) LIKE '\\x25'::bytea",
        "postgres",
        engine_kind="opengauss",
    )
    assert written == (
        "SELECT id FROM t WHERE RAWLIKE(HEXTORAW(ENCODE(CAST(b AS BYTEA), 'hex')), "
        "HEXTORAW(ENCODE(CAST('\\x25' AS BYTEA), 'hex')))"
    )


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT pg_catalog.\"like\"('abc'::text, 'a%'::text) AS v",
        "SELECT pg_catalog.\"notlike\"('abc'::text, 'a%'::text) AS v",
        "SELECT 'abc' LIKE 'a%' AS v",
        "SELECT 'abc' NOT LIKE 'a%' AS v",
        "SELECT 'abc' ~~ 'a%' AS v",
        "SELECT name FROM t WHERE name LIKE 'a%'",
    ],
)
def test_text_like_unchanged_on_opengauss(statement: str) -> None:
    assert transpile(statement, "postgres", engine_kind="opengauss") == transpile(
        statement, "postgres"
    )


def test_opengauss_engine_passes_its_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    """The openGauss engine's backend transpiles with its kind; a postgres-dialect sibling does not
    get the rewrite."""
    monkeypatch.setattr(engine_mod, "require_engine_extra", lambda kind: None)
    statement = f'SELECT pg_catalog."bytealike"({_LIT}, {_LIT}) AS v'
    monkeypatch.setenv("PROVISA_ENGINE_URL", "opengauss://u:p@h:5432/db")
    opengauss = build_engine("opengauss")
    monkeypatch.setenv("PROVISA_ENGINE_URL", "postgresql+psycopg2://u:p@h:5432/db")
    greenplum = build_engine("greenplum")
    assert opengauss.dialect == greenplum.dialect == "postgres"
    assert opengauss.backend.transpile_physical(statement) == f"SELECT RAWLIKE({_RAW}, {_RAW}) AS v"
    assert greenplum.backend.transpile_physical(statement) == transpile(statement, "postgres")
