# Copyright (c) 2026 Kenneth Stott
# Canary: 2d72e4da-408d-4aef-af31-339243502f6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic SQL's dialect: PostgreSQL text that keeps what the read means (REQ-778).

A statement is held as PostgreSQL-dialect text and read back and written again at every rewrite
(governance, RLS, routing). sqlglot writes a null-on-failure cast (``TryCast``) in that dialect as
a plain CAST, PostgreSQL having none, and the cast that answers null for a value that does not
convert (Cypher's toInteger, toFloat, toBoolean) then fails the statement instead. So the text
keeps TRY_CAST, which sqlglot reads back as the same cast, and each target writes its own: TRY_CAST,
SAFE_CAST, or for a PostgreSQL target ``pg_input_is_valid`` (``transpile.postgres_try_casts``).

A JSON value read whole (the root path, ``$``: Cypher's property of an unwound map) is written
``JSON_EXTRACT_PATH_TEXT(v, VARIADIC '{}')``, PostgreSQL's spelling, which sqlglot reads back as a
VARIADIC argument rather than the root path; every other target then wrote ``v ->> VARIADIC '{}'``.
So that spelling reads back as the root path again, and each target writes its own.

A filtered list (Cypher's ``[y IN l WHERE p]``) is kept ``FILTER(l, y -> p)``: PostgreSQL's
``ARRAY(SELECT y FROM UNNEST(l) ...)`` reads back as an array of a subquery, which no other target
writes. A PostgreSQL target writes its own (``transpile.postgres_array_filters``)."""

# Requirements: REQ-778

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator

Postgres.Generator.TRANSFORMS[exp.TryCast] = Generator.trycast_sql


def _root_read_back(name: str) -> None:
    build = Postgres.Parser.FUNCTIONS[name]

    def _build(args: list) -> exp.Expr:
        whole = (
            len(args) == 2
            and isinstance(args[1], exp.Variadic)
            and isinstance(args[1].this, exp.Literal)
            and args[1].this.name == "{}"
        )
        return build(args[:1] if whole else args)

    Postgres.Parser.FUNCTIONS[name] = _build


_root_read_back("JSON_EXTRACT_PATH")
_root_read_back("JSON_EXTRACT_PATH_TEXT")

Postgres.Generator.TRANSFORMS[exp.ArrayFilter] = lambda self, e: self.func(
    "FILTER", e.this, e.expression
)


class ContainedByOrEqual(exp.Expression, exp.Binary, exp.Predicate):  # REQ-2171
    """PostgreSQL's ``a <<= b``: an address within or equal to a network."""


class ContainsOrEqual(exp.Expression, exp.Binary, exp.Predicate):  # REQ-2171
    """PostgreSQL's ``a >>= b``: a network containing or equal to an address."""


def _reads_containment(parser: type) -> None:
    """REQ-2171: ``<<=`` and ``>>=`` read as their own operators (they tokenize as ``<`` ``<=``
    and ``>`` ``>=``, which sqlglot refused). ``<<`` and ``>>`` read as sqlglot reads them."""
    from sqlglot.tokens import TokenType

    parse_bitwise = parser._parse_bitwise

    def _parse_bitwise_with_containment(self) -> exp.Expr | None:
        this = parse_bitwise(self)
        while True:
            if self._match_pair(TokenType.LT, TokenType.LTE):
                this = self.expression(
                    ContainedByOrEqual(this=this, expression=parse_bitwise(self))
                )
            elif self._match_pair(TokenType.GT, TokenType.GTE):
                this = self.expression(ContainsOrEqual(this=this, expression=parse_bitwise(self)))
            else:
                return this

    parser._parse_bitwise = _parse_bitwise_with_containment


_reads_containment(Postgres.Parser)
Postgres.Generator.TRANSFORMS[ContainedByOrEqual] = lambda self, e: self.binary(e, "<<=")
Postgres.Generator.TRANSFORMS[ContainsOrEqual] = lambda self, e: self.binary(e, ">>=")

# DuckDB's inet extension has the same two operators, which its statements are read back with.
from sqlglot.dialects.duckdb import DuckDB  # noqa: E402

_reads_containment(DuckDB.Parser)
DuckDB.Generator.TRANSFORMS[ContainedByOrEqual] = lambda self, e: self.binary(e, "<<=")
DuckDB.Generator.TRANSFORMS[ContainsOrEqual] = lambda self, e: self.binary(e, ">>=")
