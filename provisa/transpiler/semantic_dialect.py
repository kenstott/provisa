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
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.presto import Presto
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.trino import Trino
from sqlglot.generator import Generator

from provisa.transpiler.polyfill.clickhouse import truncated

Postgres.Generator.TRANSFORMS[exp.TryCast] = Generator.trycast_sql


def _flagged_regex(self: Generator, expression: exp.RegexpILike) -> str:
    """~* where the dialect has no case-insensitive match: sqlglot writes a function none has."""
    flagged = exp.DPipe(this=exp.Literal.string("(?i)"), expression=expression.expression.copy())
    return self.sql(exp.RegexpLike(this=expression.this.copy(), expression=flagged))


for _dialect in (Trino, Presto, Spark, BigQuery):
    _dialect.Generator.TRANSFORMS[exp.RegexpILike] = _flagged_regex
Trino.Generator.TRANSFORMS[exp.IsInf] = rename_func("IS_INFINITE")
Trino.Generator.TRANSFORMS[exp.Log] = lambda self, expression: (
    self.func("LOG", expression.this, expression.expression)
    if expression.expression
    else self.func("LOG", exp.Literal.number(10), expression.this)
)
#: The names that have one spelling on ClickHouse: its other functions take any case.
for _function, _name in (
    (exp.Cbrt, "cbrt"),
    (exp.Sinh, "sinh"),
    (exp.Cosh, "cosh"),
    (exp.Acosh, "acosh"),
    (exp.Asinh, "asinh"),
    (exp.Atanh, "atanh"),
):
    ClickHouse.Generator.TRANSFORMS[_function] = rename_func(_name)


def _split_part(self: Generator, expression: exp.SplitPart) -> str:
    """ClickHouse's parts are an array's elements: past the last one an empty value, as here."""
    value = self.sql(expression.this)
    delimiter = self.sql(expression.args["delimiter"])
    part = self.sql(expression.args["part_index"])
    # An Array cannot be Nullable, so the parts are of the value with its NULL taken out first.
    return (
        f"if(isNull({value}), NULL, "
        f"splitByString(assumeNotNull({delimiter}), assumeNotNull({value}))[{part}])"
    )


ClickHouse.Generator.TRANSFORMS[exp.SplitPart] = _split_part
# Its LIKE has no ESCAPE clause: a backslash escapes there already, and it is the only escape the
# forms write.


def _truncated(self: Generator, expression: exp.TimestampTrunc) -> str:
    """A truncated timestamp, as the polyfill grid's builder writes it (a NULL part, which
    dateTrunc takes for no part at all, is a NULL)."""
    return self.sql(truncated(expression.text("unit"), expression.this))


ClickHouse.Generator.TRANSFORMS[exp.TimestampTrunc] = _truncated
ClickHouse.Generator.TRANSFORMS[exp.Escape] = lambda self, expression: self.sql(expression.this)


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


def _keyword_lambda_sql(
    self: Generator, expression: exp.Lambda, arrow_sep: str = "->", wrap: bool = True
) -> str:
    """DuckDB 2.0 binds a lambda only in its keyword form, ``LAMBDA x, y : body`` (the arrow form
    is a binder error there), so every lambda written for DuckDB takes it, however it was built.
    It is sqlglot's own spelling of a lambda read with the keyword (its ``colon`` form), written
    here without marking the tree."""
    return "LAMBDA " + Generator.lambda_sql(self, expression, arrow_sep=":", wrap=False)


DuckDB.Generator.lambda_sql = _keyword_lambda_sql  # type: ignore[method-assign]


_duckdb_json_arrow = DuckDB.Generator.TRANSFORMS[exp.JSONExtract]


def _json_extract_call(self: Generator, expression: exp.JSONExtract) -> str:
    """DuckDB's JSON extraction. ``v -> p`` still means extraction on 2.0, a step of no stated
    type keeps DuckDB's own operator, and the arrow answers as JSON_EXTRACT(v, p) does. Only where
    it is a call's argument is it written as the function: sqlglot reads ``f(v -> p)`` back as a
    lambda, which DuckDB 2.0 then binds as one (json_array_elements' unnest of
    json_extract(v, '$[*]') was written ``UNNEST(LAMBDA v : '$[*]')``)."""
    if isinstance(expression.parent, exp.Func):
        return self.func("JSON_EXTRACT", expression.this, expression.expression)
    return _duckdb_json_arrow(self, expression)


DuckDB.Generator.TRANSFORMS[exp.JSONExtract] = _json_extract_call

_postgres_json_arrow = Postgres.Generator.TRANSFORMS[exp.JSONExtract]


def _json_arrow_read_back(self: Generator, expression: exp.JSONExtract) -> str:
    """PostgreSQL's ``v -> p``, parenthesised where it is a call's argument: sqlglot reads
    ``f(v -> p)`` back as a lambda over ``v``, which DuckDB 2.0 writes ``LAMBDA v : p`` and cannot
    read inside a CAST (Cypher keys() of a relationship's properties). ``f((v -> p))`` reads back
    as the extraction, in sqlglot and in PostgreSQL."""
    written = _postgres_json_arrow(self, expression)
    return f"({written})" if isinstance(expression.parent, exp.Func) else written


Postgres.Generator.TRANSFORMS[exp.JSONExtract] = _json_arrow_read_back

from provisa.transpiler import xml_syntax as _xml_syntax  # noqa: E402, F401 -- PostgreSQL's XML
from provisa.transpiler import pg_syntax as _pg_syntax  # noqa: E402, F401 -- what sqlglot misreads
