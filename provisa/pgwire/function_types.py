# Copyright (c) 2026 Kenneth Stott
# Canary: 58faa722-fee8-40d1-b37c-dcd5c20c799c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL type a call of a pg_catalog function returns (REQ-2167, PGWIRE'S CATALOG
FUNCTIONS).

A call resolves to the overload its arguments' types name (catalog_pg17_overloads, PostgreSQL
17.11's pg_proc): an argument whose type the statement says -- a cast, a literal, a call whose own
type resolves -- picks among the overloads; one it does not say (a column) matches any. The call
returns the type every overload it may resolve to returns; where they disagree, or no overload
matches, its type is not known here."""

# Requirements: REQ-2167

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS
from provisa.transpiler.xml_syntax import RETURNS_XML, XML_VALUES

#: sqlglot's type of an argument -> PostgreSQL's format_type name of it. A varchar is text to
#: overload resolution (every pg_catalog function taking a string takes text).
_PG_NAMES: dict[Any, str] = {
    exp.DataType.Type.SMALLINT: "smallint",
    exp.DataType.Type.INT: "integer",
    exp.DataType.Type.BIGINT: "bigint",
    exp.DataType.Type.DECIMAL: "numeric",
    exp.DataType.Type.DOUBLE: "double precision",
    exp.DataType.Type.FLOAT: "real",
    exp.DataType.Type.TEXT: "text",
    exp.DataType.Type.VARCHAR: "text",
    exp.DataType.Type.NAME: "name",
    exp.DataType.Type.CHAR: "character",
    exp.DataType.Type.BPCHAR: "character",
    exp.DataType.Type.VARBINARY: "bytea",
    exp.DataType.Type.BOOLEAN: "boolean",
    exp.DataType.Type.DATE: "date",
    exp.DataType.Type.TIME: "time without time zone",
    exp.DataType.Type.TIMETZ: "time with time zone",
    exp.DataType.Type.TIMESTAMP: "timestamp without time zone",
    exp.DataType.Type.TIMESTAMPTZ: "timestamp with time zone",
    exp.DataType.Type.INTERVAL: "interval",
    exp.DataType.Type.UUID: "uuid",
    exp.DataType.Type.JSON: "json",
    exp.DataType.Type.JSONB: "jsonb",
    exp.DataType.Type.INET: "inet",
    exp.DataType.Type.BIT: "bit",
    exp.DataType.Type.MONEY: "money",
    exp.DataType.Type.XML: "xml",
    exp.DataType.Type.POINT: "point",
    exp.DataType.Type.NUMRANGE: "numrange",
}


#: A sqlglot class naming a PostgreSQL function by a name of its own.
_SPELLINGS: dict[type, tuple[str, ...]] = {
    exp.StrPosition: ("strpos", "position"),
    exp.Explode: ("unnest",),
    exp.Unnest: ("unnest",),
}


def _arg_type(arg: exp.Expr, literal: str | None, numbers: bool) -> str | None:
    """PostgreSQL's name of an argument's type, or None where the statement does not say it. A
    string literal is of unknown type, which overload resolution fits to the overload: ``literal``
    is what it is read as (None: any type). ``numbers``: a numeric literal has the type it reads
    as; otherwise it is said to fit any overload, as PostgreSQL's implicit casts widen it."""
    if isinstance(arg, (exp.Neg, exp.Paren)):
        return _arg_type(arg.this, literal, numbers)
    if isinstance(arg, exp.Literal):
        if arg.is_string:
            return literal
        if not numbers:
            return None
        return "numeric" if any(c in arg.this for c in ".eE") else "integer"
    if isinstance(arg, exp.Boolean):
        return "boolean"
    if isinstance(arg, exp.Cast):
        to = arg.to
        if isinstance(to, exp.ObjectIdentifier) or to.this == exp.DataType.Type.USERDEFINED:
            spelled = to.sql(dialect="postgres").lower().removeprefix("pg_catalog.")
            return "bit varying" if spelled == "varbit" else spelled
        if to.this == exp.DataType.Type.ARRAY:
            return None
        return _PG_NAMES.get(to.this)
    call = _call(arg)
    if call is not None:
        return call_result_type(arg)
    return None


def _call(expression: exp.Expr) -> tuple[list[str], list[exp.Expr]] | None:
    """The names a call may be written as, and its arguments in order; None for no call."""
    if isinstance(expression, exp.Dot):
        expression = expression.expression
    if isinstance(expression, exp.Anonymous):
        return [expression.name.lower()], list(expression.expressions)
    if isinstance(expression, exp.Func) and not isinstance(expression, exp.Cast):
        # A subclass (ExplodingGenerateSeries) is a form of the function its parent names.
        names = [
            *_SPELLINGS.get(type(expression), ()),
            *(
                name.lower()
                for parent in type(expression).__mro__
                if issubclass(parent, exp.Func)
                and parent is not exp.Func
                and parent.__name__ not in ("AggFunc", "SafeFunc")
                for name in parent.sql_names()
            ),
        ]
        args: list[exp.Expr] = []
        for key in expression.arg_types:
            value = expression.args.get(key)
            if isinstance(value, list):
                args.extend(value)
            elif isinstance(value, exp.Expr):
                args.append(value)
        return names, args
    return None


def _fits(argument: str | None, parameter: str) -> bool:
    """A string argument fits a character varying parameter as it fits a text one."""
    return (
        argument is None
        or argument == parameter
        or (argument == "text" and parameter == "character varying")
        or (parameter == "anyarray" and argument.endswith("[]"))
    )


def call_result_type(expression: exp.Expr) -> str | None:
    """The format_type name of what a call of a pg_catalog function returns; None when the
    expression is not such a call or its overload is not known from the statement."""
    if isinstance(expression, exp.Dot):
        expression = expression.expression
    if isinstance(expression, XML_VALUES):
        return "xml"  # PostgreSQL's grammar has these, and pg_proc none
    call = _call(expression)
    if call is None:
        return None
    names, args = call
    if names[0] in RETURNS_XML and names[0] not in FUNCTION_OVERLOADS:
        return "xml"
    # An unknown-type literal fits any overload; where that leaves the call ambiguous, it is text,
    # as PostgreSQL prefers a string category type for one. A number's own type is tried first.
    for literal, numbers in ((None, True), ("text", True), (None, False), ("text", False)):
        arg_types = [_arg_type(a, literal, numbers) for a in args]
        for name in names:
            candidates = {
                returns
                for params, returns in FUNCTION_OVERLOADS.get(name, ())
                if len(params) == len(arg_types)
                and all(_fits(t, p) for t, p in zip(arg_types, params))
            }
            if len(candidates) == 1:
                return candidates.pop()
    return None
