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

from provisa.core.pg_range import MULTIRANGE_TYPES, RANGE_TYPES
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS
from provisa.transpiler.pg_literals import values_cast
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
    exp.DataType.Type.INT4RANGE: "int4range",
    exp.DataType.Type.INT8RANGE: "int8range",
    exp.DataType.Type.NUMRANGE: "numrange",
    exp.DataType.Type.TSRANGE: "tsrange",
    exp.DataType.Type.TSTZRANGE: "tstzrange",
    exp.DataType.Type.DATERANGE: "daterange",
    exp.DataType.Type.INT4MULTIRANGE: "int4multirange",
    exp.DataType.Type.INT8MULTIRANGE: "int8multirange",
    exp.DataType.Type.NUMMULTIRANGE: "nummultirange",
    exp.DataType.Type.TSMULTIRANGE: "tsmultirange",
    exp.DataType.Type.TSTZMULTIRANGE: "tstzmultirange",
    exp.DataType.Type.DATEMULTIRANGE: "datemultirange",
}

#: PostgreSQL's format_type name of a range's element.
_ELEMENT_NAMES = {
    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
}


#: A sqlglot class naming a PostgreSQL function by a name of its own.
_SPELLINGS: dict[type, tuple[str, ...]] = {
    exp.StrPosition: ("strpos", "position"),
    exp.Explode: ("unnest",),
    exp.Unnest: ("unnest",),
    exp.JSONBObjectAgg: ("jsonb_object_agg",),
    exp.JSONObjectAgg: ("json_object_agg",),
}


def _arg_type(arg: exp.Expr, literal: str | None, numbers: bool) -> str | None:
    """PostgreSQL's name of an argument's type, or None where the statement does not say it. A
    string literal is of unknown type, which overload resolution fits to the overload: ``literal``
    is what it is read as (None: any type). ``numbers``: a numeric literal has the type it reads
    as; otherwise it is said to fit any overload, as PostgreSQL's implicit casts widen it."""
    if isinstance(arg, (exp.Neg, exp.Paren, exp.Order)):  # an aggregate's value ORDER BY ...
        return _arg_type(arg.this, literal, numbers)
    if isinstance(arg, exp.Literal):
        if arg.is_string:
            return literal
        if not numbers:
            return None
        return "numeric" if any(c in arg.this for c in ".eE") else "integer"
    if isinstance(arg, exp.Boolean):
        return "boolean"
    if isinstance(arg, exp.Column):
        said = values_cast(arg)
        return None if said is None else _arg_type(said, literal, numbers)
    if isinstance(arg, exp.Cast):
        to = arg.to
        if isinstance(to, exp.ObjectIdentifier) or to.this == exp.DataType.Type.USERDEFINED:
            spelled = to.sql(dialect="postgres").lower().removeprefix("pg_catalog.")
            return {"varbit": "bit varying", "_cstring": "cstring[]"}.get(spelled, spelled)
        if to.this == exp.DataType.Type.ARRAY:
            element = _PG_NAMES.get(to.expressions[0].this) if to.expressions else None
            return None if element is None else f"{element}[]"
        return _PG_NAMES.get(to.this)
    call = _call(arg)
    if call is not None:
        return call_result_type(arg)
    return None


def argument_type(arg: exp.Expr) -> str | None:
    """PostgreSQL's name of the type a statement says of ``arg``, None where it does not say one
    (a column, an untyped literal, a call whose overload is not known). REQ-2236's typed holes read
    an argument's type this way."""
    return _arg_type(arg, None, True)


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


#: The parameters of a function of any number of arguments of any type.
_VARIADIC_ANY = ('"any"',)
#: The pseudo-types a value of any type is.
_OF_ANY_TYPE = frozenset({'"any"', "anyelement", "anycompatible"})


def json_operator_type(expression: exp.Expr) -> str | None:
    """ "jsonb" for a json operator whose left operand is one, or for ``||`` of either operand;
    None for any other expression."""
    if isinstance(expression, exp.Paren):
        return json_operator_type(expression.this)
    readers = (exp.JSONExtract, exp.JSONBExtract, exp.JSONBDeleteAtPath, exp.Sub, exp.DPipe)
    if not isinstance(expression, readers):
        return None
    left = json_operator_type(expression.this) or _arg_type(expression.this, None, True)
    if left != "jsonb" and isinstance(expression, exp.DPipe):
        return json_operator_type(expression.expression) or _said_jsonb(expression.expression)
    return left if left == "jsonb" else None


def _said_jsonb(expression: exp.Expr) -> str | None:
    return "jsonb" if _arg_type(expression, None, True) == "jsonb" else None


#: What returns the row type of its first argument, whatever pg_proc's anyelement says.
_RETURNS_ITS_BASE = frozenset(
    f"{kind}_populate_{rows}" for kind in ("json", "jsonb") for rows in ("record", "recordset")
)


#: The window functions that return a value of their first argument (pg_proc's anyelement, which
#: its list of overloads does not hold): lag and lead with a default return the common type of the
#: value and the default.
_OF_ITS_VALUE = frozenset({"first_value", "last_value", "nth_value", "lag", "lead"})


def _value_type(name: str, args: list[exp.Expr]) -> str | None:
    """A window function's value: its first argument's type, which a default must share."""
    value = _arg_type(args[0], None, True)
    if name in ("lag", "lead") and len(args) == 3 and _arg_type(args[2], None, True) != value:
        return None
    return value


def _ordered_set_type(within: exp.WithinGroup) -> str | None:
    """An ordered-set aggregate's result: percentile_disc and mode return the type they order
    by, percentile_cont a double precision (an interval of intervals), each an array of it for
    an array of fractions."""
    call = _call(within.this)
    order = within.expression
    if call is None or not isinstance(order, exp.Order) or len(order.expressions) != 1:
        return None
    names, args = call
    ordered = order.expressions[0]
    by = _arg_type(ordered.this if isinstance(ordered, exp.Ordered) else ordered, None, True)
    fractions = _arg_type(args[0], None, True) if args else None
    of_array = "[]" if fractions is not None and fractions.endswith("[]") else ""
    if by is None:
        return None
    if "mode" in names:
        return by
    if "percentile_disc" in names:
        return by + of_array
    if "percentile_cont" in names:
        return ("interval" if by == "interval" else "double precision") + of_array
    return None


#: Each built-in type, as format_type names it. Any other name a statement casts to may be its enum.
_BUILT_IN = frozenset(row[-1] for row in TYPES.values())


def _fits(argument: str | None, parameter: str) -> bool:
    """A string argument fits a character varying parameter as it fits a text one, any argument a
    polymorphic parameter of no kind, and a range or multirange the pseudo-type of its kind."""
    return (
        argument is None
        or argument == parameter
        or parameter in _OF_ANY_TYPE
        or (parameter in _NONARRAYS and not argument.endswith("[]"))
        or (
            parameter == "anyenum" and argument not in _BUILT_IN
        )  # only the server's catalog names one
        or (argument == "text" and parameter == "character varying")
        or (parameter in ("anyarray", "anycompatiblearray") and argument.endswith("[]"))
        or (parameter in ("anyrange", "anycompatiblerange") and argument in RANGE_TYPES)
        or (
            parameter in ("anymultirange", "anycompatiblemultirange")
            and argument in MULTIRANGE_TYPES
        )
    )


def fits(argument: str | None, parameter: str) -> bool:
    """Whether an argument of type ``argument`` (None: not said) fits a parameter of type
    ``parameter``, as overload resolution here fits it."""
    return _fits(argument, parameter)


#: The polymorphic parameters of each family that take an element, or an array of one.
_NONARRAYS = frozenset({"anynonarray", "anycompatiblenonarray"})
_ELEMENTS = {
    "anyelement": frozenset({"anyelement", "anynonarray", "anyenum"}),
    "anycompatible": frozenset({"anycompatible", "anycompatiblenonarray"}),
}
_ARRAYS = {"anyelement": "anyarray", "anycompatible": "anycompatiblearray"}
_FAMILY = {
    **{p: family for family, params in _ELEMENTS.items() for p in params},
    **{a: family for family, a in _ARRAYS.items()},
}


def _element(family: str, params: tuple[str, ...], arg_types: list[str | None]) -> str | None:
    """The element type a family's arguments resolve to: an element's own type, an array's
    element (PostgreSQL's enforce_generic_type_consistency)."""
    for param, argument in zip(params, arg_types):
        if argument is None:
            continue
        if param in _ELEMENTS[family]:
            return argument
        if param == _ARRAYS[family] and argument.endswith("[]"):
            return argument[:-2]
    return None


def _resolved(returns: str, arg_types: list[str | None], params: tuple[str, ...] = ()) -> str:
    """A polymorphic result as the element, array, range or multirange among the arguments
    makes it."""
    family = _FAMILY.get(returns)
    element = None if family is None else _element(family, params, arg_types)
    if element is not None:
        return f"{element}[]" if returns in _ARRAYS.values() else element
    if returns == "anyarray":
        return next((a for a in arg_types if a is not None and a.endswith("[]")), returns)
    if returns not in ("anyelement", "anyrange", "anymultirange"):
        return returns
    for argument in arg_types:
        rtype = RANGE_TYPES.get(argument or "") or MULTIRANGE_TYPES.get(argument or "")
        if rtype is None:
            continue
        if returns == "anyelement":
            return _ELEMENT_NAMES.get(rtype.element, rtype.element)
        return rtype.name if returns == "anyrange" else rtype.multirange
    return returns


def call_result_type(expression: exp.Expr) -> str | None:
    """The format_type name of what a call of a pg_catalog function returns; None when the
    expression is not such a call or its overload is not known from the statement."""
    if isinstance(expression, exp.Dot):
        expression = expression.expression
    if isinstance(expression, exp.Window):
        expression = expression.this
    if isinstance(expression, exp.Column) and isinstance(expression.this, exp.WithinGroup):
        expression = expression.this  # pg_catalog.f(...) WITHIN GROUP: sqlglot's column of it
    if isinstance(expression, exp.WithinGroup):
        return _ordered_set_type(expression)
    if isinstance(expression, XML_VALUES):
        return "xml"  # PostgreSQL's grammar has these, and pg_proc none
    call = _call(expression)
    if call is None:
        return None
    names, args = call
    if names[0] in _RETURNS_ITS_BASE and args:
        return _arg_type(args[0], None, True)
    if names[0] in _OF_ITS_VALUE and args:
        return _value_type(names[0], args)
    if names[0] == "ntile":
        return "integer"  # a window function: pg_proc's list of overloads does not hold it
    if names[0] in RETURNS_XML and names[0] not in FUNCTION_OVERLOADS:
        return "xml"
    if names[0] in MULTIRANGE_TYPES:
        return names[0]  # a multirange's constructor is variadic: any number of ranges
    overloads = FUNCTION_OVERLOADS.get(names[0], ())
    returned = {returns for _, returns in overloads}
    if len(returned) == 1 and all(len(params) != len(args) for params, _ in overloads):
        return returned.pop()  # a VARIADIC overload: every one returns this type
    # An unknown-type literal fits any overload; where that leaves the call ambiguous, it is text,
    # as PostgreSQL prefers a string category type for one. A number's own type is tried first.
    for literal, numbers in ((None, True), ("text", True), (None, False), ("text", False)):
        arg_types = [_arg_type(a, literal, numbers) for a in args]
        for name in names:
            candidates = {
                _resolved(returns, arg_types, params)
                for params, returns in FUNCTION_OVERLOADS.get(name, ())
                if (len(params) == len(arg_types) or params == _VARIADIC_ANY)
                and all(_fits(t, p) for t, p in zip(arg_types, params))
            }
            if len(candidates) == 1:
                return candidates.pop()
    return None
