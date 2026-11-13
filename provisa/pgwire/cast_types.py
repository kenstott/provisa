# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2d4b17-6c3a-4f95-a0d8-1b7e9c5f3a62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL type of a result column that is a cast (REQ-2167, PGWIRE CATALOG AND TYPES).

A statement's result column whose expression is, at its root, a cast to a PostgreSQL type is
described as PostgreSQL describes it: with the cast's type OID and typmod. The engine computes the
value; several PostgreSQL types have no engine type of their own (varchar and character are the
engine's text, jsonb its JSON, oid its integer), and the engine names no typmod, so the cast is
what says the type. A cast is described by it only where the engine's value has that type's wire
form (``CastType.served_as``); a character(n) value is padded to its length, as bpchar's input
pads it."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
import decimal
import functools
import re
from dataclasses import dataclass
from typing import Any, Callable

from buenavista.core import BVType

from provisa.core.ir_types import interval_parts, interval_value
from provisa.core.pg_range import RANGE_TYPES
from provisa.federation.duckdb_pg_xml import xml_out
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.transpiler.pg_forms_json import jsonb_out
from provisa.transpiler.pg_held_types import HELD_AS
from provisa.transpiler.pg_type_literals import interval_text
from provisa.transpiler.xml_syntax import XmlSerialize

#: The header PostgreSQL adds to a typmod that carries a length or a precision (VARHDRSZ).
_VARHDRSZ = 4

_TEXT_FORMS = frozenset({BVType.TEXT})

_XML_FORMS = frozenset({BVType.TEXT, BVType.XML})

#: xml[]'s wire type, one the vendored BVType does not list: its OID is in BVTYPE_TO_OID
#: (provisa.pgwire.server), and the Rust encoder knows it by this name.
XML_ARRAY: Any = "xml[]"


@dataclass(frozen=True)
class CastType:
    """How a cast's result column is described: its OID, typmod and wire type; ``served_as`` the
    engine wire types whose value has this type's form; ``coerce`` brings the value to it, and
    ``of_text`` a value the engine serves as text."""

    oid: int
    typmod: int
    bvtype: BVType
    served_as: frozenset[BVType]
    coerce: Callable[[Any], Any] | None = None
    of_text: Callable[[Any], Any] | None = None


def _length(data_type: Any) -> int | None:
    sizes = data_type.expressions
    return int(sizes[0].name) if sizes else None


def _padded(length: int) -> Callable[[Any], Any]:
    """bpchar's input for character(length): cut to the length, then padded with spaces."""

    def pad(value: Any) -> Any:
        return value[:length].ljust(length) if isinstance(value, str) else value

    return pad


#: A range or multirange, computed as its text in PostgreSQL's form, by its name and sqlglot's.
_RANGE_TYPES: dict[str, CastType] = {
    **{
        r.name: CastType(r.oid, -1, BVType[r.name.upper()], _TEXT_FORMS)
        for r in RANGE_TYPES.values()
    },
    **{
        r.multirange: CastType(r.multirange_oid, -1, BVType[r.multirange.upper()], _TEXT_FORMS)
        for r in RANGE_TYPES.values()
    },
}
RANGE_CASTS = {name.upper(): cast for name, cast in _RANGE_TYPES.items()}


def _cast_type(data_type: Any) -> CastType | None:  # noqa: PLR0911 -- one return per cast type
    import sqlglot.expressions as exp

    kind = data_type.this
    t = exp.DataType.Type
    if kind == t.NAME:
        return CastType(19, -1, BVType.TEXT, _TEXT_FORMS)
    if kind == t.VARCHAR:
        length = _length(data_type)
        typmod = -1 if length is None else length + _VARHDRSZ
        return CastType(1043, typmod, BVType.TEXT, _TEXT_FORMS)
    if kind in (t.CHAR, t.BPCHAR):
        length = _length(data_type)
        if length is None and kind == t.CHAR:
            length = 1  # character with no length is character(1)
        if length is None:
            return CastType(1042, -1, BVType.TEXT, _TEXT_FORMS)
        return CastType(1042, length + _VARHDRSZ, BVType.TEXT, _TEXT_FORMS, _padded(length))
    if kind == t.DECIMAL:
        sizes = [int(e.name) for e in data_type.expressions]
        if not sizes:
            return CastType(1700, -1, BVType.DECIMAL, frozenset({BVType.DECIMAL}))
        precision, scale = sizes[0], sizes[1] if len(sizes) > 1 else 0
        typmod = ((precision << 16) | scale) + _VARHDRSZ
        return CastType(1700, typmod, BVType.DECIMAL, frozenset({BVType.DECIMAL}))
    if kind == t.FLOAT:
        return CastType(700, -1, BVType.REAL, frozenset({BVType.REAL, BVType.FLOAT}))
    if kind == t.TIMESTAMP:
        return _RETURNED["timestamp without time zone"]
    if kind == t.TIME:
        return _RETURNED["time without time zone"]
    if kind == t.JSONB:
        return _RETURNED["jsonb"]
    if kind == t.XML:  # REQ-2171: held as its text; xml_out writes its declaration
        return CastType(142, -1, BVType.XML, _XML_FORMS, xml_out)
    if kind == t.ARRAY and data_type.expressions and data_type.expressions[0].this == t.XML:
        return _RETURNED["xml[]"]
    ranged = RANGE_CASTS.get(kind.name) if isinstance(kind, exp.DataType.Type) else None
    if ranged is not None:
        return ranged
    if isinstance(data_type, exp.ObjectIdentifier) and data_type.name.lower() == "oid":
        integers = frozenset({BVType.SMALLINT, BVType.INTEGER, BVType.BIGINT, BVType.OID})
        return CastType(26, -1, BVType.OID, integers)
    return _HELD_AS_TEXT.get(data_type.sql(dialect="postgres").lower())


def _to_decimal(value: Any) -> Any:
    if value is None or isinstance(value, decimal.Decimal):
        return value
    return decimal.Decimal(str(value))


def _to_int(value: Any) -> Any:
    return None if value is None else int(value)


def _to_bool(value: Any) -> Any:
    return None if value is None else bool(value)


def _to_float(value: Any) -> Any:
    return None if value is None else float(value)


_INTEGERS = frozenset({BVType.SMALLINT, BVType.INTEGER, BVType.BIGINT})
_INTEGER_ARRAYS = frozenset({BVType.SMALLINTARRAY, BVType.INTEGERARRAY, BVType.BIGINTARRAY})
_NUMBERS = _INTEGERS | {BVType.REAL, BVType.FLOAT, BVType.DECIMAL}

#: A predicate: a boolean, whose value an engine with no boolean computes as 1 or 0.
_BOOLEAN = CastType(16, -1, BVType.BOOL, _INTEGERS | {BVType.BOOL}, _to_bool)


def _of_no_zone(value: Any) -> Any:
    """A timestamp an engine holds in a zone (ClickHouse's are in UTC) as its wall clock."""
    aware = isinstance(value, datetime.datetime) and value.tzinfo is not None
    return value.replace(tzinfo=None) if aware else value


def _clock_of(value: Any) -> Any:
    """A time: ClickHouse's Time64 reaches Arrow as the bytes of its count of microseconds."""
    if not isinstance(value, bytes):
        return value
    count = int.from_bytes(value, "little", signed=True)
    return (datetime.datetime.min + datetime.timedelta(microseconds=count)).time()


def bytes_of_text(value: Any) -> Any:
    """A call's bytea an engine holds as a string of its bytes (ClickHouse's String, which Arrow
    reads as text): the bytes the string is."""
    return value.encode("utf-8") if isinstance(value, str) else value


#: time_out's text: hours, minutes, seconds and up to six digits of a fraction.
_CLOCK_TEXT = re.compile(r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?", re.ASCII)


def clock_of_text(value: Any) -> Any:
    """A call's time an engine holds as time_out's text (ClickHouse's time is its text) as the time
    it is. 24:00:00, which PostgreSQL's time holds, is no time of day here, and is refused."""
    if not isinstance(value, str):
        return value
    clock = _CLOCK_TEXT.fullmatch(value)
    if clock is None or int(clock[1]) > 23 or int(clock[2]) > 59 or int(clock[3]) > 59:
        raise ValueError(f'invalid input syntax for type time: "{value}"')
    micros = int((clock[4] or "").ljust(6, "0"))
    return datetime.time(int(clock[1]), int(clock[2]), int(clock[3]), micros)


#: interval_out's text (IntervalStyle postgres): an amount and its unit, or the clock.
_INTERVAL_AMOUNT = re.compile(r"[+-]?\d+", re.ASCII)
_INTERVAL_UNITS = {"year": (12, 0), "years": (12, 0), "mon": (1, 0), "mons": (1, 0)} | {
    "day": (0, 1),
    "days": (0, 1),
}
_INTERVAL_CLOCK = re.compile(r"([+-]?)(\d+):(\d{2}):(\d{2})(?:\.(\d{1,6}))?", re.ASCII)


def interval_of_text(value: Any) -> Any:
    """An interval an engine serves as interval_out's text (interval_in's answer, folded to the
    text it reads as: provisa.transpiler.pg_forms_type_io) as the interval it is."""
    if not isinstance(value, str):
        return value
    words = value.split(" ")
    months = days = micros = 0
    at = 0
    while at < len(words) and value:
        clock = _INTERVAL_CLOCK.fullmatch(words[at])
        if clock is not None:
            sign = -1 if clock[1] == "-" else 1
            seconds = int(clock[2]) * 3600 + int(clock[3]) * 60 + int(clock[4])
            micros += sign * (seconds * 1_000_000 + int((clock[5] or "").ljust(6, "0")))
            at += 1
            continue
        unit = _INTERVAL_UNITS.get(words[at + 1]) if at + 1 < len(words) else None
        if unit is None or _INTERVAL_AMOUNT.fullmatch(words[at]) is None:
            raise ValueError(f'invalid input syntax for type interval: "{value}"')
        months += int(words[at]) * unit[0]
        days += int(words[at]) * unit[1]
        at += 2
    if not value:
        raise ValueError('invalid input syntax for type interval: ""')
    return interval_value(months, days, micros)


def _record_field(value: Any) -> str:
    """One field as record_out writes it: nothing for NULL, quoted where it must be."""
    if value is None:
        return ""
    if isinstance(value, list):
        text = "{" + ",".join("NULL" if v is None else str(v) for v in value) + "}"
    elif isinstance(value, bool):
        text = "t" if value else "f"
    elif isinstance(value, datetime.timedelta):
        text = interval_text(*interval_parts(value))  # interval_out, not Python's text
    else:
        text = str(value)
    if text == "" or any(c in text for c in '",\\() \t\n'):
        return '"' + text.replace("\\", "\\\\").replace('"', '""') + '"'
    return text


def record_text(value: Any) -> Any:
    """A record value (the engine's struct) in PostgreSQL's text form."""
    if isinstance(value, tuple):
        return "(" + ",".join(_record_field(v) for v in value) + ")"
    if not isinstance(value, dict):
        return value
    return "(" + ",".join(_record_field(v) for v in value.values()) + ")"


#: REQ-2171: the types only PostgreSQL has, which the engine holds as text, named by their oid.
_HELD_AS_TEXT = {
    entry[-1]: CastType(oid, -1, BVType.TEXT, _TEXT_FORMS)
    for oid, entry in TYPES.items()
    if entry[-1] in HELD_AS["VARCHAR"]
}

#: How a call of a pg_catalog function is described, by the type PostgreSQL says it returns
#: (function_types): the engine computes the value, in a type of its own where its function's
#: differs (DuckDB's length is a BIGINT, its avg a DOUBLE), brought to PostgreSQL's type. A type
#: absent here is the engine's to get right: its value is not this type's by a conversion of form.
_RETURNED: dict[str, CastType] = {
    **_HELD_AS_TEXT,
    "smallint": CastType(21, -1, BVType.SMALLINT, _INTEGERS, _to_int),
    "integer": CastType(23, -1, BVType.INTEGER, _INTEGERS, _to_int),
    "bigint": CastType(20, -1, BVType.BIGINT, _INTEGERS, _to_int),
    "boolean": _BOOLEAN,
    # An exact numeric of no fixed scale is served as its text (duckdb_pg_scalars).
    "numeric": CastType(1700, -1, BVType.DECIMAL, _NUMBERS | {BVType.TEXT}, _to_decimal),
    "double precision": CastType(701, -1, BVType.FLOAT, _NUMBERS, _to_float),
    "real": CastType(700, -1, BVType.REAL, frozenset({BVType.REAL, BVType.FLOAT}), _to_float),
    "oid": CastType(26, -1, BVType.OID, _INTEGERS | {BVType.OID}),
    "character": CastType(1042, -1, BVType.TEXT, _TEXT_FORMS),
    '"char"': CastType(18, -1, BVType.TEXT, _TEXT_FORMS),
    "name": CastType(19, -1, BVType.TEXT, _TEXT_FORMS),
    "character varying": CastType(1043, -1, BVType.TEXT, _TEXT_FORMS),
    "bit": CastType(1560, -1, BVType.BIT, frozenset({BVType.BIT, BVType.TEXT})),
    "bit varying": CastType(1562, -1, BVType.VARBIT, frozenset({BVType.BIT, BVType.TEXT})),
    **_RANGE_TYPES,
    "tsvector": CastType(3614, -1, BVType.TSVECTOR, _TEXT_FORMS),
    # An engine's json text (Trino's json_object is a varchar): jsonb_out writes it as jsonb's.
    "jsonb": CastType(
        3802, -1, BVType.JSONB, frozenset({BVType.JSON, BVType.JSONB, BVType.TEXT}), jsonb_out
    ),
    "name[]": CastType(1003, -1, BVType.NAMEARRAY, frozenset({BVType.STRINGARRAY})),
    "inet": CastType(869, -1, BVType.INET, frozenset({BVType.INET, BVType.TEXT})),
    "cidr": CastType(650, -1, BVType.CIDR, frozenset({BVType.INET, BVType.TEXT})),
    "tid": CastType(27, -1, BVType.TEXT, _TEXT_FORMS),
    "xid": CastType(28, -1, BVType.OID, _INTEGERS | {BVType.OID}),
    "xid8": CastType(5069, -1, BVType.BIGINT, _INTEGERS, _to_int),
    "record": CastType(2249, -1, BVType.TEXT, _TEXT_FORMS | _INTEGERS | {BVType.JSON}, record_text),
    "void": CastType(2278, -1, BVType.TEXT, _TEXT_FORMS),
    "xml": CastType(142, -1, BVType.XML, _XML_FORMS, xml_out),
    "xml[]": CastType(143, -1, XML_ARRAY, frozenset({BVType.STRINGARRAY, XML_ARRAY})),
    "cstring": CastType(2275, -1, BVType.TEXT, _TEXT_FORMS),
    # ClickHouse holds bytes as a String, which Arrow reads as its text.
    "bytea": CastType(
        17, -1, BVType.BYTES, frozenset({BVType.BYTES, BVType.TEXT}), of_text=bytes_of_text
    ),
    # An array of integers an engine names by none of its widths (arrow_type_name's INTEGER[] of
    # ClickHouse's Int64): the call's own type describes it.
    "integer[]": CastType(1007, -1, BVType.INTEGERARRAY, _INTEGER_ARRAYS),
    "bigint[]": CastType(1016, -1, BVType.BIGINTARRAY, _INTEGER_ARRAYS),
    "double precision[]": CastType(1022, -1, BVType.FLOATARRAY, frozenset({BVType.FLOATARRAY})),
    "text[]": CastType(1009, -1, BVType.STRINGARRAY, frozenset({BVType.STRINGARRAY})),
    # ClickHouse's time is its Time64, or the text a form writes of it (pg_forms_type_io).
    "time without time zone": CastType(
        1083,
        -1,
        BVType.TIME,
        frozenset({BVType.TIME, BVType.BYTES, BVType.TEXT}),
        _clock_of,
        of_text=clock_of_text,
    ),
    "interval": CastType(1186, -1, BVType.INTERVAL, frozenset({BVType.INTERVAL})),
    "timestamp without time zone": CastType(
        1114, -1, BVType.TIMESTAMP, frozenset({BVType.TIMESTAMP}), _of_no_zone
    ),
    "timestamp with time zone": CastType(
        1184, -1, BVType.TIMESTAMPTZ, frozenset({BVType.TIMESTAMP, BVType.TIMESTAMPTZ})
    ),
    # The engine holds money as its base; its encoder sends the value as money.
    "money": CastType(790, -1, BVType.MONEY, _NUMBERS | {BVType.TEXT, BVType.MONEY}),
    "money[]": CastType(
        791,
        -1,
        BVType.MONEYARRAY,
        frozenset({BVType.DECIMALARRAY, BVType.MONEYARRAY, BVType.STRINGARRAY}),
    ),
    # The engine answers a regtype as its name; its encoder sends it as a regtype.
    "regtype": CastType(2206, -1, BVType.REGTYPE, _TEXT_FORMS | {BVType.REGTYPE}),
}


#: The types a form holds as its text, or as what the engine has of it, by format_type's name.
_NAMED_BY_THE_CALL = {
    '"any"': 2276, "anyarray": 2277, "anycompatible": 5077, "anycompatiblearray": 5078,
    "anycompatiblemultirange": 4538, "anycompatiblenonarray": 5079, "anycompatiblerange": 5080,
    "anyelement": 2283, "anyenum": 3500, "anymultirange": 4537, "anynonarray": 2776,
    "anyrange": 3831, "cid": 29, "gtsvector": 3642, "int2vector": 22, "json": 114, "jsonpath": 4072,
    "oidvector": 30, "pg_brin_bloom_summary": 4600, "pg_brin_minmax_multi_summary": 4601,
    "pg_ddl_command": 32, "regclass": 2205, "regcollation": 4191, "regconfig": 3734,
    "regdictionary": 3769, "regnamespace": 4089, "regoper": 2203, "regoperator": 2204,
    "regproc": 24, "regprocedure": 2202, "regrole": 4096, "text": 25,
    "time with time zone": 1266, "tsquery": 3615,
}  # fmt: skip
_ANY_FORM = _TEXT_FORMS | _INTEGERS | {BVType.JSON}
_RETURNED.update(
    {name: CastType(oid, -1, BVType.TEXT, _ANY_FORM) for name, oid in _NAMED_BY_THE_CALL.items()}
)
_RETURNED["date"] = CastType(1082, -1, BVType.DATE, frozenset({BVType.DATE}))
_RETURNED["uuid"] = CastType(2950, -1, BVType.UUID, frozenset({BVType.UUID, BVType.TEXT}))


def _predicate_nodes() -> tuple[type, ...]:
    import sqlglot.expressions as exp

    return (
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.GTE,
        exp.LT,
        exp.LTE,
        exp.ArrayOverlaps,
        exp.ArrayContainsAll,
        exp.ArrayContainedBy,
        exp.Adjacent,
        exp.ExtendsLeft,
        exp.ExtendsRight,
    )


_PREDICATES = _predicate_nodes()


def _operand_type(operand: Any) -> CastType | None:
    """A range operator's result (a + b, a * b, a - b) is its operand's range or multirange."""
    import sqlglot.expressions as exp

    while isinstance(operand, exp.Paren):
        operand = operand.this
    if isinstance(operand, exp.Cast) and isinstance(operand.to.this, exp.DataType.Type):
        return RANGE_CASTS.get(operand.to.this.name)
    if isinstance(operand, (exp.Add, exp.Sub, exp.Mul)):
        return _operand_type(operand.this) or _operand_type(operand.expression)
    return None


def _null_alone(cast: Any) -> bool:
    """Whether ``cast`` is of a NULL, through parentheses."""
    import sqlglot.expressions as exp

    value = cast.this
    while isinstance(value, exp.Paren):
        value = value.this
    return isinstance(value, exp.Null)


#: The array types a NULL cast describes, by PostgreSQL's name: those a call returns, and
#: cstring[] (_cstring), which no call here returns.
_NULL_ARRAYS: dict[str, CastType] = {
    **{name: cast for name, cast in _RETURNED.items() if name.endswith("[]")},
    "cstring[]": CastType(1263, -1, BVType.STRINGARRAY, frozenset({BVType.STRINGARRAY})),
}


def _result_type(expression: Any, columns: Any) -> CastType | None:
    """How a result column is described by its root: a money expression as PostgreSQL's operators
    type it, a cast by the cast's type, a call of a pg_catalog function by the type that function
    returns; None for any other column."""
    import sqlglot.expressions as exp

    from provisa.pgwire.function_types import argument_type, call_result_type, json_operator_type
    from provisa.transpiler.pg_money import money_type

    # A column of a table only the plan's shape types: a part of the expression is not known here.
    typed = None
    if columns is not None and all(columns(c) for c in expression.find_all(exp.Column)):
        typed = money_type(expression, columns)
    if typed is not None:
        return _RETURNED[typed]
    if isinstance(expression, exp.Cast):
        if _null_alone(expression):
            # ClickHouse holds no NULL of an array (an Array is never Nullable): it is sent a bare
            # NULL (semantic_dialect), which the cast's array type describes.
            null_array = _NULL_ARRAYS.get(argument_type(expression) or "")
            if null_array is not None:
                return null_array
        return _cast_type(expression.to)
    if isinstance(expression, XmlSerialize):
        return _cast_type(expression.args["to"])
    if isinstance(expression, (exp.Paren, exp.Window)):
        return _result_type(expression.this, columns)
    if json_operator_type(expression) is not None:
        return _RETURNED["jsonb"]
    if isinstance(expression, exp.Array):
        elements = [_result_type(element, columns) for element in expression.expressions]
        xml_elements = elements and all(e is not None and e.oid == 142 for e in elements)
        return _RETURNED["xml[]"] if xml_elements else None
    if isinstance(expression, exp.Bracket):
        # An element of an array function's result, as (xpath(...))[1] reads one.
        array = call_result_type(expression.this.unnest())
        return _RETURNED.get(array[:-2]) if array and array.endswith("[]") else None
    if isinstance(expression, (exp.Add, exp.Sub, exp.Mul)):
        return _operand_type(expression.this) or _operand_type(expression.expression)
    if isinstance(expression, _PREDICATES):
        return _BOOLEAN
    if isinstance(expression, (exp.BitwiseLeftShift, exp.BitwiseRightShift)) and (
        _operand_type(expression.this) or _operand_type(expression.expression)
    ):
        return _BOOLEAN  # << and >> of a range: on anything else, a shift
    returned = call_result_type(expression)
    if returned == "interval":
        return _CALLED_INTERVAL
    return None if returned is None else _RETURNED.get(returned) or _row_type(returned)


#: A call's interval, which an engine with no interval of PostgreSQL's may hold as its text: the
#: text pg_forms_type_io folds interval_in to, on Trino.
_CALLED_INTERVAL = CastType(
    1186, -1, BVType.INTERVAL, frozenset({BVType.INTERVAL, BVType.TEXT}), of_text=interval_of_text
)


def _row_type(name: str) -> CastType | None:
    """A relation's composite type, which a populate function returns; held as its text."""
    from provisa.pgwire.catalog_pg17 import TYPES

    for oid, (_, typname, _, typtype, *_) in TYPES.items():
        if typname == name and typtype == "c":
            return CastType(oid, -1, BVType.TEXT, _TEXT_FORMS)
    return None


def _derived_xml(tree: Any, name: str) -> CastType | None:
    """The xml type of a column read from a lone subquery or CTE that says it. The engine holds
    xml as text, so its own type says nothing past the expression that made the value."""
    import sqlglot.expressions as exp

    source = tree.args.get("from_")
    inner = None if source is None or tree.args.get("joins") else source.this
    if isinstance(inner, exp.Table):
        with_ = tree.args.get("with_")
        ctes = with_.expressions if with_ is not None else ()
        inner = next((cte.this for cte in ctes if cte.alias == inner.name), None)
    elif isinstance(inner, exp.Subquery):
        inner = inner.this
    if not isinstance(inner, exp.Select):
        return None
    for select in inner.selects:
        if select.alias_or_name == name:
            typed = _column_type(inner, select.unalias(), None)
            return typed if typed is not None and typed.oid in (142, 143) else None
    return None


def _jsonb_values(values: Any) -> dict[str, CastType | None]:
    """The columns of a VALUES list its first row says are jsonb."""
    import sqlglot.expressions as exp

    alias = values.args.get("alias")
    rows = values.expressions
    if alias is None or not alias.columns or not rows or not isinstance(rows[0], exp.Tuple):
        return {}
    cells = zip(alias.columns, rows[0].expressions)
    return {
        column.name: _RETURNED["jsonb"]
        for column, cell in cells
        if isinstance(cell, exp.Cast) and cell.is_type("jsonb")
    }


def _jsonb_columns(tree: Any) -> dict[str, CastType | None]:
    """The columns a jsonb set-returning function in a statement's FROM gives it, by name."""
    import sqlglot.expressions as exp

    found: dict[str, CastType | None] = {}
    for values in tree.find_all(exp.Values):
        found.update(_jsonb_values(values))
    for table in tree.find_all(exp.Table):
        call = table.this.name.lower() if isinstance(table.this, exp.Anonymous) else ""
        alias = table.args.get("alias")
        for column in alias.columns if alias is not None else ():
            if isinstance(column, exp.ColumnDef) and column.args["kind"].is_type("jsonb"):
                found[column.name] = _RETURNED["jsonb"]  # of json_to_record's definition list
        if call in ("jsonb_path_query", "jsonb_path_query_tz"):
            found[alias.columns[0].name if alias is not None and alias.columns else call] = (
                _RETURNED["jsonb"]
            )
        names = {"jsonb_array_elements": ("value",), "jsonb_each": ("key", "value")}.get(call)
        if names is None or table.find_ancestor(exp.Select) is not tree:
            continue
        alias = table.args.get("alias")
        renamed = [c.name for c in alias.columns] if alias is not None else []
        for index, name in enumerate(names):
            kind = _RETURNED["jsonb"] if name == "value" else None
            found[renamed[index] if index < len(renamed) else name] = kind
    return found


def _typed_call(expression: Any) -> CastType | None:
    """The type a form's marker says of a value only its source has a type for (pg_forms_enum)."""
    import sqlglot.expressions as exp

    from provisa.core.pg_enum import array_text
    from provisa.transpiler.pg_forms_enum import TYPED_ARRAY, TYPED_VALUE

    called = expression.name.lower() if isinstance(expression, exp.Anonymous) else ""
    if called not in (TYPED_VALUE, TYPED_ARRAY):
        return None
    oid = int(expression.expressions[0].this)
    if called == TYPED_VALUE:
        return CastType(oid, -1, BVType.TEXT, _TEXT_FORMS)
    return CastType(oid, -1, BVType.TEXT, _TEXT_FORMS | {BVType.STRINGARRAY}, array_text)


def _column_type(tree: Any, expression: Any, columns: Any) -> CastType | None:
    import sqlglot.expressions as exp

    typed = _typed_call(expression)
    if typed is not None:
        return typed
    reads_jsonb = isinstance(expression, (exp.JSONExtract, exp.JSONBExtract))
    if reads_jsonb and isinstance(expression.this, exp.Column):
        jsonb = _jsonb_columns(tree).get(expression.this.name)
        if jsonb is not None:
            return jsonb
    if isinstance(expression, exp.Column):
        jsonb = _jsonb_columns(tree).get(expression.name)
        if jsonb is not None:
            return jsonb
        xml = _xmltable_columns(tree).get(expression.name) or _derived_xml(tree, expression.name)
        if xml is not None:
            return xml
    return _result_type(expression, columns)


def _xmltable_column(column: Any) -> CastType | None:
    if column.args.get("ordinality"):
        return _RETURNED["integer"]
    kind = column.args["kind"]
    return _cast_type(kind) if kind.expressions or kind.sql() != "DECIMAL" else _RETURNED["numeric"]


def _xmltable_columns(tree: Any) -> dict[str, CastType | None]:
    """The columns a lone XMLTABLE gives its statement, by name; none for any other FROM."""
    import sqlglot.expressions as exp

    source = tree.args.get("from_")
    table = None if source is None else source.this
    if tree.args.get("joins") or not isinstance(table, exp.Table):
        return {}
    if not isinstance(table.this, exp.XMLTable):
        return {}
    return {column.name: _xmltable_column(column) for column in table.this.args["columns"]}


@functools.lru_cache(maxsize=4096)
def result_casts(sql: str) -> tuple[CastType | None, ...] | None:
    """For each result column of ``sql`` (a single SELECT), the type its root cast gives it, or
    None for a column that is not such a cast; None for a statement that is not a single SELECT
    or that does not parse as PostgreSQL."""
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import ParseError

    from provisa.transpiler.pg_money import names_money, statement_columns

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return None  # not PostgreSQL's grammar: the engine's own types describe it
    if not isinstance(tree, exp.Select):
        return None
    columns = statement_columns(tree) if names_money(sql) else None
    out: list[CastType | None] = []
    xmltable = _xmltable_columns(tree) or _jsonb_columns(tree)
    for select in tree.selects:
        expression = select.unalias()
        if isinstance(expression, exp.Star):
            # A star's columns are otherwise known only to the engine.
            return tuple(xmltable.values()) if xmltable else None
        out.append(_column_type(tree, expression, columns))
    return tuple(out)


#: The registered types described with a typmod of their own (REQ-1961, amended 2026-10-28):
#: a sized numeric, and the character types a text column's native type may be.
_CHARACTER_KINDS = ("VARCHAR", "CHAR", "BPCHAR")


@functools.lru_cache(maxsize=4096)
def described_type(spelling: str) -> CastType | None:
    """How a registered type is described (REQ-1961, PGWIRE SENDS NUMERIC(P,S); VARCHAR AND CHAR
    KEEP THEIR TYPE): a numeric(p,s) as numeric with its typmod, and varchar, varchar(n), char(n)
    or bpchar as that type with its length typmod, as a cast to it is. None for any other type,
    which its wire type describes."""
    import sqlglot.expressions as exp
    from sqlglot.errors import SqlglotError

    from provisa.core.numeric_type import numeric_size_of

    try:
        data_type = exp.DataType.build(spelling, dialect="postgres")
    except (SqlglotError, ValueError):
        return None  # no PostgreSQL type: its wire type describes it
    kind = data_type.this
    if not isinstance(kind, exp.DataType.Type):
        return None  # an object identifier type (oid, regclass): sqlglot names it by a string
    if kind == exp.DataType.Type.DECIMAL and numeric_size_of(spelling) is None:
        return None  # an unsized numeric has no typmod
    if kind != exp.DataType.Type.DECIMAL and kind.name not in _CHARACTER_KINDS:
        return None
    return _cast_type(data_type)


def registered_spelling(data_type: str, native_type: str | None) -> str:
    """The type a registered column is described by on pgwire and in its catalog: a text column
    whose native type is a character type (registration keeps it, REQ-1961) is that type;
    any other column is its registered type."""
    if native_type and data_type.lower() == "text":
        native = described_type(native_type)
        if native is not None and native.bvtype == BVType.TEXT:
            return native_type
    return data_type
