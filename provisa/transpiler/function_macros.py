# Copyright (c) 2026 Kenneth Stott
# Canary: 9cbebdc0-4125-4511-801d-b79d35074070
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's functions as compiler pushdown (REQ-2167, FUNCTIONS ARE COMPILER PUSHDOWN): a macro
is a per-dialect rewrite of a function a dialect does not have, applied at transpile time, so every
transport and every engine that can run it gets it.

The rule (REQ-2171, THE RULE FOR TYPES AND FUNCTIONS, THE SAME TYPE UNDER ANOTHER NAME) is here and
nowhere else, in this order: :class:`Step`. ``Macro.resolved`` is the one place it is applied:

1. the dialect's own function, under any name (``own``, and PostgreSQL has every one of them);
2. the functions the dialect does have (``forms``);
3. its inline operators (``operators``);
4. otherwise the call is refused by name, never sent to fail as an engine error.

``MACROS`` is the one table: a row is a function's overload and its forms, written by family, each
family in its own module of provisa.transpiler.rows, which collects them. A form is SQL over the
overload's parameters, in the target's own dialect, or in PostgreSQL's (``Pg``) where sqlglot writes
one rewrite for each dialect listed. An overload is chosen by the type the statement says of its
first argument (``OVERLOADS``), never by the engine's type: PostgreSQL's own name for it, as pg_proc
has it, is its row. The row of the function's own name answers an argument whose type the statement
does not say.

A DuckDB form that names a ``provisa_`` function calls a Python UDF of
provisa.federation.duckdb_pg_scalars: that function is DuckDB's alone until it is written as SQL."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import functools
import itertools
import re
import string
from collections.abc import Callable, Iterable
from dataclasses import replace

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import MARK, PostgresError
from provisa.core.pg_range import (
    MULTIRANGE_TYPES,
    RANGE_TYPES,
    parse_multirange,
    parse_range,
    range_text,
)
from provisa.transpiler import (
    engine_refusals,
    polyfill,
    pg_forms,
    pg_forms_aggregate,
    pg_forms_geometric,
    pg_forms_hash,
    pg_forms_index,
    pg_forms_json,
    pg_forms_system,
    pg_forms_textsearch,
    pg_forms_types,
    rows,
)
from provisa.transpiler.rows.arrays import _ARRAY_OPERATOR_ROWS
from provisa.transpiler.rows.base import (
    _CLICKHOUSE,
    _DUCKDB,
    _TRINO,
    MAPPED,
    NATIVE,
    Forms,
    Macro,
    Pg,
    Step,
)
from provisa.transpiler.rows.hashes import _BYTEA_INPUT, _TEXT_TYPES
from provisa.transpiler.rows.numbers import _EXACT_OVERLOADS, _EXACT_ROWS, _REGRESSIONS
from provisa.transpiler.rows.system import VALUE_FUNCTIONS_OF_THE_SERVER
from provisa.transpiler.polyfill import array_dimensions as polyfill_array_dimensions
from provisa.transpiler.polyfill import notation as polyfill_notation
from provisa.transpiler.polyfill import texts as polyfill_texts
from provisa.transpiler.polyfill import trino as polyfill_trino
from provisa.transpiler.polyfill.numbers import ch_fail
from provisa.transpiler.polyfill.regex import said_text
from provisa.transpiler.pg_literals import literal_input as postgres_literal
from provisa.transpiler.pg_literals import values_cast


class FunctionRefused(ValueError):
    """A call of a function the routed dialect neither has nor has a form of: the client receives
    PostgreSQL's SQLSTATE for a feature it lacks, 0A000 feature_not_supported (REQ-2236; a grid
    row's refusal states its own, RowRefused)."""

    code = "query.function_unsupported"
    sqlstate = "0A000"

    def __init__(self, function: str, dialect: str) -> None:
        self.params = {"function": function, "dialect": dialect}
        super().__init__(f"{function}() has no form on {dialect}")


class RowRefused(FunctionRefused):
    """A call the polyfill grid's row refuses on the routed engine (REQ-2236, THE REFUSAL LIVES IN
    THE GRID): the client receives the row's SQLSTATE and message."""

    def __init__(self, function: str, dialect: str, refusal: polyfill.Refusal) -> None:
        super().__init__(function, dialect)
        self.sqlstate = refusal.sqlstate
        self.args = (refusal.message,)


#: A row replay found another answer of, by the dialects it is refused on.
_DIFFERS_ON: dict[str, frozenset[str]] = {
    # Two of its times differ by seconds, not an interval.
    "time_mi_time": _CLICKHOUSE,
    # Its decimals overflow on the form's modulus and its digits of a pg_lsn are not read back.
    "numeric_pl_pg_lsn": _CLICKHOUSE,
    "pg_lsn_mii": _CLICKHOUSE,
    "pg_lsn_pli": _CLICKHOUSE,
    # Two timestamps differ by seconds there, and by Trino's own text of an interval.
    "timestamp_mi": _CLICKHOUSE | _TRINO,
    "timestamptz_mi": _CLICKHOUSE | _TRINO,
    # T-SQL has no array, and reads a form's a[1] as a name in brackets.
    **dict.fromkeys(pg_forms_aggregate.ARRAY_INPUT, frozenset({"tsql"})),
}
#: A dialect's form where the generated one's arithmetic does not hold: Int64 wraps as the sum does.
_WRAPPING_SUM = Pg(
    "CASE WHEN a IS NULL THEN CAST(b AS BIGINT) WHEN b IS NULL THEN CAST(a AS BIGINT) "
    "ELSE CAST(a AS BIGINT) + CAST(b AS BIGINT) END"
)
_ENGINE_FORMS: dict[str, Forms] = {
    "int2_sum": {_CLICKHOUSE: _WRAPPING_SUM},
    "int4_sum": {_CLICKHOUSE: _WRAPPING_SUM},
}


def _with_replay(macro: Macro) -> Macro:
    differs = macro.differs_on | _DIFFERS_ON.get(macro.name, frozenset())
    forms = {**_ENGINE_FORMS.get(macro.name, {}), **macro.operators}
    return replace(macro, differs_on=differs, operators=forms)


#: The rows of no form: what a dialect's answer differs in and no form writes.
REFUSED_ROWS = frozenset(
    {
        ("exact_avg_interval", 1),
        ("current_query", 0),
        *((name, len(params.split(", "))) for name, params in pg_forms_hash.COMPOSITES),
        # A shape's name of one argument whose type the statement does not say: no overload.
        *((name, 1) for name, _ in pg_forms_geometric.BARE),
    }
)


#: A call's name and argument count -> its overload: the one table.
MACROS: dict[tuple[str, int], Macro] = {(m.name, len(m.params)): _with_replay(m) for m in rows.ROWS}
#: (function, argument count, the type the statement says of its first argument) -> its row:
#: PostgreSQL's own name for the overload. The operation is chosen by the type, not the engine's.
OVERLOADS: dict[tuple[str, int, str], str] = {
    **pg_forms_system.OVERLOADS,
    ("area", 1, "box"): "box_area",
    ("area", 1, "path"): "path_area",
    ("area", 1, "circle"): "circle_area",
    ("center", 1, "box"): "box_center",
    ("center", 1, "circle"): "circle_center",
    ("length", 1, "lseg"): "lseg_length",
    ("length", 1, "path"): "path_length",
    ("length", 1, "tsvector"): "tsvector_length",
    ("unnest", 1, "tsvector"): "tsvector_unnest",
    ("strip", 1, "tsvector"): "tsvector_strip",
    ("abbrev", 1, "inet"): "inet_abbrev",
    ("abbrev", 1, "cidr"): "cidr_abbrev",
    ("set_masklen", 2, "cidr"): "cidr_set_masklen",
    **{
        (name, 1, of): f"{of_row}_{name}"
        for name in ("int2", "int4", "int8", "float4", "float8")
        for of, of_row in (("decimal", "numeric"), ("jsonb", "jsonb"))
    },
    ("numeric", 1, "jsonb"): "jsonb_numeric",
    **{
        (name, 1, of): f"{row}to{'i' + name[3:]}"
        for name in ("int2", "int4", "int8")
        for of, row in (("double precision", "d"), ("float", "f"), ("real", "f"))
    },
    ("int4", 1, "boolean"): "bool_int4",
    ("avg", 1, "real"): "float4_avg",
    ("avg", 1, "float"): "float4_avg",
    ("round", 1, "double precision"): "dround",
    ("trunc", 1, "double precision"): "dtrunc",
    **{(name, 1, "decimal"): f"numeric_{name}" for name in ("ceil", "floor", "round", "trunc")},
    ("md5", 1, "bytea"): "md5_bytea",
    **pg_forms_geometric.OVERLOADS,
    **pg_forms_hash.OVERLOADS,
    ("int4", 1, "bit"): "bittoint4",
    ("int8", 1, "bit"): "bittoint8",
    ("int4", 1, '"char"'): "chartoi4",
    ("time", 1, "interval"): "interval_time",
    ("time", 1, "timestamp"): "timestamp_time",
    ("time", 1, "timestamptz"): "timestamptz_time",
    ("time", 1, "timetz"): "timetz_time",
    ("time", 2, "time"): "time_scale",
    ("timestamp", 2, "date"): "datetime_pl",
    ("timestamp", 1, "timestamptz"): "timestamptz_timestamp",
    ("time", 2, "date"): "datetime_pl",
    ("timestamp", 2, "timestamp"): "timestamp_scale",
    ("timestamptz", 2, "timestamptz"): "timestamptz_scale",
    ("timetz", 1, "timestamptz"): "timestamptz_timetz",
    ("timetz", 1, "time"): "time_timetz",
    ("interval", 1, "time"): "time_interval",
    ("interval", 2, "interval"): "interval_scale",
    **_EXACT_OVERLOADS,
    ("round", 1, "real"): "dround",
    ("round", 2, "decimal"): "round_numeric",
    ("trunc", 2, "decimal"): "trunc_numeric",
    ("text", 1, "inet"): "inet_text",
    **{
        (name, 1, of): f"{name}_collated"
        for name in ("max", "min")
        for of in ("text", "char", "bpchar", "varchar")
    },
    **{
        (name, arity, of): f"{of}_{suffix}"
        for of in ("lseg", "line")
        for name, arity, suffix in (
            ("ishorizontal", 1, "horizontal"),
            ("isvertical", 1, "vertical"),
            ("isparallel", 2, "parallel"),
            ("isperp", 2, "perp"),
        )
    },
}
#: The calls of no row of the table, which ``inline`` writes itself.
_COUNTS_NAMES = (
    "num_nulls", "num_nonnulls", "rank", "dense_rank", "percent_rank", "cume_dist",
    "percentile_cont", "percentile_disc", "ntile", "mode",
)  # fmt: skip
#: An overload's row -> the function a statement called, which names its column.
_CALLED = {
    **{row: name for (name, _, _), row in OVERLOADS.items()},
    **{row: "encode" for row in ("encode_hex", "encode_base64", "encode_escape")},
    "regexp_replace_at": "regexp_replace",
    "pg_to_hex": "to_hex",
}
#: How a statement already written through sqlglot spells a row's call, and how a client's spells
#: one pg_syntax reads by its routine's name (overlaps, overlaps_timestamp's).
_SPELLED = {
    "encode", "substring", "regexp_replace", "array_concat", "to_number",
    "date_trunc", "substr", "left", "right", "overlaps",
}  # fmt: skip
_NAMES = (
    frozenset(name for name, _ in MACROS)
    | {name for name, _, _ in OVERLOADS}
    | _SPELLED
    | set(_COUNTS_NAMES)
    | engine_refusals.NAMES
)


def _called_by_name(names: Iterable[str]) -> str:
    """A call of one of ``names``, by its name, quoted or not."""
    return r"\b(" + "|".join(sorted(names)) + r")\"?\s*\("


_MENTIONED = re.compile(
    _called_by_name(_NAMES | pg_forms_textsearch.NAMES)
    + f"|{pg_forms_json.OPERATORS}|{pg_forms_textsearch.OPERATORS}",
    re.IGNORECASE,
)


@functools.cache
def _grid_mentioned(dialect: str) -> re.Pattern[str] | None:
    """A call of a function a grid row writes on ``dialect``, or what a rewrite of its column
    mentions: a row that leaves the engine's own call there (array_agg on Trino, whose row is
    ClickHouse's; ::text, which a ClickHouse rewrite mentions) does not walk the statement."""
    names = polyfill.GRID.names_on(dialect)
    held = [_called_by_name(names)] if names else []
    held.extend(polyfill.GRID.mentions_on(dialect))
    return re.compile("|".join(held), re.IGNORECASE) if held else None


#: A call by pg_catalog, or a value of a type a call of it may be refused for.
_QUALIFIED = re.compile(r"\bpg_catalog\s*\.\s*\"?\w+\"?\s*\(|::\s*interval\b", re.IGNORECASE)
#: The names provisa.pgwire.catalog answers where a statement reads no table.
SERVER_VALUE_NAMES = frozenset(m.name for m in VALUE_FUNCTIONS_OF_THE_SERVER)


#: Functions whose overloads differ by a type every dialect here holds as the same text: only the
#: statement can tell them apart. An unknown-typed literal is the first, as PostgreSQL resolves it.
_TOLD_ONLY_BY_THE_STATEMENT = {"set_masklen": frozenset({"inet", "cidr"})}


def _is_told(name: str, args: list[exp.Expr]) -> bool:
    """Whether a call says which overload it means; a column of cidr does not, and the inet row
    would keep what its mask clears (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED)."""
    if name in pg_forms_system.OF_A_STORED_VALUE:
        return args[0].find(exp.Column) is None
    types = _TOLD_ONLY_BY_THE_STATEMENT.get(name)
    if types is None:
        return True
    return said_type(args[0]) in types or isinstance(args[0].unnest(), (exp.Literal, exp.Null))


def said_type(node: exp.Expr) -> str | None:
    """The PostgreSQL type the statement says an argument is; None where it does not."""
    if isinstance(node, exp.Paren):
        return said_type(node.this)
    if isinstance(node, exp.Column):
        said = values_cast(node)
        return None if said is None else said_type(said)
    return node.to.sql(dialect="postgres").lower() if isinstance(node, exp.Cast) else None


#: The rows of a type whose NULL answers NULL, whatever digits the function would give a value.
_ANSWERED_BY_A_NULL = frozenset(m.name for m in _EXACT_ROWS)
_COLLATED = frozenset({"max_collated", "min_collated"})


def _bucket_said(value: exp.Expr) -> str | None:
    """What width_bucket's resolution reads of an argument; an untyped number's literal says it."""
    node = value.unnest()
    node = node.this if isinstance(node, exp.Neg) else node
    written = isinstance(node, exp.Literal) and not node.is_string and not node.is_int
    return said_type(value) or ("decimal" if written else None)


def _bucket_row(args: list[exp.Expr]) -> str:
    """width_bucket's row: PostgreSQL picks numeric or float8 from all three values, and float8 of
    integers. What no statement says of a column, the engine's type answers in the row of the name."""
    if len(args) == 2:
        nested = isinstance(args[1].unnest(), exp.Array) and any(
            isinstance(item, exp.Array) for item in args[1].unnest().expressions
        )
        return (
            pg_forms_geometric.OVERLOADS[("width_bucket", 2, pg_forms_geometric.NESTED)]
            if (nested)
            else "width_bucket"
        )
    said = {_bucket_said(a) for a in args[:3]}
    if said & {"double precision", "real"}:
        return "width_bucket_float8"
    if any(s and s.startswith("decimal") for s in said if s):
        told = all(a.find(exp.Column) is None for a in args[:3]) or any(
            (said_type(a) or "").startswith("decimal") for a in args[:3]
        )
        if told:
            return "width_bucket_numeric"
    return "width_bucket" if any(a.find(exp.Column) for a in args[:3]) else "width_bucket_float8"


def overload(name: str, args: list[exp.Expr]) -> str:
    """The row a call reads: the overload of the type its first argument is said to be, else the
    row of its own name."""
    if name == "width_bucket" and len(args) in (2, 4):
        return _bucket_row(args)
    said = said_type(args[0]) if args else None
    if said is None:
        return name
    row = OVERLOADS.get((name, len(args), said.split("(")[0]), name)
    if row in _ANSWERED_BY_A_NULL and isinstance(args[0].unnest().this, exp.Null):
        return name
    if row in _COLLATED and args[0].find(exp.Column) is None:
        return name  # one value, which no order changes
    return row


def outcome(name: str, arity: int, dialect: str) -> str:
    """What a call does on ``dialect``: native, mapped, emulated or refused."""
    step, _ = MACROS[(name, arity)].resolved(dialect)
    if dialect in NATIVE:
        return "native"
    if step is Step.REFUSED:
        return "refused"
    return "mapped" if step is Step.OWN else "emulated"


def mentioned(sql: str, dialect: str) -> bool:
    """Whether ``sql`` may call a function of the table that ``dialect`` does not have itself, or
    names one by pg_catalog; a statement that does neither is not parsed."""
    if dialect in NATIVE:
        return False
    grid = _grid_mentioned(dialect)
    return (
        _MENTIONED.search(sql) is not None
        or _QUALIFIED.search(sql) is not None
        or (grid is not None and grid.search(sql) is not None)
    )


@functools.lru_cache(maxsize=2048)
def _parsed(name: str, arity: int, dialect: str) -> exp.Expr:
    _, form = MACROS[(name, arity)].resolved(dialect)
    assert isinstance(form, str)
    parsed = sqlglot.parse_one(form, read="postgres" if isinstance(form, Pg) else dialect)
    return parsed.transform(_thrown) if dialect == "clickhouse" else parsed


def _thrown(node: exp.Expr) -> exp.Expr:
    """A refusal on ClickHouse: it reads a bad text cast to a Nullable as NULL, and folds a cast
    of a constant in every branch of a CASE, where throwIf is left to the branch that runs. It has
    no NULL of an array either, and a column of one is never NULL."""
    if not isinstance(node, exp.Cast):
        return node
    text = node.this
    if isinstance(text, exp.Null) and node.to.is_type("array"):
        return exp.Cast(this=exp.Array(expressions=[]), to=node.to)
    if not isinstance(text, exp.Cast) or not text.is_type(*exp.DataType.INTEGER_TYPES):
        return node
    if MARK not in text.this.sql():
        return node
    return exp.Cast(
        this=exp.Anonymous(this="throwIf", expressions=[exp.Literal.number(1), text.this]),
        to=node.to,
    )


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


#: The types a row of an overload reads as their text, which is how a dialect without them holds
#: them: once the type has chosen the row, a cast to it says nothing more.
_HELD_AS_TEXT = frozenset(
    {
        "point", "line", "lseg", "box", "path", "polygon", "circle", "tsvector", "inet", "cidr",
        "macaddr", "macaddr8", "pg_lsn", "cid", "record", "cstring",
    }
)  # fmt: skip


#: A text padded on the right to a count of characters, by dialect: ClickHouse's rpad counts bytes.
_PADDED_BY_CHARACTERS = {"clickhouse": "rightPadUTF8"}


def _argument(value: exp.Expr, held: bool = False, dialect: str = "") -> exp.Expr:
    size = _padded_to(value) if held else None
    if said_type(value) in _HELD_AS_TEXT or (held and isinstance(value.unnest(), exp.Cast)):
        value = value.unnest().this
    if size is not None:  # bpchar's input pads to its length, which the text does not hold
        pad = _PADDED_BY_CHARACTERS.get(dialect, "rpad")
        value = exp.Anonymous(this=pad, expressions=[value, size, exp.Literal.string(" ")])
    return value if isinstance(value, _ATOMIC) else exp.Paren(this=value)


def _padded_to(value: exp.Expr) -> exp.Expr | None:
    """The length a cast to character(n) says."""
    node = value.unnest()
    if isinstance(node, exp.Cast) and node.to.is_type("char"):
        # character alone is character(1).
        return node.to.expressions[0].this if node.to.expressions else exp.Literal.number(1)
    return None


def _in_a_subquery(form: exp.Expr, params: tuple[str, ...], args: list[exp.Expr]) -> exp.Expr:
    """``form`` over its parameters as the columns of a row of its arguments: a lambda cannot read
    a subquery, and a form may be one."""
    row = exp.select(*(exp.alias_(a, p) for p, a in zip(params, args)))
    return exp.select(form).from_(row.subquery("_args")).subquery()


#: What answers differently each time it is evaluated (PostgreSQL's pg_proc.provolatile).
_VOLATILE = frozenset(
    {
        "array_sample", "array_shuffle", "clock_timestamp", "currval", "gen_random_uuid", "lastval",
        "nextval", "random", "random_normal", "setval", "timeofday",
    }
)  # fmt: skip


def _is_volatile(value: exp.Expr) -> bool:
    return any(
        isinstance(node, (exp.Rand, exp.Uuid))
        or (isinstance(node, exp.Anonymous) and node.name.lower() in _VOLATILE)
        for node in value.walk()
    )


def _repeats_volatile(named: list[str], params: tuple[str, ...], args: list[exp.Expr]) -> bool:
    """Whether a form names twice (``named``: each parameter it names, as often as it does) a
    parameter whose argument differs each time it is evaluated."""
    return any(named.count(p) > 1 and _is_volatile(a) for p, a in zip(params, args))


def _held_once(
    form: exp.Expr, params: tuple[str, ...], args: list[exp.Expr], node: exp.Expr, name: str,
    dialect: str,
) -> exp.Expr:  # fmt: skip
    """``form`` over a row of its arguments, each evaluated once. A row of arguments naming no
    column is the same row for every row of a FROM, so that call is refused by name rather than
    answered with one value for all of them."""
    select = node.find_ancestor(exp.Select)
    reads_rows = select is not None and select.args.get("from_") is not None
    if reads_rows and not any(a.find(exp.Column) is not None for a in args):
        raise FunctionRefused(name, dialect)
    return _in_a_subquery(form, params, args)


def _written(macro: Macro, args: list[exp.Expr], dialect: str, node: exp.Expr) -> exp.Expr:
    """The call's form with each parameter its argument. An argument is written wherever the form
    names it, and evaluated each time; one that differs each time is held once (REQ-2171, REWRITE
    AFTER ROUTING). A row of arguments naming no column is the same row for every row of a FROM,
    so that call is refused by name rather than answered with one value for all of them."""
    form = _parsed(macro.name, len(args), dialect).copy()
    held = macro.name in pg_forms_types.HELD | pg_forms_hash.HELD | pg_forms_index.HELD
    named = [c.name for c in form.find_all(exp.Column) if not c.table]
    if _repeats_volatile(named, macro.params, args):
        each = [_argument(a, held, dialect) for a in args]
        return _held_once(form, macro.params, each, node, macro.name, dialect)
    if form.find(exp.Lambda) is not None and any(a.find(exp.Select) is not None for a in args):
        return _in_a_subquery(form, macro.params, [_argument(a) for a in args])
    values = dict(zip(macro.params, args))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name], held, dialect).copy()
        return node

    return form.transform(bound)


@functools.lru_cache(maxsize=512)
def _replacement(form: str, dialect: str, pattern: str) -> exp.Expr:
    """A grid rule's replacement on ``dialect``, as a form here is read: on ClickHouse, with its
    refusals thrown (:func:`_thrown`). The tree is shared: graft copies it."""
    tree = polyfill_notation.replacement(form, polyfill.written_in(dialect), pattern)
    return tree.transform(_thrown) if dialect == "clickhouse" else tree


def _grafted(
    replacement: exp.Expr, params: tuple[str, ...], args: list[exp.Expr], node: exp.Expr,
    name: str, dialect: str,
) -> exp.Expr:  # fmt: skip
    """A grid rule's replacement with each hole its argument, as :func:`_written` binds a form."""
    bound = [_argument(a) for a in args]
    named = [h for h in map(polyfill.hole_name, replacement.walk()) if h is not None]
    once = _repeats_volatile(named, params, args) or (
        replacement.find(exp.Lambda) is not None and any(a.find(exp.Select) for a in args)
    )
    if once:
        over_columns = polyfill.graft(replacement, {p: exp.column(p) for p in params})
        return _held_once(over_columns, params, bound, node, name, dialect)
    return polyfill.graft(replacement, dict(zip(params, bound)))


def _polyfilled(
    target: exp.Expr, node: exp.Expr, dialect: str, rewrite: Callable[[exp.Expr], exp.Expr]
) -> exp.Expr | None:
    """A call a rule of the polyfill grid matches (REQ-2236), written in the routed engine's form;
    None where no rule matches. A dialect that is no column of the grid has no form of it, and the
    call is refused by name. A rewrite function's refusal of a value is raised as the row's."""
    try:
        return _polyfilled_by_rule(target, node, dialect, rewrite)
    except polyfill_notation.Refused as refused:
        called = target.name.lower() if isinstance(target, exp.Anonymous) else _feature_of(target)
        raise RowRefused(called, dialect, refused.refusal) from refused


def _feature_of(target: exp.Expr) -> str:
    """The name of the grid's feature a typed node's rule is of."""
    names = type(target).sql_names() if isinstance(target, exp.Func) else ()
    return names[0].lower() if names else type(target).__name__.lower()


def _polyfilled_by_rule(
    target: exp.Expr, node: exp.Expr, dialect: str, rewrite: Callable[[exp.Expr], exp.Expr]
) -> exp.Expr | None:
    """:func:`_polyfilled`, a rewrite function's refusal raised as it is."""
    applied = polyfill.GRID.read(target, dialect)
    if applied is None:
        return None
    called = target.name.lower() if isinstance(target, exp.Anonymous) else applied.feature.name
    form = applied.form
    if polyfill.column_of(dialect) is None:
        if applied.rule.unlisted_keep:
            return None  # the row polyfills its columns only: this dialect keeps what it had
        raise FunctionRefused(called, dialect)
    if form is None:
        return node  # the engine's own, as the statement writes it (by pg_catalog on DuckDB)
    if isinstance(form, polyfill.Refusal):
        raise RowRefused(called, dialect, form)
    if isinstance(form, polyfill.Rewrite):
        if form.function is polyfill_trino.kept:
            # The old table's form: no argument is read here, as each would be read again by the
            # old table's walk (twice a level of a nested call: abs of abs of abs).
            return None
        written = applied.rewritten
        if written is None:
            # A rule's own pattern: its arguments in their forms first, as a form's holes are
            # bound, since the walk does not descend into what a rule writes.
            argued = target.copy()
            # The copy reads where the call stands (a window's, a filter's), as the call does.
            argued.parent, argued.arg_key = target.parent, target.arg_key
            for argument in list(argued.iter_expressions()):
                argument.replace(argument.transform(rewrite))
            written = form.function(argued, dialect)
        if written is None:
            return None
    else:
        assert applied.rule.pattern is not None
        params = applied.rule.holes()
        args = [applied.bindings[p].transform(rewrite) for p in params]
        replacement = _replacement(form, dialect, applied.rule.pattern)
        written = _grafted(replacement, params, args, node, called, dialect)
    if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
        return exp.alias_(written, called)  # named for the function
    return written


#: The functions that count the NULLs of any number of arguments: which of the two a form answers.
_COUNTS = {"num_nulls": "IS NULL", "num_nonnulls": "IS NOT NULL"}


def _counted(name: str, args: list[exp.Expr], dialect: str) -> exp.Expr:
    """num_nulls and num_nonnulls, of arguments or of an array's elements (VARIADIC)."""
    test = _COUNTS[name]
    values = args
    if len(args) == 1 and isinstance(args[0], exp.Variadic):
        array = args[0].this
        if isinstance(array, exp.Array):
            values = list(array.expressions)
        elif dialect in _DUCKDB:
            written = f"CAST(len(list_filter(a, _x -> _x {test})) AS INTEGER)"
            bound = sqlglot.parse_one(written, read="duckdb")
            return bound.transform(lambda n: array.copy() if n == exp.column("a") else n)
        else:
            raise FunctionRefused(name, dialect)
    terms = " + ".join(
        f"CASE WHEN ({v.sql(dialect='postgres')}) {test} THEN 1 ELSE 0 END" for v in values
    )
    return sqlglot.parse_one(f"CAST({terms or '0'} AS INTEGER)", read="postgres")


#: What a hypothetical-set aggregate answers of a row placed among the rows, over ``ahead``, the
#: rows sorting before it, ``peers``, those level with it, and ``groups``, the sets of level rows
#: before it.
_HYPOTHETICAL = {
    "rank": "CAST(1 + {ahead} AS BIGINT)",
    "dense_rank": "CAST(1 + {groups} AS BIGINT)",
    "percent_rank": (
        "CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE PRECISION) "
        "ELSE CAST({ahead} AS DOUBLE PRECISION) / COUNT(*) END"
    ),
    "cume_dist": "CAST({ahead} + {peers} + 1 AS DOUBLE PRECISION) / (COUNT(*) + 1)",
}


def _ordered_set_name(node: exp.WithinGroup) -> str:
    call = node.this
    return call.name.lower() if isinstance(call, exp.Anonymous) else call.sql_name().lower()


def _hypothetical(node: exp.WithinGroup, dialect: str) -> exp.Expr:
    """rank(v) WITHIN GROUP (ORDER BY k), which no engine has, as the counts it is. Of one sort key:
    a row of several compares its keys in turn, which no form here holds."""
    name = _ordered_set_name(node)
    direct, keys = node.this.expressions, node.expression.expressions
    if len(direct) != 1 or len(keys) != 1:
        raise FunctionRefused(name, dialect)
    key, value = (f"({e.sql(dialect='postgres')})" for e in (keys[0].this, direct[0]))
    sooner = ">" if keys[0].args.get("desc") else "<"
    nulls = (key, value) if keys[0].args.get("nulls_first") else (value, key)
    before = f"({key} {sooner} {value} OR ({nulls[0]} IS NULL AND {nulls[1]} IS NOT NULL))"
    level = f"(({key} = {value}) OR ({key} IS NULL AND {value} IS NULL))"
    ahead = f"COALESCE(SUM(CASE WHEN {before} THEN 1 ELSE 0 END), 0)"
    peers = f"COALESCE(SUM(CASE WHEN {level} THEN 1 ELSE 0 END), 0)"
    # COUNT(DISTINCT) leaves out NULL, which sorts as one group where it is before the row.
    groups = (
        f"(COUNT(DISTINCT CASE WHEN {before} THEN {key} END) + "
        f"COALESCE(MAX(CASE WHEN {before} AND {key} IS NULL THEN 1 ELSE 0 END), 0))"
    )
    written = _HYPOTHETICAL[name].format(ahead=ahead, peers=peers, groups=groups)
    return sqlglot.parse_one(written, read="postgres")


def _fractions(argument: exp.Expr) -> list[float | None]:
    """The fractions a statement's percentile says, a NULL as None."""
    node = argument.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Array):
        return [value for item in node.expressions for value in _fractions(item)]
    if isinstance(node, exp.Literal) and node.is_string:  # an array's text: '{0.25,0.5}'
        items = node.this.strip().removeprefix("{").removesuffix("}").split(",")
        return [None if i.strip().upper() == "NULL" else float(i) for i in items if i.strip()]
    if isinstance(node, exp.Null):
        return [None]
    if isinstance(node, exp.Neg):
        return [None if v is None else -v for v in _fractions(node.this)]
    if not isinstance(node, exp.Literal):  # a bound parameter: the engine checks its value
        return []
    return [float(node.sql(dialect="postgres"))]


#: DuckDB takes a negative fraction as its sort descending, and 0 has none: a smallest positive
#: fraction is the same row.
_FIRST_DESCENDING = 5e-324


def _of_intervals(node: exp.WithinGroup, fractions: list[float], array: bool) -> exp.Expr:
    """DuckDB's percentile_cont of intervals interpolates microseconds, carrying months and days
    into hours: as PostgreSQL's, each fraction's two neighbours among the keys sorted as
    PostgreSQL compares intervals, and interval_lerp between them where they differ."""
    ordered = node.expression.expressions[0]
    key = ordered.this.sql(dialect="duckdb")
    direction = " DESC" if ordered.args.get("desc") else ""
    place = "(_f * (_n - 1))"
    lo = f"_k[CAST(floor({place}) AS BIGINT) + 1]"
    hi = f"_k[CAST(ceil({place}) AS BIGINT) + 1]"
    each = (
        f"CASE WHEN ceil({place}) = floor({place}) THEN {lo} "
        f"ELSE {pg_forms.interval_lerp(lo, hi, f'{place} - floor({place})')} END"
    )
    said = ", ".join(map(repr, fractions))
    answer = (
        f"list_transform([{said}], _f -> {each})" if array else pg_forms.bound("_f", said, each)
    )
    written = pg_forms.bound(
        "_k",
        f"list({key} ORDER BY {pg_forms.interval_span(key)}{direction}) "
        f"FILTER (WHERE {key} IS NOT NULL)",
        pg_forms.bound("_n", f"count({key})", f"CASE WHEN _n = 0 THEN NULL ELSE {answer} END"),
    )
    return sqlglot.parse_one(written, read="duckdb")


#: The rows' keys, sorted, bound to ``_s``, and each fraction, bound to ``_f`` where there are
#: several: Trino's approx_percentile and ClickHouse's quantile answer approximately.
_EXACT = {
    "trino": {
        "sorted": "array_sort(filter(array_agg({k}), _e -> _e IS NOT NULL))",
        "descending": "reverse({sorted})",
        # No keys answer NULL, as PostgreSQL does: the line between two of none would read
        # element_at(_s, 0), which Trino refuses ("SQL array indices start at 1"). Only NULL keys
        # leave _s empty; no rows leave it NULL (array_agg over none), whose cardinality is NULL.
        "bound": (
            "element_at(transform(ARRAY[{sorted}], "
            "_s -> CASE WHEN coalesce(cardinality(_s), 0) = 0 THEN NULL ELSE {answer} END), 1)"
        ),
        "each": "transform(ARRAY[{fractions}], _f -> {answer})",
        "disc": "element_at(_s, CAST(greatest(ceiling({f} * cardinality(_s)), 1) AS BIGINT))",
        "cont": (
            "element_at(_s, {low}) + (element_at(_s, {high}) - element_at(_s, {low})) * "
            "({place} - {low})"
        ),
        "place": "({f} * (cardinality(_s) - 1) + 1)",
        "low": "CAST(floor({place}) AS BIGINT)",
        "high": "CAST(ceiling({place}) AS BIGINT)",
        "number": "CAST({k} AS DOUBLE)",
    },
    "clickhouse": {
        "sorted": "arraySort(groupArray({k}))",
        "descending": "arrayReverseSort(groupArray({k}))",
        # No keys answer NULL. An array of fractions' answers over none raises by name instead:
        # ClickHouse holds no Nullable(Array), and a server without Variant types (24.3, the
        # integration lanes') has no common type of NULL and an array (polyfill.clickhouse
        # PERCENTILE states the limitation).
        "bound": "arrayElement(arrayMap(_s -> if(length(_s) = 0, NULL, {answer}), [{sorted}]), 1)",
        "bound array": (
            "arrayElement(arrayMap(_s -> if({no_keys} = 0, {answer}, []), [{sorted}]), 1)"
        ),
        "each": "arrayMap(_f -> {answer}, [{fractions}])",
        "disc": "_s[toUInt64(greatest(ceil({f} * length(_s)), 1))]",
        "cont": "_s[{low}] + (_s[{high}] - _s[{low}]) * ({place} - {low})",
        "place": "({f} * (length(_s) - 1) + 1)",
        "low": "toUInt64(floor({place}))",
        "high": "toUInt64(ceil({place}))",
        "number": "toFloat64({k})",
    },
}


#: ClickHouse's refusal of an array of fractions' percentiles over no keys (polyfill.clickhouse
#: PERCENTILE).
_NO_KEYS = "percentile of an array of fractions over no keys does not read this value"


def _exact(node: exp.WithinGroup, fractions: list[float], array: bool, dialect: str) -> exp.Expr:
    """A percentile from the sorted keys, as PostgreSQL's: the first key past the fraction, or the
    line between the two either side of it."""
    forms = _EXACT["trino" if dialect in _TRINO else dialect]
    ordered = node.expression.expressions[0]
    interpolated = isinstance(node.this, exp.PercentileCont)
    if interpolated and said_type(ordered.this) == "interval":
        raise FunctionRefused(node.this.sql_name().lower(), dialect)
    key = ordered.this.sql(dialect=dialect)
    key = forms["number"].format(k=key) if interpolated else key
    sorted_ = forms["sorted"].format(k=key)
    if ordered.args.get("desc"):
        sorted_ = forms["descending"].format(sorted=sorted_, k=key)

    def answer(fraction: str) -> str:
        if not interpolated:
            return forms["disc"].format(f=fraction)
        place = forms["place"].format(f=fraction)
        return forms["cont"].format(
            place=place,
            low=forms["low"].format(place=place),
            high=forms["high"].format(place=place),
        )

    each = (
        forms["each"].format(fractions=", ".join(map(repr, fractions)), answer=answer("_f"))
        if array
        else answer(repr(fractions[0]))
    )
    bound = forms["bound array"] if array and "bound array" in forms else forms["bound"]
    no_keys = ch_fail("0A000", _NO_KEYS)
    written = bound.format(
        sorted=sorted_, answer=each, no_keys=no_keys.format(condition="length(_s) = 0")
    )
    return sqlglot.parse_one(written, read=dialect)


#: An element of an array, counted from 1, as each dialect reads one.
_ELEMENT = {"trino": "element_at", "clickhouse": "arrayElement", "duckdb": "list_extract"}


def _in_place(written: exp.Expr, fractions: list[float | None], dialect: str) -> exp.Expr:
    """``written``, the answers of the fractions that are not NULL, with each NULL fraction's NULL
    back in its place, as PostgreSQL answers an array that holds one: a NULL of the aggregate's
    type over the same rows."""
    element = _ELEMENT["trino" if dialect in _TRINO else dialect]

    def at(index: int) -> exp.Expr:
        return exp.Anonymous(this=element, expressions=[written.copy(), exp.Literal.number(index)])

    places = itertools.count(1)
    return exp.Array(
        expressions=[
            exp.Case(ifs=[exp.If(this=exp.false(), true=at(1))])
            if fraction is None
            else at(next(places))
            for fraction in fractions
        ]
    )


def _percentile(node: exp.WithinGroup, dialect: str) -> exp.Expr:
    """percentile_cont and percentile_disc as PostgreSQL has them: a NULL fraction answers NULL of
    the aggregate's type over the same rows, in its place in an array of them, and one outside 0
    to 1 is refused."""
    argument = node.this.this
    array = isinstance(argument.unnest(), exp.Array) or (
        isinstance(argument.unnest(), exp.Cast) and argument.unnest().is_type("array")
    )
    named = "percentile_cont" if isinstance(node.this, exp.PercentileCont) else "percentile_disc"
    fractions = _fractions(argument)
    if not fractions and not array:  # bound: DuckDB's own percentile checks the value it is given
        if dialect in _DUCKDB:
            return node
        raise FunctionRefused(named, dialect)
    for fraction in fractions:
        if fraction is not None and not 0 <= fraction <= 1:
            raise PostgresError("22003", f"percentile value {fraction:g} is not between 0 and 1")
    held = argument.unnest()
    while isinstance(held, exp.Cast):
        held = held.this.unnest()
    whole = isinstance(held, exp.Null)  # the fraction, or the array of them, is NULL
    # REQ-2167: let ClickHouse return its own engine error for a NULL array fraction rather than
    # raising FunctionRefused (commit 4c510070b2 changed MATRIX["clickhouse"] to expect "error").
    placed = "trino" if dialect in _TRINO else dialect
    if None in fractions and not whole and placed not in _ELEMENT:
        raise FunctionRefused(named, dialect)
    known = [fraction for fraction in fractions if fraction is not None] or [0.0]
    written = _of_the_known(node, known, array, dialect)
    if whole:
        return exp.Case(ifs=[exp.If(this=exp.false(), true=written)])
    return _in_place(written, fractions, dialect) if None in fractions else written


def _of_the_known(node: exp.WithinGroup, known: list[float], array: bool, dialect: str) -> exp.Expr:
    """The percentile of each fraction in ``known``, none of them NULL."""
    ordered = node.expression.expressions[0]
    if dialect in _TRINO or dialect == "clickhouse":
        return _exact(node, known, array, dialect)
    said = said_type(ordered.this) == "interval"
    if said and isinstance(node.this, exp.PercentileCont) and dialect in _DUCKDB:
        return _of_intervals(node, known, array)
    if dialect in _DUCKDB and ordered.args.get("desc"):
        known = [fraction or _FIRST_DESCENDING for fraction in known]
    numbers = [exp.Literal.number(repr(fraction)) for fraction in known]
    node.this.set("this", exp.Array(expressions=numbers) if array else numbers[0])
    return node


#: The functions in FROM with columns of their own, which DuckDB's unnest of a struct does not make.
_ROWS_OF_COLUMNS = frozenset(
    {"pg_options_to_table", "aclexplode", "pg_timezone_names", "pg_timezone_abbrevs"}
)


#: byteain's escapes: a doubled backslash, or a byte in three octal digits.
_BYTEA_ESCAPE = re.compile(r"\\\\|\\([0-3][0-7]{2})")


#: A class sqlglot reads a call of a function of the table as.
_READ_AS: dict[type[exp.Expr], str] = {
    exp.JustifyDays: "justify_days",
    exp.JustifyHours: "justify_hours",
    exp.JustifyInterval: "justify_interval",
    exp.MD5: "md5",
    exp.Acosh: "acosh",
    exp.Asinh: "asinh",
    exp.Atanh: "atanh",
    exp.Avg: "avg",
    exp.Ascii: "ascii",
    exp.BitLength: "bit_length",
    exp.Ceil: "ceil",
    exp.Cot: "cot",
    exp.Floor: "floor",
    exp.Max: "max",
    exp.Min: "min",
    exp.Exp: "exp",
    exp.Ln: "ln",
    exp.Sqrt: "sqrt",
    exp.Stddev: "stddev",
    exp.StddevPop: "stddev_pop",
    exp.StddevSamp: "stddev_samp",
    exp.Variance: "variance",
    exp.VariancePop: "var_pop",
}


_REGRESSIONS_READ: dict[type[exp.Expr], str] = {
    getattr(exp, class_name): macro.name
    for macro in _REGRESSIONS
    if hasattr(exp, class_name := "".join(part.title() for part in macro.name.split("_")))
}


def _counts_from_the_first(start: exp.Expr | None) -> bool:
    """Whether a substring's start is a position no dialect counts otherwise: 1 or more."""
    return (_constant(start) or 0) >= 1


def _engines_own_substring(node: exp.Substring) -> bool:
    """Whether a substring is one every engine's own reads as PostgreSQL does: a start of 1 or more
    and no length a constant says is negative (PostgreSQL's 22011, which the row raises) or ends
    past an integer (which DuckDB's own overflows on)."""
    start, length = node.args.get("start"), node.args.get("length")
    if not _counts_from_the_first(start):
        return False
    count = _constant(length)
    return count is None or (count >= 0 and (_constant(start) or 0) + count <= 2**31 - 1)


def _call(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call the table may hold."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if type(node) in _READ_AS:
        return _READ_AS[type(node)], [node.this]
    if type(node) in _REGRESSIONS_READ:
        return _REGRESSIONS_READ[type(node)], [node.this, node.expression]
    if isinstance(node, exp.Timestamp):
        return "timestamp", [a for a in (node.this, node.args.get("zone")) if a is not None]
    if isinstance(node, exp.ToNumber):
        return "to_number", [node.this, node.args["format"]]
    if isinstance(node, exp.WidthBucket):
        at = ("this", "min_value", "max_value", "num_buckets", "threshold")
        return "width_bucket", [node.args[k] for k in at if node.args.get(k) is not None]
    if isinstance(node, exp.Length):  # an overload of a type: length(text) has no row
        return "length", [node.this]
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.IntDiv):  # sqlglot's reading of div
        return "div", [node.this.this, node.this.expression]
    if isinstance(node, (exp.Round, exp.Trunc)):
        called = "round" if isinstance(node, exp.Round) else "trunc"
        return called, [a for a in (node.this, node.args.get("decimals")) if a is not None]
    if isinstance(node, exp.ArrayConcat) and len(node.expressions) == 1:
        return "array_cat", [node.this, node.expressions[0]]
    if isinstance(node, exp.Repeat):
        return "repeat", [node.this, node.args["times"]]
    if isinstance(node, exp.ArrayRemove):
        return "array_remove", [node.this, node.expression]
    if isinstance(node, exp.Substring) and not _engines_own_substring(node):
        length = node.args.get("length")
        return "substr", [node.this, node.args["start"], *([] if length is None else [length])]
    if isinstance(node, exp.RegexpReplace) and node.args.get("position") is not None:
        at = ("this", "expression", "replacement", "position", "occurrence")
        if node.args.get("modifiers") is None:
            return "regexp_replace_at", [node.args[k] for k in at if node.args.get(k) is not None]
    if isinstance(node, exp.Log) and node.this == exp.Literal.number(10):  # sqlglot's log10
        return "log10", [node.args["expression"]]
    if isinstance(node, (exp.Log, exp.Pow)):
        called = "log" if isinstance(node, exp.Log) else "power"
        return called, [a for a in (node.this, node.expression) if a is not None]
    if isinstance(node, exp.Time):  # what sqlglot reads as a zone is an overload's second argument
        return "time", [a for a in (node.this, node.args.get("zone")) if a is not None]
    if isinstance(node, exp.SHA2Digest):  # sqlglot's reading of sha256, sha384 and sha512
        length = node.args.get("length")
        return f"sha{length.this if isinstance(length, exp.Literal) else 256}", [node.this]
    if isinstance(node, exp.Encode) and isinstance(node.args.get("charset"), exp.Literal):
        return f"encode_{node.args['charset'].this.lower()}", [node.this]
    if isinstance(node, exp.RegexpSubstr):
        keys = ("this", "expression", "position", "occurrence", "parameters", "group")
        return "regexp_substr", [node.args[k] for k in keys if node.args.get(k) is not None]
    if isinstance(node, exp.RegexpInstr):
        keys = ("this", "expression", "position", "occurrence", "option", "parameters", "group")
        return "regexp_instr", [node.args[k] for k in keys if node.args.get(k) is not None]
    return None


def _columns_of(table: exp.Table, rewrite: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A function of ``_ROWS_OF_COLUMNS`` in FROM: a row of its struct's fields, under the name the
    statement gives it."""
    call = table.this
    name = call.name.lower()
    written = rewrite(call)
    written = written.this if isinstance(written, exp.Alias) else written
    rows = written.this if isinstance(written, exp.Explode) else written.expressions[0]
    expanded = exp.Anonymous(
        this="unnest",
        expressions=[rows, exp.Kwarg(this=exp.var("recursive"), expression=exp.true())],
    )
    alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
    return exp.Subquery(this=exp.select(expanded), alias=alias)


def _bytea_digits(text: str) -> str | None:
    """The hex digits of byteain's reading of ``text``; None for what it refuses."""
    if text.startswith("\\x"):
        digits = text[2:]
        valid = len(digits) % 2 == 0 and all(c in string.hexdigits for c in digits)
        return digits if valid else None
    found = b""
    position = 0
    for escape in _BYTEA_ESCAPE.finditer(text):
        found += text[position : escape.start()].encode()
        code = escape.group(1)
        found += b"\\" if code is None else bytes([int(code, 8)])
        position = escape.end()
    rest = text[position:]
    return None if "\\" in rest else (found + rest.encode()).hex()


def _bytea_input(value: exp.Expr, called: str, dialect: str) -> exp.Expr:
    """A literal that is a bytea as those bytes, on any dialect: PostgreSQL reads the literal of
    sha256('\\x6162') as byteain does, where a dialect would hash its text."""
    said = said_type(value)
    if said is not None and said.split("(")[0] in _TEXT_TYPES:
        raise FunctionRefused(called, dialect)  # function sha256(text) does not exist
    literal = value.this if said == "bytea" else value
    if isinstance(literal, exp.Null) and dialect == "mysql":  # it has no CAST AS VARBINARY
        return literal
    if not isinstance(literal, exp.Literal) or not literal.is_string:
        return value
    digits = _bytea_digits(literal.this)
    if digits is None:
        raise FunctionRefused(called, dialect)  # invalid input syntax for type bytea
    if dialect == "tsql":  # it has no UNHEX
        return exp.HexString(this=digits)
    return exp.Unhex(this=exp.Literal.string(digits))


def _digits_as_written(value: exp.Expr) -> exp.Expr:
    """A numeric literal as the text the statement wrote: an engine's float would lose digits."""
    node = value.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    negated = isinstance(node, exp.Neg)
    number = node.this.unnest() if negated else node
    if not isinstance(number, exp.Literal) or number.is_string:
        return value
    return exp.Literal.string(f"{'-' if negated else ''}{number.this}")


def _range_as_written(value: exp.Expr) -> exp.Expr:
    """A range literal as range_out writes it: a column's text is that already."""
    node = value.unnest()
    if not isinstance(node, exp.Cast):
        return value
    literal, said = node.this.unnest(), node.to.sql(dialect="postgres").lower()
    if isinstance(literal, exp.Null):
        return literal
    if not isinstance(literal, exp.Literal) or not literal.is_string:
        return node.this
    if said in RANGE_TYPES:
        return exp.Literal.string(range_text(parse_range(RANGE_TYPES[said], literal.this)))
    rtype = MULTIRANGE_TYPES[said]
    ranges = parse_multirange(rtype, literal.this).ranges
    return exp.Literal.string("{" + ",".join(range_text(r) for r in ranges) + "}")


def range_rows_chosen(tree: exp.Expr) -> exp.Expr:
    """Each hash of a range in the row of the type its statement says: the pass that reads ranges
    runs first, and leaves no cast to say it."""

    def chosen(node: exp.Expr) -> exp.Expr:
        dotted = isinstance(node, exp.Dot) and isinstance(node.this, exp.Identifier)
        target = node.expression if dotted and node.this.name.lower() == "pg_catalog" else node
        call = _call(target)
        if call is None:
            return node
        name = overload(call[0], call[1])
        if name not in pg_forms_hash.RANGE_ROWS:
            return node
        args = call[1]
        return exp.Anonymous(this=name, expressions=[_range_as_written(args[0]), *args[1:]])

    return tree.transform(chosen)


def _array_elements_as_written(value: exp.Expr) -> exp.Expr:
    """An array literal as its elements: an array_in text is read by no engine's cast."""
    node = value.unnest()
    return postgres_literal(node) if isinstance(node, exp.Cast) else value


_ARRAY_INPUT = (
    pg_forms_hash.ARRAY_ROWS | pg_forms_aggregate.ARRAY_INPUT | pg_forms_index.ARRAY_INPUT
)


def _strict_null(name: str, arity: int, dialect: str) -> exp.Expr | None:
    """A statement's NULL of a strict function whose form reads its bytes, or on ClickHouse, which
    has no NULL of an array, of one over an array. None where the form answers it."""
    if name in pg_forms_hash.ROWS:
        return exp.cast(exp.null(), "BIGINT" if arity > 1 else "INT")
    if name in pg_forms_index.PER_ENGINE_RESULTS:
        return exp.cast(exp.null(), pg_forms_index.PER_ENGINE_RESULTS[name])
    return exp.null() if dialect == "clickhouse" and name in _ARRAY_INPUT else None


def _is_null(value: exp.Expr) -> bool:
    """Whether an argument is a NULL, of any type the statement says."""
    node = value.unnest()
    return isinstance(node.this.unnest() if isinstance(node, exp.Cast) else node, exp.Null)


def _array_as_text(value: exp.Expr) -> exp.Expr:
    """An array literal as its elements' text: no engine's decimal holds NaN or every digit, and
    no engine's date holds infinity."""
    node = value.unnest()
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Array):
        return exp.Cast(this=node.this, to=exp.DataType.build("TEXT[]", dialect="postgres"))
    return value


#: The names sqlglot would read as something else: a row of the table, by its own name.
_KEPT_BY_NAME = frozenset(name for name, _ in MACROS) | {"char"}


#: PostgreSQL's reading of a position before the value, or a negative length, which these engines
#: read from its end.
_FROM_THE_END = frozenset({"trino", "presto", "athena", "clickhouse"})
_EDGE_FORMS: dict[tuple[type[exp.Expr], bool], str] = {
    (exp.Substring, False): "SUBSTRING(v, GREATEST(s, 1))",
    (exp.Substring, True): "SUBSTRING(v, GREATEST(s, 1), GREATEST(s + n - GREATEST(s, 1), 0))",
    (exp.Left, True): (
        "CASE WHEN n < 0 THEN SUBSTRING(v, 1, GREATEST(LENGTH(v) + n, 0)) ELSE LEFT(v, n) END"
    ),
    (exp.Right, True): (
        "CASE WHEN n < 0 THEN SUBSTRING(v, LEAST(1 - n, LENGTH(v) + 1)) "
        "ELSE SUBSTRING(v, GREATEST(LENGTH(v) - n + 1, 1)) END"
    ),
}


def _constant(node: exp.Expr | None) -> int | None:
    """The whole number ``node`` always is; None for anything a row decides."""
    node = node.unnest() if node is not None else None
    if isinstance(node, exp.Neg):
        inner = _constant(node.this)
        return None if inner is None else -inner
    if isinstance(node, exp.Add):
        left, right = _constant(node.this), _constant(node.expression)
        return None if left is None or right is None else left + right
    if isinstance(node, exp.Literal) and node.is_int:
        return int(node.this)
    if isinstance(node, exp.Cast) and node.to.this in _INTEGER_RANGES:
        # 42::integer names its value as plainly as 42, where the type holds it; '42'::integer too.
        quoted = node.this.unnest()
        if isinstance(quoted, exp.Literal) and quoted.is_string:
            text = quoted.this.strip()
            quoted = exp.Literal.number(text) if re.fullmatch(r"[+-]?[0-9]+", text) else None
        inner = _constant(quoted)
        low, high = _INTEGER_RANGES[node.to.this]
        return inner if inner is not None and low <= inner <= high else None
    return None


#: The values each integer type holds: a cast of a constant past them raises (22003).
_INTEGER_RANGES = {
    exp.DataType.Type.SMALLINT: (-(2**15), 2**15 - 1),
    exp.DataType.Type.INT: (-(2**31), 2**31 - 1),
    exp.DataType.Type.BIGINT: (-(2**63), 2**63 - 1),
}


def _within_the_value(args: list[exp.Expr]) -> bool:
    """Whether substr's start is a constant of at least 1 and its length none or a constant of at
    least 0, ending within an integer."""
    start = _constant(args[1])
    length = _constant(args[2]) if len(args) == 3 else 0
    return (
        start is not None
        and start >= 1
        and length is not None
        and length >= 0
        and start + length <= 2**31 - 1  # past an integer DuckDB's own overflows
    )


def _past_the_edge(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """substring, left or right as PostgreSQL reads their position, where a row decides it or a
    constant is one the engine would read from the end; a safe constant stays the engine's own."""
    if isinstance(node, exp.Substring):
        start, length = node.args.get("start"), node.args.get("length")
        if start is not None and (start.unnest().is_string or said_type(start) == "text"):
            raise FunctionRefused("substring", dialect)  # substring(text, pattern)
        safe = (_constant(start) or 0) >= 1
        values = {"v": node.this, "s": start, "n": length}
        key = (exp.Substring, length is not None)
        if start is None:
            return None
    else:
        count = node.expression
        constant = _constant(count)
        safe = isinstance(node, exp.Left) and constant is not None and constant >= 0
        values = {"v": node.this, "n": count}
        key = (type(node), True)
    present = [value for value in values.values() if value is not None]
    if safe or any(_is_volatile(value) for value in present):
        return None

    def bound(part: exp.Expr) -> exp.Expr:
        if isinstance(part, exp.Column) and part.name in values:
            return _argument(values[part.name]).copy()
        return part

    form = _EDGE_FORMS[key]
    if dialect == "clickhouse":  # its length is unsigned, which no position mixes with a signed n
        form = form.replace("LENGTH(v)", "CAST(LENGTH(v) AS BIGINT)")
    return sqlglot.parse_one(form, read="postgres").transform(bound)


#: The most digits past the point a decimal of 38 holds and leaves room for a whole part.
_MOST_SCALE = 18
#: The most digits past the point DuckDB's DECIMAL holds: its width is 38 at most.
_DUCKDB_MOST_SCALE = 38


def refuse_named(tree: exp.Expr, dialect: str) -> None:
    """A statement a later rewrite would take for something else, read for what a replay refused:
    in_range names no range, and pg_ranges reads it as one."""
    for node in _unqualified(tree.copy()).walk():
        name = engine_refusals.refused_call(node, dialect, said_type)
        if name is not None:
            raise FunctionRefused(name, dialect)


def _unqualified(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each pg_catalog.f(...) the call of f, as sqlglot reads it: no engine but
    PostgreSQL and DuckDB has that schema, and sqlglot writes a qualified call as it stands."""
    for dot in reversed(list(tree.find_all(exp.Dot))):
        call = dot.expression
        if not isinstance(dot.this, exp.Identifier) or dot.this.name.lower() != "pg_catalog":
            continue
        if isinstance(call, exp.Anonymous) and call.name.lower() in _KEPT_BY_NAME:
            call = exp.Anonymous(this=call.name.lower(), expressions=call.expressions)
        elif isinstance(call, exp.Anonymous):
            args = [said_text(a) for a in call.expressions]
            call = exp.func(call.name, *args, dialect="postgres")
        if isinstance(call, (exp.Func, exp.Binary)):  # mod is a binary as sqlglot reads it
            replaced = dot.replace(call)
            tree = replaced if dot is tree else tree
    return tree


#: The rows whose result has the scale their second argument says.
_OF_A_SAID_SCALE = frozenset({"round_numeric", "trunc_numeric"})


def _said_scale(scale: exp.Expr, called: str, dialect: str) -> int | None:
    """The digits a call says it keeps, None for a NULL, which answers NULL. A dialect's decimal
    keeps its argument's scale, and a scale only a row knows cannot be a type. A replay on the other
    engines refused a scale below 0 and one a decimal of 38 has no room for."""
    said = scale.unnest()
    if isinstance(said, exp.Cast):
        said = said.this.unnest()
    if isinstance(said, exp.Null):
        return None
    negated = said.this.unnest() if isinstance(said, exp.Neg) else None
    if isinstance(negated, exp.Cast):  # -1::integer is -(1::integer): the cast reads 1 first
        inner = _constant(negated.this)
        if inner is None:
            raise FunctionRefused(called, dialect)
        if inner > _INT4_GREATEST:
            raise PostgresError("22003", "integer out of range")  # the cast, before the negation
        said = exp.Neg(this=exp.Literal.number(inner))
    digits = _constant(said)
    if digits is None and isinstance(said, exp.Literal) and said.is_string:
        digits = _integer_text(said.this)  # '0'::integer
    most = _DUCKDB_MOST_SCALE if dialect in _DUCKDB else _MOST_SCALE
    negative_kept = dialect in _DUCKDB or dialect == "clickhouse"  # ClickHouse's round takes -38..
    if digits is None or digits > most or (not negative_kept and digits < 0):
        raise FunctionRefused(called, dialect)
    return digits


_INT4_GREATEST = 2147483647
#: The fewest digits ClickHouse's round and trunc of a Decimal take; below it every numeric a
#: Decimal holds (38 digits) is 0, as PostgreSQL's round and trunc answer it.
_CLICKHOUSE_LEAST_SCALE = -38


def _integer_text(text: str) -> int | None:
    """The integer an integer's text says, as int4in reads it; None for any other text."""
    try:
        return int(text.strip())
    except ValueError:
        return None


def _answered_by_a_row(node: exp.Expr, dialect: str) -> bool:
    """Whether a row of the polyfill grid answers ``node`` on ``dialect`` (REQ-2236; REQ-2167, AN
    ANSWER WITHIN A STATED BOUND COMES BEFORE A REFUSAL): its form is the engine's SQL, or a
    rewrite that writes this call. A row that keeps the old table's form, a rewrite that leaves
    this call or refuses it, and a refusal answer nothing: the call is refused as it was."""
    try:
        applied = polyfill.GRID.read(node, dialect)  # a rule of no pattern runs its rewrite here
        if applied is None or applied.form is None or isinstance(applied.form, polyfill.Refusal):
            return False
        if isinstance(applied.form, str) or applied.rewritten is not None:
            return True
        argued = node.copy()  # read where the call stands, as _polyfilled_by_rule reads it
        argued.parent, argued.arg_key = node.parent, node.arg_key
        return applied.form.function(argued, dialect) is not None
    except polyfill_notation.Refused as refused:
        if refused.ruling:  # the row names what of the call it refuses
            called = node.name.lower() if isinstance(node, exp.Anonymous) else _feature_of(node)
            raise RowRefused(called, dialect, refused.refusal) from refused
        return False


def _answered_first(node: exp.Expr, dialect: str, skipped: tuple[polyfill.Feature, ...]) -> bool:
    """Whether a grid rule over every call (Rewrite.first, read before any pattern by the walk)
    answers ``node``, a call the old table refuses: a bit string's row, whose name bytea's
    patterns share. Read only for such a call: the rules are many, and the walk reads them again.
    A rule of a feature in ``skipped`` is not read (:func:`inline`'s ``generated``)."""
    try:
        first = polyfill.GRID.read_first(node, dialect, skipped)
    except polyfill_notation.Refused as refused:
        if refused.ruling:
            called = node.name.lower() if isinstance(node, exp.Anonymous) else _feature_of(node)
            raise RowRefused(called, dialect, refused.refusal) from refused
        return False
    return first is not None and first.rewritten is not None


def _of_dimensions_on_duckdb(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` with each call of an array of more than one dimension in its DuckDB form
    (polyfill.array_dimensions.on_duckdb); its row's refusal raises with PostgreSQL's SQLSTATE."""
    try:
        return polyfill_array_dimensions.on_duckdb(tree)
    except polyfill_array_dimensions.CallRefused as refused:
        raise RowRefused(refused.called, dialect, refused.refusal) from refused


def inline(tree: exp.Expr, dialect: str, *, generated: bool) -> exp.Expr:
    """``tree`` with each call of a function of the table written in its form for ``dialect``.
    Raises :class:`FunctionRefused` for a call of one it has no form of. A call with no overload
    of its argument count is left, for the engine to say so.

    ``generated``: Provisa wrote ``tree`` from a graph IR (Cypher, SPARQL, GraphQL), where a list
    of lists is a nested list; PostgreSQL's reading of an array of more than one dimension is
    for SQL a client wrote alone (REQ-2236)."""
    if dialect in NATIVE:
        return tree
    # REQ-2236: SQL Provisa generated reads a list of lists as nested lists, never by dimensions.
    skipped = (polyfill_array_dimensions.FEATURE,) if generated else ()
    if dialect not in _DUCKDB:
        tree = _unqualified(tree)
        for node in tree.walk():
            if _answered_by_a_row(node, dialect):
                continue  # REQ-2167: an answer within a stated bound comes before a refusal
            refused_call = engine_refusals.refused_call(node, dialect, said_type)
            if refused_call is not None and not _answered_first(node, dialect, skipped):
                raise FunctionRefused(refused_call, dialect)
    elif not generated:
        # REQ-2236: an array of more than one dimension as PostgreSQL reads it, before any form
        # (a text search's or JSON's among them) reads the arrays.
        tree = _of_dimensions_on_duckdb(tree, dialect)
    try:
        tree = pg_forms_textsearch.lowered(tree, dialect)
        tree = pg_forms_json.lowered(tree, dialect, lambda n: _answered_by_a_row(n, dialect))
    except (pg_forms_json.NoForm, pg_forms_textsearch.NoForm) as refused:
        raise FunctionRefused(refused.called, dialect) from refused

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.WithinGroup):
            if isinstance(node.this, (exp.PercentileCont, exp.PercentileDisc)):
                return _percentile(node, dialect)
            if _ordered_set_name(node) in _HYPOTHETICAL:
                return _hypothetical(node, dialect)
            if dialect in _TRINO or dialect == "clickhouse":
                raise FunctionRefused(_ordered_set_name(node), dialect)  # mode: no engine's own
            return node
        if (
            isinstance(node, exp.Table)
            and isinstance(node.this, exp.Anonymous)
            and node.this.name.lower() in _ROWS_OF_COLUMNS
            and dialect in _DUCKDB
        ):
            return _columns_of(node, rewrite)
        if isinstance(node, exp.RegexpReplace) and dialect in _FROM_THE_END:
            parts = (node.this, node.expression, node.args["replacement"])
            if any(_is_null(part) for part in parts):
                return exp.Cast(this=exp.Null(), to=exp.DataType.build("TEXT"))
        if (
            isinstance(node, exp.Anonymous)
            and node.name.lower() in ("date_part", "extract")
            and dialect in _FROM_THE_END
            and any(_is_null(part) for part in node.expressions)
        ):  # strict, and an engine's own part of a NULL is its error
            return exp.Cast(this=exp.Null(), to=exp.DataType.build("DOUBLE"))
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        # A grid rule over every call, before the forms of a substring past its edge: a substring
        # of a bit string is its row's.
        try:
            first = polyfill.GRID.read_first(target, dialect, skipped)
        except polyfill_notation.Refused as refused:
            if not refused.ruling:
                raise
            called = (
                target.name.lower() if isinstance(target, exp.Anonymous) else _feature_of(target)
            )
            raise RowRefused(called, dialect, refused.refusal) from refused
        if first is not None and first.rewritten is not None:
            in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
            return first.rewritten if in_select else first.rewritten.unalias()
        # A substring of a range's own text (pg_ranges' mark): ASCII, which no row reads by
        # characters, so the grid is not read of it (its arguments would be read twice).
        tried = bool(node.meta.get(polyfill_texts.RANGE_TEXT))
        if isinstance(node, (exp.Substring, exp.Left, exp.Right)) and dialect in _FROM_THE_END:
            if dialect == "clickhouse" and not tried:
                # REQ-2236: a grid row that reads the position as PostgreSQL does (ClickHouse's
                # text by characters) comes before this form, which reads bytes there.
                polyfilled, tried = _polyfilled(target, node, dialect, rewrite), True
                if polyfilled is not None:
                    return polyfilled
            edged = _past_the_edge(node, dialect)
            if edged is not None:
                return edged
        polyfilled = None if tried else _polyfilled(target, node, dialect, rewrite)
        if polyfilled is not None:
            return polyfilled
        if type(node) in _ARRAY_OPERATOR_ROWS and dialect in _TRINO:
            row = _ARRAY_OPERATOR_ROWS[type(node)]
            target = exp.Anonymous(this=row, expressions=[node.this, node.expression])
        call = _call(target)
        if call is None:
            return node
        called, args = call
        if called in _COUNTS:
            return _counted(called, [a.transform(rewrite) for a in args], dialect)
        name = overload(called, args)
        macro = MACROS.get((name, len(args)))
        if macro is None:
            return node
        step, form = macro.resolved(dialect)
        told = _is_told(called, args)  # by the name as written: a row's own name says its overload
        called = _CALLED.get(name, called)
        if step is Step.REFUSED or not told:
            raise FunctionRefused(called, dialect)
        if form is MAPPED:
            return target
        if name in _BYTEA_INPUT:
            args = [_bytea_input(a, called, dialect) for a in args]
        if name in pg_forms_hash.NUMERIC_INPUT:
            args = [_digits_as_written(args[0]), *args[1:]]
        if name in _ARRAY_INPUT:
            args = [_array_elements_as_written(a) for a in args]
        if name in pg_forms_hash.TEXT_ARRAY_ROWS:
            args = [_array_as_text(args[0]), *args[1:]]
        if any(_is_null(a) for a in args):
            answer = _strict_null(name, len(args), dialect)
            if answer is not None:
                in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
                return exp.alias_(answer, called) if in_select else answer
        if name == "substr" and _within_the_value(args):
            # A start of at least 1 and a length of none or at least 0 every engine reads as
            # PostgreSQL does: the engine's own, as a substring past no edge is (_past_the_edge).
            length = args[2].transform(rewrite) if len(args) == 3 else None
            written = exp.Substring(
                this=args[0].transform(rewrite), start=args[1].transform(rewrite), length=length
            )
            in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
            return exp.alias_(written, called) if in_select else written
        digits = _said_scale(args[1], called, dialect) if name in _OF_A_SAID_SCALE else None
        if digits is not None:  # an engine takes a scale only as a constant
            args = [args[0], exp.Literal.number(digits)]
        # transform does not revisit what a replaced node holds: a call in an argument.
        if digits is not None and dialect == "clickhouse" and digits < _CLICKHOUSE_LEAST_SCALE:
            written = exp.Mul(this=args[0].transform(rewrite), expression=exp.Literal.number(0))
        else:
            written = _written(macro, [a.transform(rewrite) for a in args], dialect, node)
        if digits is not None:  # the scale PostgreSQL's numeric answers with, 0 for one below it
            scaled = f"DECIMAL(38, {max(digits, 0)})"
            written = exp.Cast(this=written, to=exp.DataType.build(scaled, dialect="postgres"))
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, called)  # named for the function
        return written

    return tree.transform(rewrite, copy=False)


#: The name and arguments of a call the table may hold, for the grid's rules that read calls.
call_of = _call
