# Copyright (c) 2026 Kenneth Stott
# Canary: b27e6f09-13d4-4c8a-9f61-e4d05a7c3b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The enum functions on DuckDB, Trino and ClickHouse (REQ-2167, REQ-2171), replayed against
postgres:17.11's recorded answers, and the calls a statement's own shape decides: lastval,
pg_get_publication_tables and regexp_replace's Nth match on ClickHouse.

An engine reads a source's enum as its label: Trino and ClickHouse have no such type, and what
DuckDB attaches it still sends a client as text. The labels' order and the type's numbers are the
source's alone (:mod:`provisa.core.pg_enum`), so :func:`with_source_enums` writes them into the
statement, before anything reads it, and everything after is a function of the text: the forms
below order a value by its place among the labels, on a column as on a literal, and
``pg_typed_value`` or ``pg_typed_array`` around a result says the type a client is told
(provisa.pgwire.cast_types). A call on a value no statement or registration says is a source's enum
is left as it stands, where the engine's own enum answers or the call is refused by name.

A cast to a source's enum that no form consumed names a type only the source and what attaches it
hold. Routing, which comes later, decides whether the statement reaches them:
:func:`marked_source_casts` says which casts those are, and :func:`lowered`, which knows the dialect,
writes each back for PostgreSQL and DuckDB and refuses it by the type's name on any other."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable, Mapping

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.scope import Scope, traverse_scope

from provisa.core.pg_enum import SourceEnum, SourceEnums, named, of_oid
from provisa.core.pg_error import PostgresError

TYPED_VALUE = "pg_typed_value"
TYPED_ARRAY = "pg_typed_array"
SOURCE_CAST = "pg_source_cast"
_D, _T, _C, _P = "duckdb", "trino", "clickhouse", "postgres"
_OF = {
    _D: frozenset({_D}),
    _T: frozenset({_T, "athena", "presto"}),
    _C: frozenset({_C}),
    _P: frozenset({_P}),
}
_ENGINES = tuple(_OF)
#: Each engine's place of a value among the labels, NULL if it is NULL.
_PLACE = {
    _D: "list_position(labels, {})",
    _T: "array_position(labels, {})",
    _C: "nullIf(indexOf(labels, {}), 0)",
    _P: "array_position(labels, {})",
}
_ELEMENT = {
    _D: "list_extract(labels, {})",
    _T: "element_at(labels, {})",
    _C: "arrayElement(labels, {})",
    _P: "(labels)[{}]",
}
_COUNT = {
    _D: "len(labels)",
    _T: "cardinality(labels)",
    _C: "length(labels)",
    _P: "cardinality(labels)",
}
_SLICE = {
    _D: "list_slice(labels, {first}, {last})",
    _T: "slice(labels, {first}, greatest({last} - {first} + 1, 0))",
    _C: "arraySlice(labels, {first}, greatest({last} - {first} + 1, 0))",
    _P: "(labels)[{first}:{last}]",
}


def _ordered(dialect: str, keep: str) -> str:
    """One of ``a`` and ``b``, strict: ``keep`` is the comparison that keeps ``a``, which holds of
    neither order when a place is NULL."""
    a, b = _PLACE[dialect].format("a"), _PLACE[dialect].format("b")
    other = {">=": "<", "<=": ">"}[keep]
    return f"CASE WHEN {a} {keep} {b} THEN a WHEN {a} {other} {b} THEN b END"


def _range(dialect: str) -> str:
    """enum_range: from ``a``, or the first label, to ``b``, or the last; empty past each other."""
    first = f"coalesce({_PLACE[dialect].format('a')}, 1)"
    last = f"coalesce({_PLACE[dialect].format('b')}, {_COUNT[dialect]})"
    return _SLICE[dialect].format(first=first, last=last)


def _extreme(dialect: str, aggregate: str) -> str:
    return _ELEMENT[dialect].format(f"{aggregate}({_PLACE[dialect].format('value')})")


def _per_engine(form: Callable[[str], str]) -> dict[frozenset[str], str]:
    return {_OF[d]: form(d) for d in _ENGINES}


#: A row: name, parameters, each dialect's form.
ROWS: tuple[tuple[str, tuple[str, ...], dict[frozenset[str], str]], ...] = (
    (TYPED_VALUE, ("oid", "value"), _per_engine(lambda _: "value")),
    (TYPED_ARRAY, ("oid", "value"), _per_engine(lambda _: "value")),
    ("pg_enum_larger", ("labels", "a", "b"), _per_engine(lambda d: _ordered(d, ">="))),
    ("pg_enum_smaller", ("labels", "a", "b"), _per_engine(lambda d: _ordered(d, "<="))),
    ("pg_enum_slice", ("labels", "a", "b"), _per_engine(_range)),
    ("pg_enum_max", ("labels", "value"), _per_engine(lambda d: _extreme(d, "max"))),
    ("pg_enum_min", ("labels", "value"), _per_engine(lambda d: _extreme(d, "min"))),
)

_CALLS_AN_ENUM = re.compile(r"\benum_(first|last|range|larger|smaller)\b|\b(max|min)\b", re.I)

#: table, column -> the type a table's column is registered with.
TableType = Callable[[exp.Table, str], "str | None"]


def _no_table(_table: exp.Table, _name: str) -> str | None:
    return None


def _of_type(enums: SourceEnums, data_type: exp.DataType) -> SourceEnum | None:
    kind = data_type.args.get("kind")
    if data_type.this != exp.DataType.Type.USERDEFINED or kind is None:
        return None
    if isinstance(kind, exp.Dot):
        return named(enums, kind.this.name, kind.expression.name)
    return named(enums, None, kind.name)


class _Statement:
    """What a statement says of its enums: through a cast, a typed call, or a column."""

    def __init__(self, tree: exp.Expr, enums: SourceEnums, table_type: TableType) -> None:
        self.enums, self.table_type = enums, table_type
        try:
            self.scopes = {id(scope.expression): scope for scope in traverse_scope(tree)}
        except OptimizeError:
            self.scopes = {}  # no scopes to follow: a column says nothing

    def _of_column(self, column: exp.Column) -> SourceEnum | None:
        scope = self.scopes.get(id(column.find_ancestor(exp.Select)))
        if scope is None:
            return None
        sources = scope.selected_sources
        selected = sources.get(column.table) if column.table else None
        if selected is None and not column.table and len(sources) == 1:
            selected = next(iter(sources.values()))
        if selected is None:
            return None
        source = selected[1]
        if isinstance(source, exp.Table):
            spelled = self.table_type(source, column.name)
            if not spelled:
                return None
            schema, _, name = spelled.rpartition(".")
            return named(self.enums, schema or None, name)
        if isinstance(source, Scope) and isinstance(source.expression, exp.Select):
            for select in source.expression.selects:
                if select.alias_or_name == column.name:
                    return self.of(select.unalias())
        return None

    def of(self, value: exp.Expr) -> SourceEnum | None:
        """The source's enum ``value`` is of, None if nothing says so."""
        if isinstance(value, exp.Paren):
            return self.of(value.this)
        if isinstance(value, exp.Cast):
            return _of_type(self.enums, value.to)
        if isinstance(value, exp.Column):
            return self._of_column(value)
        if isinstance(value, exp.Anonymous) and value.name.lower() == TYPED_VALUE:
            return of_oid(self.enums, int(value.expressions[0].this))
        return None


def _text(value: exp.Expr, enum: SourceEnum) -> exp.Expr:
    """``value`` as its label. A literal is read as PostgreSQL's input reads it."""
    inner = value.this if isinstance(value, (exp.Paren, exp.Cast)) else value
    if isinstance(inner, exp.Literal) and inner.is_string:
        if inner.this not in enum.labels:
            raise PostgresError(
                "22P02", f'invalid input value for enum {enum.shown}: "{inner.this}"'
            )
        return inner.copy()
    if isinstance(inner, exp.Anonymous) and inner.name.lower() == TYPED_VALUE:
        return inner.expressions[1].copy()
    return exp.cast(inner.copy(), "TEXT")


def _labels(enum: SourceEnum) -> exp.Array:
    return exp.Array(expressions=[exp.Literal.string(label) for label in enum.labels])


def _typed(wrapper: str, oid: int, value: exp.Expr) -> exp.Anonymous:
    return exp.Anonymous(this=wrapper, expressions=[exp.Literal.number(oid), value])


def _form(name: str, enum: SourceEnum, *values: exp.Expr) -> exp.Anonymous:
    args = [_labels(enum), *(_text(v, enum) for v in values)]
    return exp.Anonymous(this=name, expressions=args)


def _enum_call(call: exp.Anonymous, said: _Statement) -> exp.Expr | None:
    name, args = call.name.lower(), call.expressions
    enums = [said.of(arg) for arg in args]
    enum = next((e for e in enums if e is not None), None)
    if enum is None or len(args) not in (1, 2):
        return None
    if name in ("enum_first", "enum_last") and len(args) == 1:
        for arg in args:
            _text(arg, enum)
        label = enum.labels[0 if name == "enum_first" else -1]
        return _typed(TYPED_VALUE, enum.oid, exp.Literal.string(label))
    if name == "enum_range" and len(args) == 1:
        _text(args[0], enum)
        return _typed(TYPED_ARRAY, enum.array_oid, _labels(enum))
    if name == "enum_range":
        return _typed(TYPED_ARRAY, enum.array_oid, _form("pg_enum_slice", enum, *args))
    if name in ("enum_larger", "enum_smaller") and len(args) == 2:
        return _typed(TYPED_VALUE, enum.oid, _form(f"pg_{name}", enum, *args))
    if (
        name in ("max", "min") and len(args) == 1
    ):  # pg_catalog."max": sqlglot's Max is the bare name
        return _typed(TYPED_VALUE, enum.oid, _form(f"pg_enum_{name}", enum, *args))
    return None


def with_source_enums(sql: str, enums: SourceEnums, table_type: TableType = _no_table) -> str:
    """``sql`` with each enum function of a source's enum written over its labels and its type."""
    if not isinstance(enums, Mapping) or not enums or _CALLS_AN_ENUM.search(sql) is None:
        return sql
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return sql  # not PostgreSQL's grammar: nothing here reads it
    said = _Statement(tree, enums, table_type)
    changed = False
    for node in reversed(list(tree.walk(bfs=True))):
        written: exp.Expr | None = None
        if isinstance(node, exp.Anonymous) and not isinstance(node.parent, exp.Window):
            written = _enum_call(node, said)
        elif isinstance(node, (exp.Max, exp.Min)) and not isinstance(node.parent, exp.Window):
            enum = said.of(node.this)
            if enum is not None:
                name = "max" if isinstance(node, exp.Max) else "min"
                written = _typed(TYPED_VALUE, enum.oid, _form(f"pg_enum_{name}", enum, node.this))
        if written is None:
            continue
        changed = True
        qualified = node.parent if isinstance(node.parent, exp.Dot) else node
        if qualified is tree:
            return sql  # a call is no statement
        qualified.replace(written)
    return tree.sql(dialect="postgres") if changed else sql


def marked_source_casts(sql: str, enums: SourceEnums) -> str:
    """``sql`` with each cast to a source's enum, or to an array of one, written as a call of
    SOURCE_CAST that names the type. A read alone: a write's casts are for its source."""
    if not isinstance(enums, Mapping) or not any(name in sql for _, name in enums):
        return sql
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return sql  # not PostgreSQL's grammar: nothing here reads it
    if not isinstance(tree, exp.Query):
        return sql
    casts = [
        cast
        for cast in reversed(list(tree.find_all(exp.Cast)))
        if any(_of_type(enums, held) for held in cast.to.find_all(exp.DataType))
    ]
    for cast in casts:
        shown = exp.Literal.string(cast.to.sql(dialect="postgres"))
        cast.replace(exp.Anonymous(this=SOURCE_CAST, expressions=[cast.this.copy(), shown]))
    return tree.sql(dialect="postgres") if casts else sql


_POSTGRES_FORMS = {name: (params, forms[_OF[_P]]) for name, params, forms in ROWS}


def _written_for_postgres(call: exp.Anonymous) -> exp.Expr:
    """A marker or a form, as the source that holds the type reads it."""
    if call.name.lower() == SOURCE_CAST:
        value, shown = call.expressions
        cast = sqlglot.parse_one(f"CAST(NULL AS {shown.this})", read="postgres")
        cast.this.replace(value.copy())
        return cast
    params, form = _POSTGRES_FORMS[call.name.lower()]
    values = dict(zip(params, call.expressions))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return values[node.name].copy()
        return node

    return sqlglot.parse_one(form, read="postgres").transform(bound)


def _settled_for_the_source(tree: exp.Expr, dialect: str) -> bool:
    """Each form and marker of this module that the macro table does not settle. The table leaves
    PostgreSQL its own functions and these are none of its, so its forms are written here, and a
    cast is written back where the type is held: at the source, and on the DuckDB that attaches it.
    Elsewhere the cast is refused by the type's name, ``type(value)`` being the function PostgreSQL
    reads it as."""
    from provisa.transpiler.function_macros import FunctionRefused

    marks = [
        call
        for call in reversed(list(tree.find_all(exp.Anonymous)))
        if call.name.lower() == SOURCE_CAST or call.name.lower() in _POSTGRES_FORMS
    ]
    if dialect != _P:
        marks = [mark for mark in marks if mark.name.lower() == SOURCE_CAST]
    if marks and dialect not in (_P, _D):
        raise FunctionRefused(marks[0].expressions[1].this, dialect)
    for mark in marks:  # innermost first: each holds what it reads as already written
        mark.replace(_written_for_postgres(mark))
    return bool(marks)


_TABLES = "pg_get_publication_tables"
_ANSWERED_BY_ITS_SHAPE = re.compile(
    rf"\b(lastval|{_TABLES}|regexp_replace|pg_typed_(value|array)|pg_enum_\w+|{SOURCE_CAST})\b",
    re.I,
)
_ADVANCES_A_SEQUENCE = frozenset({"nextval", "setval"})


def _call_of(node: exp.Expr) -> str:
    return node.name.lower() if isinstance(node, exp.Anonymous) else ""


def _names(args: list[exp.Expr]) -> list[str] | None:
    """The publications a call names, none for a NULL array; None if the statement does not say."""
    if len(args) == 1 and isinstance(args[0], exp.Variadic):
        array = args[0].this
        held = array.this if isinstance(array, exp.Cast) else array
        if isinstance(held, exp.Null):
            return []
        if isinstance(held, exp.Literal) and held.is_string:
            inner = held.this.strip().removeprefix("{").removesuffix("}")
            return [name.strip() for name in inner.split(",") if name.strip()]
        args = held.expressions if isinstance(held, exp.Array) else [held]
    named_ = [a.this if isinstance(a, exp.Cast) else a for a in args]
    if all(isinstance(a, exp.Literal) and a.is_string for a in named_):
        return [a.this for a in named_]
    return None


def _publications(tree: exp.Expr, dialect: str) -> None:
    """No publication exists on a Provisa server: PostgreSQL's error for one a call names, and for
    none no row, so the select list holds a call that adds none to a select that has none."""
    from provisa.transpiler.function_macros import FunctionRefused

    for call in [n for n in tree.find_all(exp.Anonymous) if _call_of(n) == _TABLES]:
        names = _names(call.expressions)
        qualified = call.parent if isinstance(call.parent, exp.Dot) else call
        item = qualified.parent if isinstance(qualified.parent, exp.Alias) else qualified
        select = item.parent
        if names is None or not isinstance(select, exp.Select):
            raise FunctionRefused(_TABLES, dialect)
        if names:
            raise PostgresError("42704", f'publication "{names[0]}" does not exist')
        qualified.replace(exp.cast(exp.null(), "TEXT"))
        select.where(exp.false(), copy=False)


def _refuse_a_pattern_that_varies(tree: exp.Expr, dialect: str) -> None:
    """ClickHouse's search for the Nth match reads its pattern once, as a constant: a column's is
    refused by name, here, rather than by the engine's own error."""
    from provisa.transpiler.function_macros import FunctionRefused

    for node in tree.find_all(exp.RegexpReplace, exp.Anonymous):
        if isinstance(node, exp.RegexpReplace):
            pattern = node.expression if node.args.get("position") is not None else None
        else:
            many = _call_of(node) == "regexp_replace" and len(node.expressions) > 3
            pattern = node.expressions[1] if many else None
        if pattern is not None and pattern.find(exp.Column, exp.Parameter, exp.Placeholder):
            raise FunctionRefused("regexp_replace", dialect)


def lowered(sql: str, dialect: str) -> str:
    """``sql`` with what its shape decides of these functions settled. A statement that calls
    nextval beside lastval is refused by name, for nextval is: an engine drops a column nothing
    reads, so its own refusal might never run, and lastval's error would be the wrong one."""
    from provisa.transpiler.function_macros import FunctionRefused

    if _ANSWERED_BY_ITS_SHAPE.search(sql) is None:
        return sql
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return sql  # not PostgreSQL's grammar: nothing here reads it
    called = {_call_of(n) for n in tree.find_all(exp.Anonymous)}
    if "lastval" in called and called & _ADVANCES_A_SEQUENCE:
        raise FunctionRefused("lastval", dialect)
    if dialect == _C:
        _refuse_a_pattern_that_varies(tree, dialect)
    settled = _settled_for_the_source(tree, dialect)
    if _TABLES not in called:
        return tree.sql(dialect="postgres") if settled else sql
    _publications(tree, dialect)
    return tree.sql(dialect="postgres")
