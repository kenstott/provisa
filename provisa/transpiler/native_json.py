# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1e8b3a-9c24-4d7b-a6e0-3b8d2c7f4e19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The datasource cast (REQ-1965): a natively array, struct or map column handed over as JSON by
an expression the source or engine executes.

The compiler wraps each read of such a column in a marker call that carries the column's hidden
attributes (``native_json_marker``). ``resolve_native_json`` runs where the routed dialect is
known -- ``transpile`` -- and replaces each marker with that backend's own to-JSON expression,
from the one table below. Provisa executes nothing and touches no value.

Exactness inside the JSON, for an array of one primitive type (depth 1 or more): a decimal with a
declared precision is written as a string; a timestamp as ISO 8601 with ``T``, a timestamp with
time zone taken to UTC first; binary as a scalar binary value reads (``\\x00ff``,
``ir_types.bytea_hex``); a float that is not a number or is
infinite is refused by the backend, naming ``json.non_finite_number``. A struct, a map or an array
of them (depth 0) gets the backend's plain to-JSON: nothing inside it is typed.

A backend with no entry, or an element type its entry has no verified form for, is refused by
name: ``NativeJsonUnsupported`` (``query.json_operation_unsupported``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core.json_refusal import NON_FINITE, JsonRefused

MARKER = "provisa_native_json"


@dataclass(frozen=True)
class NestedColumn:
    """The hidden attributes of a natively nested column (``Column.array_depth``,
    ``element_type``, ``native_type``), as the translation reads them."""

    native_type: str
    array_depth: int = 0
    element_type: str | None = None


class NativeJsonUnsupported(JsonRefused):
    """A backend that has no verified way to hand this native value over as JSON."""

    code = "query.json_operation_unsupported"

    def __init__(self, backend: str, column: str, native_type: str, reason: str) -> None:
        self.params = {
            "operation": "to_json",
            "backend": backend,
            "column": column,
            "native_type": native_type,
        }
        super().__init__(
            f"{column}: {backend} has no verified way to read a {native_type} column as JSON "
            f"({reason})"
        )


def native_json_marker(column: exp.Expr, nested: NestedColumn) -> exp.Anonymous:
    """The marker the compiler emits for a read of a natively nested column."""
    return exp.Anonymous(
        this=MARKER,
        expressions=[
            column,
            exp.Literal.string(nested.native_type),
            exp.Literal.number(nested.array_depth),
            exp.Literal.string(nested.element_type or ""),
        ],
    )


def marked_column(node: exp.Expr | None) -> tuple[exp.Expr, NestedColumn] | None:
    """The column and attributes a marker call carries; None when ``node`` is not a marker."""
    if not isinstance(node, exp.Anonymous) or str(node.this).lower() != MARKER:
        return None
    column, native, depth, element = node.expressions
    return column, NestedColumn(
        native_type=native.name, array_depth=int(depth.name), element_type=element.name or None
    )


def _base(element_type: str) -> str:
    return element_type.split("(", 1)[0].strip()


def _has_time_zone(native_type: str) -> bool:
    return "time zone" in native_type or "timestamptz" in native_type


# An element's own expression: a template over ``{x}``, None for "as the backend writes it", or a
# function of the column's attributes giving either.
_ElementForm = str | None | Callable[[NestedColumn], "str | None"]


@dataclass(frozen=True)
class _Backend:
    """One backend's to-JSON forms. ``to_json`` is the template over ``{col}`` for the whole
    value; ``each`` maps every innermost element of a depth-``n`` array through a template over
    ``{x}``; ``elements`` gives, per element type, that template (None: no pre-cast needed).
    An element type missing from ``elements`` has no verified form on this backend."""

    to_json: str
    each: Callable[[str, int, str], str]
    elements: dict[str, _ElementForm]


def _lambda_each(function: str) -> Callable[[str, int, str], str]:
    """Nested ``function(c, x -> ...)`` to depth ``n`` (DuckDB list_transform, Trino transform)."""

    def each(col: str, depth: int, element: str) -> str:
        body = element.format(x=f"x{depth}")
        for level in range(depth, 0, -1):
            operand = col if level == 1 else f"x{level - 1}"
            body = f"{function}({operand}, x{level} -> {body})"
        return body

    return each


def _postgres_each(col: str, depth: int, element: str) -> str:
    """PostgreSQL has no element-wise map; an array's type records no dimensions and its depth is
    always 1 (``column_typing``), so the elements are unnested in order and re-collected. A NULL
    array stays NULL."""
    del depth
    mapped = element.format(x="x")
    return (
        f"CASE WHEN {col} IS NULL THEN NULL ELSE "
        f"ARRAY(SELECT {mapped} FROM UNNEST({col}) WITH ORDINALITY AS u(x, i) ORDER BY i) END"
    )


_AS_WRITTEN = None
_AS_VARCHAR = "CAST({x} AS VARCHAR)"
_AS_TEXT = "CAST({x} AS TEXT)"


def _sized_decimal(as_text: str) -> _ElementForm:
    """A decimal with a declared precision travels as a string, exact on every engine; one with
    none is floating point (REQ-1961) and is written as the backend writes a number."""
    return lambda nested: as_text if "(" in (nested.element_type or "") else None


def _timestamp(plain: str, with_time_zone: str) -> _ElementForm:
    """A timestamp keeps its instant: one with a time zone is taken to UTC before it is written
    (the native type, not the element type, says which)."""
    return lambda nested: with_time_zone if _has_time_zone(nested.native_type) else plain


# A float element that is not a number or is infinite is not JSON. The backend refuses it, naming
# the rule; nothing is substituted for it.
_DUCKDB_FINITE = (
    f"CASE WHEN {{x}} IS NULL OR isfinite({{x}}) THEN {{x}} ELSE error('{NON_FINITE}') END"
)
_TRINO_FINITE = f"IF({{x}} IS NULL OR is_finite({{x}}), {{x}}, fail('{NON_FINITE}'))"
# PostgreSQL has no function that raises; a cast that cannot succeed raises with the rule's name
# in the source's own message ("invalid input syntax for type double precision"). The cast reads
# the element: a cast of a constant is evaluated when the statement is planned, branch taken or
# not, and refused every array of finite numbers.
_POSTGRES_FINITE = (
    "CASE WHEN {x} IS NULL OR ({x} <> 'NaN' AND {x} <> 'Infinity' AND {x} <> '-Infinity') "
    f"THEN {{x}} ELSE CAST('{NON_FINITE}: ' || CAST({{x}} AS TEXT) AS DOUBLE PRECISION) END"
)


_BACKENDS: dict[str, _Backend] = {
    # Verified in-process: tests/unit/test_native_json_duckdb.py executes every form.
    "duckdb": _Backend(
        to_json="to_json({col})",
        each=_lambda_each("list_transform"),
        elements={
            "text": _AS_WRITTEN,
            "smallint": _AS_WRITTEN,
            "integer": _AS_WRITTEN,
            "bigint": _AS_WRITTEN,
            "boolean": _AS_WRITTEN,
            "numeric": _sized_decimal(_AS_VARCHAR),
            "date": _AS_VARCHAR,
            "time": _AS_VARCHAR,
            "uuid": _AS_VARCHAR,
            "timestamp": _timestamp(
                "strftime({x}, '%Y-%m-%dT%H:%M:%S.%f')",
                "strftime({x} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%f')",
            ),
            "bytea": "'\\x' || lower(hex({x}))",
            "float": _DUCKDB_FINITE,
            "double": _DUCKDB_FINITE,
        },
    ),
    # Pinned by the container lane: tests/integration/test_native_json_postgres_e2e.py.
    "postgres": _Backend(
        to_json="to_jsonb({col})",
        each=_postgres_each,
        elements={
            "text": _AS_WRITTEN,
            "smallint": _AS_WRITTEN,
            "integer": _AS_WRITTEN,
            "bigint": _AS_WRITTEN,
            "boolean": _AS_WRITTEN,
            "numeric": _sized_decimal(_AS_TEXT),
            "date": _AS_WRITTEN,
            "time": _AS_TEXT,
            "uuid": _AS_WRITTEN,
            "timestamp": _timestamp(
                "to_char({x}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')",
                "to_char({x} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')",
            ),
            "bytea": "'\\x' || encode({x}, 'hex')",
            "float": _POSTGRES_FINITE,
            "double": _POSTGRES_FINITE,
        },
    ),
    # Pinned by the container lane: tests/integration/test_native_json_trino_e2e.py.
    "trino": _Backend(
        to_json="CAST({col} AS JSON)",
        each=_lambda_each("transform"),
        elements={
            "text": _AS_WRITTEN,
            "smallint": _AS_WRITTEN,
            "integer": _AS_WRITTEN,
            "bigint": _AS_WRITTEN,
            "boolean": _AS_WRITTEN,
            "numeric": _sized_decimal(_AS_VARCHAR),
            "date": _AS_VARCHAR,
            "uuid": _AS_VARCHAR,
            # date_format's %f carries milliseconds only: a timestamp(6) lost its microseconds.
            # A timestamp(6) written as text keeps all six digits, whatever precision it held.
            "timestamp": _timestamp(
                "replace(CAST(CAST({x} AS TIMESTAMP(6)) AS VARCHAR), ' ', 'T')",
                "replace(CAST(CAST({x} AT TIME ZONE 'UTC' AS TIMESTAMP(6)) AS VARCHAR), ' ', 'T')",
            ),
            "bytea": "'\\x' || lower(to_hex({x}))",
            "float": _TRINO_FINITE,
            "double": _TRINO_FINITE,
        },
    ),
}
# REQ-2050: an instant's element is a timestamp element whose native type has a time zone -- the
# same form, which takes it to UTC before writing it.
for _backend in _BACKENDS.values():
    _backend.elements["timestamptz"] = _backend.elements["timestamp"]
_BACKENDS["postgresql"] = _BACKENDS["postgres"]


def to_json_sql(col: str, nested: NestedColumn, dialect: str, column_name: str) -> str:
    """The backend's own expression, in its dialect, that hands ``col`` over as JSON."""
    backend = _BACKENDS.get(dialect)
    if backend is None:
        raise NativeJsonUnsupported(dialect, column_name, nested.native_type, "no to-JSON form")
    if nested.array_depth == 0 or nested.element_type is None:
        return backend.to_json.format(col=col)
    base = _base(nested.element_type)
    if base not in backend.elements:
        raise NativeJsonUnsupported(
            dialect,
            column_name,
            nested.native_type,
            f"no verified form for an element of type {nested.element_type}",
        )
    form = backend.elements[base]
    element = form(nested) if callable(form) else form
    if element is None:
        return backend.to_json.format(col=col)
    return backend.to_json.format(col=backend.each(col, nested.array_depth, element))


def resolve_native_json(sql: str, dialect: str) -> str:
    """``sql`` (already in ``dialect``) with every marker replaced by that backend's to-JSON
    expression. A statement with no marker is returned as given, unparsed."""
    if MARKER not in sql.lower():
        return sql
    tree = sqlglot.parse_one(sql, read=dialect)
    for node in list(tree.find_all(exp.Anonymous)):
        marked = marked_column(node)
        if marked is None:
            continue
        column, nested = marked
        rendered = to_json_sql(column.sql(dialect=dialect), nested, dialect, column.name)
        node.replace(sqlglot.parse_one(rendered, read=dialect))
    return tree.sql(dialect=dialect)


def strip_native_json(tree: exp.Expr, served_as_json: Callable[[exp.Expr], bool]) -> None:
    """Replace, in place, each marker whose column ``served_as_json`` accepts with the bare
    column: the address it is read from already holds JSON (a replica), so no cast applies."""
    for node in list(tree.find_all(exp.Anonymous)):
        marked = marked_column(node)
        if marked is not None and served_as_json(marked[0]):
            node.replace(marked[0])


def source_projection(columns: list, dialect: str, left_out: frozenset[str] = frozenset()) -> str:
    """The select list that reads a source table's rows for landing (REQ-1965): ``*`` for a table
    with no natively nested or subtyped column; otherwise every data column by name, each natively
    nested one through this backend's to-JSON expression under its own name -- so the store
    receives JSON, produced by the source or engine that holds the native value. ``columns`` are
    the registered columns (``name``, ``native_filter_type``, the three hidden attributes and the
    subtype).

    REQ-2171: a subtyped column is read in its subtype's own form (``ir_types.landing_form``); one
    in ``left_out`` -- which a reader of its source has no such form for -- is not read at all."""
    from provisa.core.ir_types import landing_form

    data = [
        c
        for c in columns
        if getattr(c, "native_filter_type", None) is None and c.name not in left_out
    ]
    if not left_out and not any(
        getattr(c, "native_type", None) or getattr(c, "subtype", None) for c in data
    ):
        return "*"
    parts: list[str] = []
    for c in data:
        quoted = exp.to_identifier(c.name, quoted=True).sql(dialect=dialect)
        native = getattr(c, "native_type", None)
        subtype = getattr(c, "subtype", None)
        if subtype:
            form = landing_form(subtype, dialect)
            if form is None:
                raise ValueError(
                    f"column {c.name}: a {subtype} has no form to be landed in from {dialect}"
                )
            parts.append(f"{form.format(col=quoted)} AS {quoted}")
            continue
        if not native:
            parts.append(quoted)
            continue
        nested = NestedColumn(native, getattr(c, "array_depth", 0) or 0, c.element_type)
        parts.append(f"{to_json_sql(quoted, nested, dialect, c.name)} AS {quoted}")
    return ", ".join(parts)
