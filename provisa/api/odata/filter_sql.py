# Copyright (c) 2026 Kenneth Stott
# Canary: 25ad721b-7ab0-4551-bda0-3fb576ac5dd6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ``$filter`` tree as the WHERE of a compiled read (REQ-2193).

The GraphQL ``where`` a read compiles from has no function of a column and no NOT, so the condition
is built as SQL tree nodes and added to the compiled statement, which then goes through
``validate_sql`` and ``apply_governance`` as any statement does: both read this condition as they
read a WHERE the compiler wrote. A masked column is refused here as everywhere (V005), and only a
faked column, whose predicate reads its fake, compares.

Nothing a client wrote reaches the statement's text. A string, date, time or guid is a bound value;
a number with no exponent or a boolean is a literal node built from the value the parser read, and
the exact digits of a decimal stay exact. A column name comes from the role's entity set.
"""

# Requirements: REQ-2193

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import sqlglot
from sqlglot import exp

from provisa.api.odata.errors import QueryOptionRefused
from provisa.api.odata.expression import Call, Comparison, In, Literal, Logical, Name, Node, Not
from provisa.api.odata.model import EntitySet
from provisa.compiler.sql_types import CompiledQuery
from provisa.compiler.sql_where import text_cast
from provisa.core.ir_types import to_ir

_LIKE_ESCAPE = "!"
_KINDS = {
    "Edm.Binary": "binary",
    "Edm.Boolean": "boolean",
    "Edm.Date": "date",
    "Edm.DateTimeOffset": "datetime",
    "Edm.Decimal": "number",
    "Edm.Double": "number",
    "Edm.Guid": "guid",
    "Edm.Int16": "number",
    "Edm.Int32": "number",
    "Edm.Int64": "number",
    "Edm.SByte": "number",
    "Edm.Single": "number",
    "Edm.String": "string",
    "Edm.TimeOfDay": "time",
}
# The type a value of a kind is bound as where no column says otherwise.
_BOUND_AS = {
    "string": "VARCHAR",
    "date": "DATE",
    "datetime": "TIMESTAMPTZ",
    "time": "TIME",
    "guid": "UUID",
}
_ORDERINGS = {"gt": exp.GT, "ge": exp.GTE, "lt": exp.LT, "le": exp.LTE}
_PARTS = {"year": "YEAR", "month": "MONTH", "day": "DAY"}


def _invalid(message: str) -> QueryOptionRefused:
    return QueryOptionRefused(400, "invalid_query_option", f"$filter: {message}")


@dataclass(frozen=True)
class _Typed:
    sql: exp.Expr
    kind: str
    data_type: str | None = None  # a column's registered type


def _utc_text(value: datetime) -> str:
    """The instant in UTC. A timestamp with no offset is read as UTC (``edm._datetime_offset``),
    so an offset in a literal is applied here, not dropped by a cast."""
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def _like_pattern(prefix: str, value: str, suffix: str) -> str:
    escaped = "".join(f"{_LIKE_ESCAPE}{c}" if c in (_LIKE_ESCAPE, "%", "_") else c for c in value)
    return f"{prefix}{escaped}{suffix}"


class _Condition:
    """Builds one condition, collecting what it binds. ``first`` is the number of its first
    placeholder: the compiled statement's own come before it."""

    def __init__(self, entity: EntitySet, first: int) -> None:
        self.entity = entity
        self.first = first
        self.bound: list[object] = []

    def _bind(self, value: object, sql_type: str | None) -> exp.Expr:
        self.bound.append(value)
        number = self.first + len(self.bound) - 1
        placeholder = exp.Parameter(this=exp.Literal.number(number))
        if sql_type is None:
            return placeholder
        return exp.Cast(this=placeholder, to=exp.DataType.build(sql_type, dialect="postgres"))

    def _literal(self, node: Literal, against: _Typed | None) -> _Typed:
        """The literal as a value compared with ``against``, whose column's type casts it."""
        value = node.value
        column_cast = text_cast(against.data_type) if against is not None else None
        if node.kind == "number":
            if isinstance(value, float):
                return _Typed(self._bind(value, None), "number")  # INF, NaN, an exponent
            assert isinstance(value, (int, Decimal))
            text = str(value) if isinstance(value, int) else format(value, "f")
            return _Typed(exp.Literal.number(text), "number")
        if node.kind == "boolean":
            return _Typed(exp.true() if value else exp.false(), "boolean")
        if node.kind == "string":
            assert isinstance(value, str)
            typed = against is not None
            return _Typed(
                self._bind(value, column_cast or (None if typed else "VARCHAR")), "string"
            )
        if node.kind == "guid":
            assert isinstance(value, UUID)
            return _Typed(self._bind(str(value), column_cast or "UUID"), "guid")
        if node.kind == "time":
            assert isinstance(value, time)
            return _Typed(self._bind(value.isoformat(), column_cast or "TIME"), "time")
        if node.kind == "date":
            assert isinstance(value, date)
            if against is not None and against.kind == "datetime":
                midnight = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
                return _Typed(self._bind(_utc_text(midnight), column_cast), "datetime")
            return _Typed(self._bind(value.isoformat(), column_cast or "DATE"), "date")
        assert isinstance(value, datetime)
        text = _utc_text(value)
        if column_cast is None or column_cast == "TIMESTAMPTZ":
            text += "+00:00"
        return _Typed(self._bind(text, column_cast or "TIMESTAMPTZ"), "datetime")

    def _column(self, node: Name) -> _Typed:
        found = self.entity.property(node.name)
        if found is None:
            # A column the role cannot see is unknown, as one that does not exist is.
            raise _invalid(f"no property {node.name!r} of {self.entity.name}")
        sql = exp.column(found.column, quoted=True)
        return _Typed(sql, _KINDS[found.type.name], found.data_type)

    def _name(self, node: Name) -> _Typed:
        """A column as a value: served as a string, it must also be one to the engine holding it."""
        column = self._column(node)
        data_type = column.data_type
        assert data_type is not None
        if column.kind == "string" and to_ir(data_type) != "text" and text_cast(data_type) is None:
            raise QueryOptionRefused(
                501, "not_implemented", f"filtering on {node.name} of {data_type} is not served"
            )
        return column

    def _call(self, node: Call) -> _Typed:
        if node.name in ("contains", "startswith", "endswith"):
            return self._text_search(node)
        argument = self.operand(node.args[0], None)
        if node.name in _PARTS:
            if argument.kind not in ("date", "datetime"):
                raise _invalid(f"{node.name} takes a date, given {argument.kind}")
            part = exp.var(_PARTS[node.name])
            return _Typed(exp.Extract(this=part, expression=argument.sql), "number")
        if argument.kind != "string":
            raise _invalid(f"{node.name} takes a string, given {argument.kind}")
        if node.name == "length":
            return _Typed(exp.Length(this=argument.sql), "number")
        lowered = node.name == "tolower"
        return _Typed((exp.Lower if lowered else exp.Upper)(this=argument.sql), "string")

    def _text_search(self, node: Call) -> _Typed:
        subject = self.operand(node.args[0], None)
        if subject.kind != "string":
            raise _invalid(f"{node.name} takes a string, given {subject.kind}")
        searched = node.args[1]
        if not isinstance(searched, Literal) or searched.kind != "string":
            raise QueryOptionRefused(
                501,
                "not_implemented",
                f"{node.name} of anything but a string literal is not served",
            )
        assert isinstance(searched.value, str)
        prefix = "" if node.name == "startswith" else "%"
        suffix = "" if node.name == "endswith" else "%"
        pattern = self._bind(_like_pattern(prefix, searched.value, suffix), None)
        like = exp.Like(this=subject.sql, expression=pattern)
        return _Typed(exp.Escape(this=like, expression=exp.Literal.string(_LIKE_ESCAPE)), "boolean")

    def operand(self, node: Node, against: _Typed | None) -> _Typed:
        """``node`` as a value."""
        if isinstance(node, Literal):
            if node.kind == "null":
                raise _invalid("null is compared with eq, ne or in")
            return self._literal(node, against)
        if isinstance(node, Name):
            return self._name(node)
        if isinstance(node, Call):
            return self._call(node)
        return _Typed(self.condition(node), "boolean")

    def _pair(self, left: Node, right: Node) -> tuple[_Typed, _Typed]:
        """Both sides of a comparison: a literal takes the type of what it is compared with."""
        if isinstance(left, Literal) and not isinstance(right, Literal):
            value = self.operand(right, None)
            return self.operand(left, value), value
        value = self.operand(left, None)
        return value, self.operand(right, value)

    def _compatible(self, left: _Typed, right: _Typed, operator: str) -> None:
        if left.kind != right.kind:
            raise _invalid(f"{left.kind} and {right.kind} are not compared by {operator}")

    def _comparison(self, node: Comparison) -> exp.Expr:
        left, right = node.left, node.right
        if isinstance(left, Literal) and left.kind == "null":
            left, right = right, left
        if isinstance(right, Literal) and right.kind == "null" and node.operator in ("eq", "ne"):
            both_null = isinstance(left, Literal) and left.kind == "null"
            if both_null:
                subject: exp.Expr = exp.Null()
            elif isinstance(left, Name):
                subject = self._column(left).sql  # any column may be null, whatever its type
            else:
                subject = self.operand(left, None).sql
            null = exp.Is(this=subject, expression=exp.Null())
            return null if node.operator == "eq" else exp.Not(this=null)
        a, b = self._pair(left, right)
        self._compatible(a, b, node.operator)
        if node.operator == "ne":
            return exp.NullSafeNEQ(this=a.sql, expression=b.sql)  # a null is not equal to a value
        if node.operator == "eq":
            literal = isinstance(left, Literal) or isinstance(right, Literal)
            kind = exp.EQ if literal else exp.NullSafeEQ  # two nulls are equal
            return kind(this=a.sql, expression=b.sql)
        return _ORDERINGS[node.operator](this=a.sql, expression=b.sql)

    def _in(self, node: In) -> exp.Expr:
        subject = self.operand(node.left, None)
        values = [i for i in node.items if not (isinstance(i, Literal) and i.kind == "null")]
        parts: list[exp.Expr] = []
        if values:
            typed = [self.operand(item, subject) for item in values]
            for item in typed:
                self._compatible(subject, item, "in")
            parts.append(exp.In(this=subject.sql, expressions=[t.sql for t in typed]))
        if len(values) < len(node.items):
            parts.append(exp.Is(this=subject.sql.copy(), expression=exp.Null()))
        return parts[0] if len(parts) == 1 else exp.paren(exp.or_(*parts, copy=False), copy=False)

    def _boolean(self, node: Node) -> exp.Expr:
        value = self.operand(node, None)
        if value.kind != "boolean":
            raise _invalid(f"a {value.kind} is not a condition")
        return value.sql

    def condition(self, node: Node) -> exp.Expr:
        """``node`` as a condition. ``not`` reads NULL as false: an OData comparison with a null is
        false, so its negation holds, where SQL's is NULL and NOT NULL would drop the row."""
        if isinstance(node, Comparison):
            return self._comparison(node)
        if isinstance(node, In):
            return self._in(node)
        if isinstance(node, Not):
            inner = self._boolean(node.operand)
            return exp.Not(this=exp.Coalesce(this=inner, expressions=[exp.false()]))
        if isinstance(node, Logical):
            joined = exp.and_ if node.operator == "and" else exp.or_
            parts = [self._boolean(operand) for operand in node.operands]
            return exp.paren(joined(*parts, copy=False, wrap=False), copy=False)
        return self._boolean(node)


def filter_condition(entity: EntitySet, tree: Node, first: int) -> tuple[exp.Expr, list[object]]:
    """``tree`` as a condition on one read of ``entity``, and what it binds from placeholder
    ``first``. A column is named alone: the select it joins reads that table first."""
    condition = _Condition(entity, first)
    return condition.condition(tree), condition.bound


def with_filter(compiled: CompiledQuery, entity: EntitySet, tree: Node | None) -> CompiledQuery:
    """``compiled`` with ``tree`` as its WHERE. The statement reads one table, named by its columns
    alone, so a column of the condition is named as they are."""
    if tree is None:
        return compiled
    where, bound = filter_condition(entity, tree, len(compiled.params) + 1)
    statement = sqlglot.parse_one(compiled.sql, read="postgres")
    if not isinstance(statement, exp.Select):
        raise QueryOptionRefused(
            501, "not_implemented", f"$filter on {entity.name} is not served: its read is no SELECT"
        )
    filtered = statement.where(where, copy=False)
    return replace(
        compiled,
        sql=filtered.sql(dialect="postgres"),
        params=[*compiled.params, *bound],
    )
