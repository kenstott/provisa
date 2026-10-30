# Copyright (c) 2026 Kenneth Stott
# Canary: b37a2c91-5e08-4d6f-a1c4-9f6e03d7c285
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's money on the routed dialect (REQ-2167, REQ-2171).

Money is stored and computed as its IR base, ``numeric(21,4)``. Two things here keep it money:

``money_type`` says what an expression is, by PostgreSQL's operators: money + money is money,
money * a number is money, money / a number is money, money / money a double. The surfaces that
describe a result read it, so ``price + price`` is money to the client whatever the engine's
decimal is.

``lowered`` writes a statement for the routed dialect, the one seam every route takes. PostgreSQL
has the type: a money column is given it and its own operators answer. Any other dialect gets the
operation over the base, in cents as cash.c computes it: a literal is read here as money's input,
a division by an integer truncates, a product or a quotient with a float is rounded half to even
in a double, a result outside int8 cents is PostgreSQL's error where the dialect can raise one.
A use PostgreSQL has no operator for is refused as PostgreSQL refuses it, wherever the statement
says the operand's type.

Refused by name, as every subtype's operation is (:class:`SubtypeOperationRefused`): an operation
the dialect has no form of."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Decimal

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.scope import Scope, traverse_scope

from provisa.core.ir_types import SUBTYPES
from provisa.core.pg_error import MARK, PostgresError
from provisa.core.pg_money import cash_in
from provisa.transpiler.native_subtype import COMPARISONS, SubtypeOperationRefused, marked

MONEY = "money"
DOUBLE = "double precision"

_MAX = Decimal("92233720368547758.07")
_MIN = Decimal("-92233720368547758.08")
#: 2**63: the first double outside int8.
_FLOAT_LIMIT = "9223372036854775808.0"
#: Cents, then dollars: what a value holds once it is whole cents.
_DOLLARS_TYPE = "DECIMAL(19,2)"

_T = exp.DataType.Type
_INTEGERS = (_T.TINYINT, _T.SMALLINT, _T.INT, _T.BIGINT)
_FLOATS = (_T.FLOAT, _T.DOUBLE, _T.DECIMAL)
#: The types money casts to, besides itself: PostgreSQL has no other.
_CASTS_TO = (_T.DECIMAL, _T.TEXT, _T.VARCHAR, _T.CHAR, _T.BPCHAR)
_PG_TYPE_NAMES = {
    _T.SMALLINT: "smallint",
    _T.INT: "integer",
    _T.BIGINT: "bigint",
    _T.FLOAT: "real",
    _T.DOUBLE: "double precision",
    _T.DECIMAL: "numeric",
    _T.TEXT: "text",
    _T.VARCHAR: "text",
    _T.BOOLEAN: "boolean",
}
#: The kinds of number money multiplies and divides by: PostgreSQL's int2, int4, int8 and float.
_INTEGER, _FLOAT = "integer", "float"
#: A dialect's function raising an error from a query, as a value of any type.
_RAISERS = {"duckdb": "error", "trino": "fail", "athena": "fail"}
#: A dialect's type names for an integer, as its typeof() says them.
_TYPEOF_DIALECTS = frozenset({"duckdb", "trino", "athena"})
_INTEGER_NAMES = ("tinyint", "smallint", "integer", "bigint", "hugeint")
#: The engines that may hand a money column over in the source's text form: an attached
#: PostgreSQL's money is a VARCHAR to DuckDB.
_TEXT_HELD = frozenset({"duckdb", "trino", "athena"})

_NOT_FOR_MONEY: dict[type, str] = {exp.Avg: "avg", exp.Abs: "abs", exp.Round: "round"}

#: The statements ``lowered`` has anything to do in.
_NAMED = re.compile(r"\bmoney\b|cash", re.IGNORECASE)


def _base() -> exp.DataType:
    return exp.DataType.build(SUBTYPES[MONEY].base, dialect="postgres")


def _no_column(_column: exp.Column) -> str | None:
    return None


ColumnType = Callable[[exp.Column], "str | None"]


def _pg_call(node: exp.Expr) -> exp.Anonymous | None:
    if isinstance(node, exp.Dot) and node.this.name.lower() == "pg_catalog":
        node = node.expression
    return node if isinstance(node, exp.Anonymous) else None


#: Each family function, by what it returns. ``money_type`` of a call of any other is None.
_RETURNS: dict[str, str | None] = {
    "cash_pl": MONEY,
    "cash_mi": MONEY,
    "cashlarger": MONEY,
    "cashsmaller": MONEY,
    "cash_in": MONEY,
    MONEY: MONEY,
    "cash_div_cash": DOUBLE,
    **{f"cash_{op}_{kind}": MONEY for op in ("mul", "div") for kind in ("flt4", "flt8")},
    **{f"cash_{op}_{kind}": MONEY for op in ("mul", "div") for kind in ("int2", "int4", "int8")},
    **{f"{kind}_mul_cash": MONEY for kind in ("flt4", "flt8", "int2", "int4", "int8")},
}


def _first_money(nodes: list[exp.Expr], column: ColumnType) -> str | None:
    kinds = {money_type(n, column) for n in nodes}
    return MONEY if MONEY in kinds else DOUBLE if DOUBLE in kinds else None


def _branches(node: exp.Expr) -> list[exp.Expr] | None:
    """The values a conditional or a choosing function answers one of; None for any other node."""
    if isinstance(node, exp.Case):
        values = [i.args.get("true") for i in node.args["ifs"]] + [node.args.get("default")]
        return [v for v in values if v is not None]
    if isinstance(node, exp.If):
        return [v for v in (node.args.get("true"), node.args.get("false")) if v is not None]
    if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
        return [node.this, *node.expressions]
    if isinstance(node, exp.Nullif):
        return [node.this]
    return None


def money_type(node: exp.Expr, column: ColumnType = _no_column) -> str | None:  # noqa: PLR0911
    """What ``node`` is by PostgreSQL's money operators: ``money``, ``double precision`` for an
    expression a money quotient decides, None for any other. ``column`` says a column's type."""
    if isinstance(node, (exp.Paren, exp.Alias)):
        return money_type(node.this, column)
    found = marked(node)
    if found is not None:
        return MONEY if found[1] == MONEY else None
    if isinstance(node, exp.Column):
        return column(node)
    if isinstance(node, exp.Cast):
        return MONEY if node.to.this == _T.MONEY else None
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
        left = money_type(node.this, column)
        right = money_type(node.expression, column)
        if DOUBLE in (left, right):
            return DOUBLE
        if isinstance(node, exp.Div) and left == right == MONEY:
            return DOUBLE
        return MONEY if MONEY in (left, right) else None
    if isinstance(node, (exp.Sum, exp.Max, exp.Min)):
        return MONEY if money_type(node.this, column) == MONEY else None
    if isinstance(node, exp.Subquery):
        query = node.this
        return money_type(query.selects[0], column) if isinstance(query, exp.Select) else None
    branches = _branches(node)
    if branches is not None:
        return _first_money(branches, column)
    call = _pg_call(node)
    return _RETURNS.get(call.name.lower()) if call is not None else None


def _source_type(source: Scope, name: str, column: ColumnType) -> str | None:
    """What a derived table's, a CTE's or a VALUES' column ``name`` is."""
    query = source.expression
    if isinstance(query, exp.Values):
        names = query.alias_column_names
        if name not in names:
            return None
        return money_type(query.expressions[0].expressions[names.index(name)], column)
    if not isinstance(query, exp.Query):
        return None
    for projection in query.selects:
        if projection.alias_or_name.lower() == name:
            return money_type(projection, column)
    return None


def names_money(sql: str) -> bool:
    """Whether ``sql`` can hold a money expression its text says: every other statement is left
    unread by what follows money through it."""
    return _NAMED.search(sql) is not None


def statement_columns(
    tree: exp.Expr, table_type: Callable[[exp.Table, str], "str | None"] = lambda _t, _n: None
) -> ColumnType:
    """What each column of ``tree`` is by money's operators, followed through the derived tables,
    CTEs and VALUES it reads; ``table_type`` says a table's column."""
    try:
        scopes = {id(scope.expression): scope for scope in traverse_scope(tree)}
    except OptimizeError:
        return _no_column  # no scopes to follow: only what is marked is known

    def column(node: exp.Column) -> str | None:
        scope = scopes.get(id(node.find_ancestor(exp.Select)))
        if scope is None:
            return None
        sources = scope.sources
        named = [sources[node.table]] if node.table in sources else list(sources.values())
        for source in named:
            typed = (
                table_type(source, node.name)
                if isinstance(source, exp.Table)
                else _source_type(source, node.name.lower(), column)
            )
            if typed is not None:
                return typed
        return None

    return column


# --- parts of an expression over the base --------------------------------------------------------


def _number(value: Decimal | int) -> exp.Expr:
    return exp.Literal.number(format(value, "f") if isinstance(value, Decimal) else value)


def _as(node: exp.Expr, type_: str | exp.DataType) -> exp.Cast:
    return exp.Cast(this=node, to=exp.DataType.build(type_, dialect="postgres"))


def _money_of(cents: int) -> exp.Expr:
    return exp.Cast(this=_number(Decimal(cents).scaleb(-2)), to=_base())


def _is_string(node: exp.Expr) -> bool:
    return isinstance(node, exp.Literal) and node.is_string


def _operand(node: exp.Expr) -> exp.Expr:
    """An operand of a money operator: a string literal is of unknown type, money's input."""
    return _money_of(cash_in(node.this)) if _is_string(node) else node


def _raises(sqlstate: str, message: str, dialect: str) -> exp.Expr | None:
    raiser = _RAISERS.get(dialect)
    if raiser is None:
        return None
    return exp.Anonymous(
        this=raiser, expressions=[exp.Literal.string(f"{MARK} {sqlstate}: {message}")]
    )


def _case(condition: exp.Expr, then: exp.Expr, otherwise: exp.Expr) -> exp.Expr:
    return exp.Case(ifs=[exp.If(this=condition, true=then)], default=otherwise)


def _in_range(value: exp.Expr, dialect: str) -> exp.Expr:
    """``value`` as the base, PostgreSQL's error where it is outside int8 cents."""
    refused = _raises("22003", "money out of range", dialect)
    if refused is None:
        return exp.Cast(this=value, to=_base())
    outside = exp.or_(
        exp.GT(this=value.copy(), expression=_number(_MAX)),
        exp.LT(this=value.copy(), expression=_number(_MIN)),
    )
    return exp.Cast(this=_case(outside, refused, value), to=_base())


def _cents(value: exp.Expr) -> exp.Expr:
    """A money value's whole cents, an int8."""
    product = exp.Mul(this=exp.Paren(this=value), expression=exp.Literal.number(100))
    return _as(exp.Round(this=product, decimals=exp.Literal.number(0)), "BIGINT")


def _cents_double(value: exp.Expr) -> exp.Expr:
    return _as(_cents(value), "DOUBLE PRECISION")


def _from_cents(cents: exp.Expr) -> exp.Expr:
    return exp.Mul(this=exp.Paren(this=cents), expression=exp.Literal.number("0.01"))


def _rint(value: exp.Expr, dialect: str) -> exp.Expr:
    """C's rint: to the nearest whole number, an exact half to the even one."""
    if dialect == "duckdb":
        return exp.Anonymous(this="round_even", expressions=[value, exp.Literal.number(0)])
    half = exp.EQ(
        this=exp.Sub(this=value.copy(), expression=exp.Floor(this=value.copy())),
        expression=exp.Literal.number("0.5"),
    )
    doubled = exp.Mul(
        this=exp.Literal.number(2),
        expression=exp.Round(
            this=exp.Div(this=value.copy(), expression=exp.Literal.number(2)),
            decimals=exp.Literal.number(0),
        ),
    )
    return _case(half, doubled, exp.Round(this=value, decimals=exp.Literal.number(0)))


def _rounded(cents: exp.Expr, dialect: str) -> exp.Expr:
    """A cents double, rounded as cash.c rounds it, as the base; PostgreSQL's error outside int8."""
    whole = _rint(cents, dialect)
    value = _from_cents(_as(whole, "NUMERIC(19,0)"))
    refused = _raises("22003", "money out of range", dialect)
    if refused is None:
        return exp.Cast(this=value, to=_base())
    limit = _as(exp.Literal.number(_FLOAT_LIMIT), "DOUBLE PRECISION")
    outside = exp.or_(
        exp.GTE(this=whole.copy(), expression=limit),
        exp.LT(this=whole.copy(), expression=exp.Neg(this=limit.copy())),
    )
    return exp.Cast(this=_case(outside, refused, value), to=_base())


def _nonzero(divisor: exp.Expr, dialect: str) -> exp.Expr:
    """``divisor``, PostgreSQL's error where it is zero."""
    refused = _raises("22012", "division by zero", dialect)
    if refused is None:
        return divisor
    return _case(
        exp.EQ(this=divisor.copy(), expression=exp.Literal.number(0)), refused, divisor.copy()
    )


def _number_kind(node: exp.Expr) -> str | None:
    """What the statement says of a number: an integer, a float (a numeric converts to one, as
    PostgreSQL converts it for money's operators), or None where it does not say."""
    if isinstance(node, (exp.Paren, exp.Neg)):
        return _number_kind(node.this)
    if isinstance(node, exp.Literal) and not node.is_string:
        return _INTEGER if node.is_int else _FLOAT
    if isinstance(node, exp.Cast):
        if node.to.this in _INTEGERS:
            return _INTEGER
        return _FLOAT if node.to.this in _FLOATS else None
    return None


def _by_kind(
    number: exp.Expr,
    dialect: str,
    operation: str,
    integer: Callable[[], exp.Expr],
    floating: Callable[[], exp.Expr],
) -> exp.Expr:
    """The form of an operation for ``number``'s kind: where the statement does not say it, the
    one the engine's typeof picks when it binds. A dialect with no typeof refuses by name."""
    kind = _number_kind(number)
    if kind == _INTEGER:
        return integer()
    if kind == _FLOAT:
        return floating()
    if dialect not in _TYPEOF_DIALECTS:
        raise SubtypeOperationRefused(MONEY, operation, dialect)
    typeof = exp.Lower(this=exp.Anonymous(this="typeof", expressions=[number.copy()]))
    names = [exp.Literal.string(n) for n in _INTEGER_NAMES]
    return _case(exp.In(this=typeof, expressions=names), integer(), floating())


def _integer_quotient(cents: exp.Expr, divisor: exp.Expr, dialect: str) -> exp.Expr:
    """Two integers divided, truncated toward zero. PostgreSQL's ``/`` of two integers reaches
    DuckDB as a double's division, and its ``//`` on a double does not truncate: its divide()
    is the integers' own."""
    if dialect == "duckdb":
        return exp.Anonymous(this="divide", expressions=[cents, divisor])
    return exp.Div(this=cents, expression=divisor)


def _multiply(value: exp.Expr, number: exp.Expr, dialect: str) -> exp.Expr:
    return _by_kind(
        number,
        dialect,
        "*",
        lambda: _in_range(exp.Mul(this=value.copy(), expression=number.copy()), dialect),
        lambda: _rounded(
            exp.Mul(this=_cents_double(value.copy()), expression=_as(number.copy(), "FLOAT8")),
            dialect,
        ),
    )


def _divide(value: exp.Expr, number: exp.Expr, dialect: str) -> exp.Expr:
    def integer() -> exp.Expr:
        divisor = _nonzero(_as(number.copy(), "BIGINT"), dialect)
        cents = _cents(value.copy())
        return exp.Cast(this=_from_cents(_integer_quotient(cents, divisor, dialect)), to=_base())

    def floating() -> exp.Expr:
        divisor = _nonzero(_as(number.copy(), "FLOAT8"), dialect)
        return _rounded(exp.Div(this=_cents_double(value.copy()), expression=divisor), dialect)

    return _by_kind(number, dialect, "/", integer, floating)


def _quotient(a: exp.Expr, b: exp.Expr, dialect: str) -> exp.Expr:
    """money / money: a double, the two cents counts divided."""
    return exp.Div(this=_cents_double(a), expression=_nonzero(_cents_double(b), dialect))


def _sign(a: exp.Expr, b: exp.Expr) -> exp.Expr:
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.LT(this=a.copy(), expression=b.copy()),
                true=_as(exp.Literal.number(-1), "INTEGER"),
            ),
            exp.If(
                this=exp.GT(this=a.copy(), expression=b.copy()),
                true=_as(exp.Literal.number(1), "INTEGER"),
            ),
            exp.If(
                this=exp.EQ(this=a.copy(), expression=b.copy()),
                true=_as(exp.Literal.number(0), "INTEGER"),
            ),
        ]
    )


def _chosen(a: exp.Expr, b: exp.Expr, larger: bool) -> exp.Expr:
    """cashlarger and cashsmaller: strict, so NULL for either NULL; the first of two equals."""
    first = (exp.GTE if larger else exp.LTE)(this=a.copy(), expression=b.copy())
    second = (exp.LT if larger else exp.GT)(this=a.copy(), expression=b.copy())
    return exp.Case(ifs=[exp.If(this=first, true=a), exp.If(this=second, true=b)])


def _money_text(value: exp.Expr, dialect: str) -> exp.Expr:
    """money's text form."""
    if dialect == "duckdb":
        return exp.Anonymous(this="provisa_cash_out", expressions=[_cents(value)])
    if dialect in ("trino", "athena"):
        unsigned = exp.Anonymous(
            this="format",
            expressions=[
                exp.Literal.string("$%,.2f"),
                exp.Abs(this=_as(value.copy(), _DOLLARS_TYPE)),
            ],
        )
        negative = exp.LT(this=value.copy(), expression=exp.Literal.number(0))
        sign = _case(negative, exp.Literal.string("-"), exp.Literal.string(""))
        return exp.DPipe(this=sign, expression=unsigned)
    raise SubtypeOperationRefused(MONEY, "cast to text", dialect)


def _money_words(value: exp.Expr, dialect: str) -> exp.Expr:
    if dialect == "duckdb":
        return exp.Anonymous(this="provisa_cash_words", expressions=[_cents(value)])
    raise SubtypeOperationRefused(MONEY, "cash_words", dialect)


def _money_bytes(value: exp.Expr, dialect: str) -> exp.Expr:
    """cash_send: the int8, big-endian."""
    if dialect == "duckdb":
        return exp.Anonymous(this="provisa_cash_send", expressions=[_cents(value)])
    if dialect in ("trino", "athena"):
        return exp.Anonymous(this="to_big_endian_64", expressions=[_cents(value)])
    raise SubtypeOperationRefused(MONEY, "cash_send", dialect)


def _money_input(value: exp.Expr, dialect: str) -> exp.Expr:
    """A value read as money: money's input of its text, which reads a number's digits exactly as
    numeric's cast to money rounds them."""
    if dialect == "duckdb":
        return exp.Anonymous(this="provisa_cash_in", expressions=[_as(value, "VARCHAR")])
    return _base_of_text(value)


def _base_of_text(value: exp.Expr) -> exp.Expr:
    """A value in a source's text form (``-$1,234.56``), or its number, as the base."""
    stripped: exp.Expr = _as(value, "VARCHAR")
    for symbol in ("$", ","):
        stripped = exp.Anonymous(
            this="REPLACE",
            expressions=[stripped, exp.Literal.string(symbol), exp.Literal.string("")],
        )
    return exp.Cast(this=stripped, to=_base())


def _column_value(column: exp.Expr, dialect: str) -> exp.Expr:
    """A money column as the routed dialect holds it, as money."""
    if dialect == "postgres":
        return _as(column, MONEY)
    return _base_of_text(column) if dialect in _TEXT_HELD else exp.Cast(this=column, to=_base())


# --- PostgreSQL's refusals -----------------------------------------------------------------------


def _type_said(node: exp.Expr) -> str | None:
    """PostgreSQL's name of ``node``'s type where the statement says it."""
    if isinstance(node, exp.Paren):
        return _type_said(node.this)
    if isinstance(node, exp.Literal) and not node.is_string:
        return "integer" if node.is_int else "numeric"
    if isinstance(node, exp.Cast):
        return _PG_TYPE_NAMES.get(node.to.this)
    return None


def _no_operator(left: str | None, symbol: str, right: str | None) -> PostgresError:
    named = " ".join(side for side in (left, symbol, right) if side)
    return PostgresError("42883", f"operator does not exist: {named}")


# --- the statement ------------------------------------------------------------------------------


class _Lowering:
    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        self.types: dict[int, str | None] = {}
        self.said: dict[int, str | None] = {}
        self.written: set[int] = set()

    def kind(self, node: exp.Expr) -> str | None:
        return self.types.get(id(node))

    def note(self, nodes: list[exp.Expr], column: ColumnType) -> None:
        for node in nodes:
            self.types[id(node)] = money_type(node, column)
            self.said[id(node)] = _type_said(node)

    def replaced(self, old: exp.Expr, new: exp.Expr) -> None:
        self.types[id(new)] = self.types.get(id(old))
        self.said[id(new)] = self.said.get(id(old))

    def _cast(self, node: exp.Cast) -> exp.Expr:
        value = node.this
        if node.to.this == _T.MONEY:
            return self._to_money(value)
        if self.kind(value) != MONEY:
            return node
        target = node.to.this
        if target not in _CASTS_TO:
            name = _PG_TYPE_NAMES.get(target) or node.to.sql(dialect="postgres").lower()
            raise PostgresError("42846", f"cannot cast type money to {name}")
        if target == _T.DECIMAL:
            dollars = _as(value, _DOLLARS_TYPE)
            if not node.to.expressions:
                return dollars
            return exp.Cast(this=dollars, to=node.to)
        return exp.Cast(this=_money_text(value, self.dialect), to=node.to)

    def _to_money(self, value: exp.Expr) -> exp.Expr:
        inner = value
        while isinstance(inner, exp.Paren):
            inner = inner.this
        if self.kind(inner) == MONEY:
            return inner
        if isinstance(inner, exp.Null) or (
            isinstance(inner, exp.Cast) and isinstance(inner.this, exp.Null)
        ):
            return exp.Cast(this=exp.Null(), to=_base())
        negative = isinstance(inner, exp.Neg)
        digits = inner.this if negative else inner
        if isinstance(digits, exp.Literal):
            text = f"{'-' if negative else ''}{digits.this}"
            return _money_of(cash_in(text))
        said = self.said.get(id(inner))
        if said not in (None, "integer", "bigint", "numeric", "text"):
            raise PostgresError("42846", f"cannot cast type {said} to money")
        return _money_input(value, self.dialect)

    def _arithmetic(self, node: exp.Expr, symbol: str) -> exp.Expr:
        left, right = node.this, node.expression
        left_money = self.kind(left) == MONEY
        right_money = self.kind(right) == MONEY
        a, b = _operand(left), _operand(right)
        if isinstance(node, (exp.Add, exp.Sub)):
            if not (left_money and (right_money or _is_string(right))) and not (
                right_money and _is_string(left)
            ):
                said = self.said.get(id(right if left_money else left))
                if said is not None:
                    names = (MONEY, said) if left_money else (said, MONEY)
                    raise _no_operator(names[0], symbol, names[1])
            kind = exp.Add if isinstance(node, exp.Add) else exp.Sub
            return _in_range(kind(this=a, expression=b), self.dialect)
        if isinstance(node, exp.Mul):
            value, number = (a, b) if left_money else (b, a)
            return _multiply(value, number, self.dialect)
        if left_money and right_money:
            return _quotient(a, b, self.dialect)
        if right_money:
            raise _no_operator(self.said.get(id(left)) or "unknown", "/", MONEY)
        return _divide(a, b, self.dialect)

    def _comparison(self, node: exp.Expr) -> exp.Expr:
        left, right = node.this, node.expression
        if MONEY not in (self.kind(left), self.kind(right)):
            return node
        for side, other in ((left, right), (right, left)):
            if self.kind(side) == MONEY and self.kind(other) != MONEY:
                said = self.said.get(id(other))
                if said is not None:
                    names = (MONEY, said) if side is left else (said, MONEY)
                    symbol = _SYMBOLS[type(node)]
                    raise _no_operator(names[0], symbol, names[1])
        node.set("this", _operand(left))
        node.set("expression", _operand(right))
        return node

    def _call(self, call: exp.Anonymous) -> exp.Expr | None:  # noqa: PLR0911, PLR0912 -- one per family
        name = call.name.lower()
        args = list(call.expressions)
        dialect = self.dialect
        if name == "pg_typeof" and len(args) == 1 and self.kind(args[0]) is not None:
            return _as(exp.Literal.string(str(self.kind(args[0]))), "REGTYPE")
        if name == "numeric" and len(args) == 1 and self.kind(args[0]) == MONEY:
            return _as(args[0], _DOLLARS_TYPE)
        if name not in _FAMILY:
            return None
        if name == MONEY and len(args) == 1:
            return self._to_money(args[0])
        if name == "cash_in" and len(args) == 1:
            return self._to_money(args[0])
        if name == "cash_out" and len(args) == 1:
            return _money_text(_operand(args[0]), dialect)
        if name == "cash_send" and len(args) == 1:
            return _money_bytes(_operand(args[0]), dialect)
        if name == "cash_words" and len(args) == 1:
            return _money_words(_operand(args[0]), dialect)
        if len(args) != 2:  # noqa: PLR2004 -- the rest are binary
            return None
        a, b = _operand(args[0]), _operand(args[1])
        comparison = _COMPARED.get(name)
        if comparison is not None:
            return comparison(this=a, expression=b)
        if name == "cash_pl":
            return _in_range(exp.Add(this=a, expression=b), dialect)
        if name == "cash_mi":
            return _in_range(exp.Sub(this=a, expression=b), dialect)
        if name == "cash_cmp":
            return _sign(a, b)
        if name in ("cashlarger", "cashsmaller"):
            return _chosen(a, b, name == "cashlarger")
        if name == "cash_div_cash":
            return _quotient(a, b, dialect)
        if name.startswith("cash_mul_"):
            return _multiply(a, _typed(b, name), dialect)
        if name.startswith("cash_div_"):
            return _divide(a, _typed(b, name), dialect)
        if name.endswith("_mul_cash"):
            return _multiply(b, _typed(a, name), dialect)
        return None

    def write(self, node: exp.Expr) -> exp.Expr:  # noqa: PLR0911 -- one per kind of node
        found = marked(node)
        if found is not None and found[1] == MONEY:
            return _column_value(found[0], self.dialect)
        if self.dialect == "postgres":
            return node  # every operation is PostgreSQL's own
        if isinstance(node, exp.Cast):
            return self._cast(node)
        if isinstance(node, exp.Neg) and self.kind(node.this) == MONEY:
            raise _no_operator(None, "-", MONEY)
        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            sides = (self.kind(node.this), self.kind(node.expression))
            if MONEY in sides:
                return self._arithmetic(node, _ARITHMETIC[type(node)])
            return node
        if isinstance(node, COMPARISONS):
            return self._comparison(node)
        if isinstance(node, (exp.Between, exp.In)) and self.kind(node.this) == MONEY:
            for key in ("low", "high"):
                if node.args.get(key) is not None:
                    node.set(key, _operand(node.args[key]))
            if node.expressions:
                node.set("expressions", [_operand(e) for e in node.expressions])
            return node
        if isinstance(node, tuple(_NOT_FOR_MONEY)) and self.kind(node.this) == MONEY:
            name = _NOT_FOR_MONEY[type(node)]
            raise PostgresError("42883", f"function {name}(money) does not exist")
        if self.kind(node) == MONEY and _branches(node) is not None:
            return _with_money_branches(node)
        if isinstance(node, exp.Dot) and id(node.expression) in self.written:
            return node.expression
        if isinstance(node, exp.Anonymous):
            return self._call(node) or node
        return node

    def run(self, tree: exp.Expr) -> exp.Expr:
        nodes = list(tree.walk(bfs=False))
        self.note(nodes, statement_columns(tree))
        for node in reversed(nodes):
            new = self.write(node)
            if new is node:
                continue
            self.replaced(node, new)
            self.written.add(id(new))
            if node is tree:
                tree = new
            else:
                node.replace(new)
        return tree


_SYMBOLS: dict[type, str] = {
    exp.EQ: "=",
    exp.NEQ: "<>",
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.NullSafeEQ: "=",
    exp.NullSafeNEQ: "<>",
}
_ARITHMETIC: dict[type, str] = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/"}
_COMPARED: dict[str, type[exp.Expr]] = {
    "cash_eq": exp.EQ,
    "cash_ne": exp.NEQ,
    "cash_lt": exp.LT,
    "cash_le": exp.LTE,
    "cash_gt": exp.GT,
    "cash_ge": exp.GTE,
}


#: Every function ``_Lowering._call`` answers.
_FAMILY = frozenset({*_RETURNS, *_COMPARED, "cash_cmp", "cash_out", "cash_send", "cash_words"})


def _typed(number: exp.Expr, name: str) -> exp.Expr:
    """A family function's number, of the kind its name says."""
    return _as(number, "BIGINT") if "int" in name else _as(number, "FLOAT8")


def _with_money_branches(node: exp.Expr) -> exp.Expr:
    """A choice among money values: a string literal among them is money's input."""
    if isinstance(node, exp.Case):
        for branch in node.args["ifs"]:
            branch.set("true", _operand(branch.args["true"]))
        if node.args.get("default") is not None:
            node.set("default", _operand(node.args["default"]))
    elif isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
        node.set("this", _operand(node.this))
        node.set("expressions", [_operand(e) for e in node.expressions])
    return node


def lowered(sql: str, dialect: str) -> str:
    """``sql`` (PostgreSQL's) with every money value and operation written for ``dialect``. A
    statement with none is returned as given, unparsed unless it names money."""
    if not names_money(sql):
        return sql
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return sql  # what reads it next reports it, or reads it its own way
    before = tree.sql(dialect="postgres")
    written = _Lowering(dialect).run(tree)
    after = written.sql(dialect="postgres")
    return sql if after == before else after


__all__ = ["DOUBLE", "MONEY", "lowered", "money_type", "names_money", "statement_columns"]
