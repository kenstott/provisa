# Copyright (c) 2026 Kenneth Stott
# Canary: bd190f8f-79d1-4164-a8b9-585f54dac7dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``db2`` SQLGlot dialect: IBM Db2 for Linux, UNIX and Windows 11.5, the SQL a Db2 federation
engine's statements are written in (REQ-2236, REQ-2167). Defining the class registers the name.

SQLGlot has no Db2 dialect. This one subclasses SQLGlot's ANSI dialect, the closest: Db2's SQL is
the standard's (double-quoted identifiers, ``||``, ``FETCH FIRST``, ``VALUES``, ``IS DISTINCT
FROM``), and every PostgreSQL-only form SQLGlot's PostgreSQL generator would write is avoided.

Exact or refused (REQ-2236): a statement is written for Db2 only when each construct in it has a
form there that gives PostgreSQL's answer. Each node of the statement must be of a kind listed in
:data:`_EXACT`, or have its own check; any other construct is refused by name (:class:`Db2Refused`)
before any SQL is written, so no construct reaches Db2 in a form whose answer may differ. Each
refusal, its reason, and the SQLSTATE and message a client receives are rows of the polyfill grid
(provisa/transpiler/polyfill/exact_engines.py), which :func:`refused` reads, and are listed on
the engine's page (docs/conformance/polyfill/). Among the refusals, each with its reason:

- ``avg``: Db2's AVG of an integer column is an integer (truncated); PostgreSQL's is numeric.
- ``greatest``/``least``/``concat``: Db2 returns NULL for any NULL argument; PostgreSQL skips it.
- ``round``/``power``: Db2's result type follows its integer arguments; PostgreSQL's does not.
- intervals, ``date_trunc``, ``current_timestamp``/``current_date``: Db2 has no interval type,
  its labeled durations keep a date a date, and its clock is the server's zone, not the session's.
- casts to ``numeric`` without a precision (Db2's default is ``DECIMAL(5,0)``), ``time`` (Db2's has
  no fractional seconds), ``timestamptz`` (Db2 LUW has none), ``bytea``, ``uuid``, JSON and arrays.
- regular expressions: Db2's REGEXP_LIKE is ICU's syntax, not PostgreSQL's.
- a cast to an integer type of a value not known to be integral: Db2 truncates a fraction
  (``CAST(2.5 AS INTEGER)`` is 2), PostgreSQL rounds (3) (provisa/core/sql_integral.py).

Forms that differ in spelling only: ``LIMIT n OFFSET m`` is ``OFFSET m ROWS FETCH FIRST n ROWS
ONLY``; a SELECT with no FROM reads ``SYSIBM.SYSDUMMY1``; ``a % b`` is ``MOD(a, b)``; ``ILIKE`` is
``LOWER(a) LIKE LOWER(b)``; a LIKE with no ESCAPE is given PostgreSQL's backslash; string length,
position and substring count characters (``CODEUNITS32``), as PostgreSQL's do, never bytes;
``text`` is ``VARCHAR(8168 CODEUNITS32)``, Db2's longest VARCHAR; every identifier is quoted, for
Db2 folds an unquoted name to upper case while Provisa's tables are created with quoted lower-case
names (provisa/federation/materialize_exec.py)."""

# Requirements: REQ-2236, REQ-2167, REQ-1922
from __future__ import annotations

import functools
from typing import Any

import sqlglot.expressions as exp
from sqlglot import generator
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy

from provisa.core.sql_integral import fractional_integer_cast

#: The dialect's name, as ``source_registry.engine_dialect`` returns it for a Db2 engine.
NAME = "db2"

#: Db2's longest VARCHAR in characters (32672 bytes over 4-byte code units).
_VARCHAR_MAX = 8168
#: Db2's largest DECIMAL precision.
_DECIMAL_MAX = 31

#: The node kinds whose Db2 form is the one SQLGlot's ANSI generator writes, and whose answer is
#: PostgreSQL's: the statement's structure, comparisons, logic and the aggregates that agree.
_EXACT: tuple[type[exp.Expr], ...] = (
    exp.Select,
    exp.From,
    exp.Join,
    exp.Where,
    exp.Group,
    exp.Having,
    exp.Order,
    exp.Ordered,
    exp.Limit,
    exp.Offset,
    exp.Fetch,
    exp.With,
    exp.CTE,
    exp.Subquery,
    exp.Union,
    exp.Intersect,
    exp.Except,
    exp.Values,
    exp.Tuple,
    exp.Table,
    exp.TableAlias,
    exp.Alias,
    exp.Column,
    exp.Identifier,
    exp.Star,
    exp.Dot,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Paren,
    exp.And,
    exp.Or,
    exp.Not,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
    exp.In,
    exp.Between,
    exp.Exists,
    exp.NullSafeEQ,
    exp.NullSafeNEQ,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Neg,
    exp.Mod,
    exp.DPipe,
    exp.Like,
    exp.ILike,
    exp.Case,
    exp.If,
    exp.Coalesce,
    exp.Nullif,
    exp.Count,
    exp.Sum,
    exp.Min,
    exp.Max,
    exp.Distinct,
    exp.Window,
    exp.WindowSpec,
    exp.RowNumber,
    exp.Rank,
    exp.DenseRank,
    exp.Lower,
    exp.Upper,
    exp.Replace,
    exp.Abs,
    exp.Floor,
    exp.Placeholder,
    exp.Parameter,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Schema,
    exp.DataTypeParam,
    exp.Var,
)

#: The fields EXTRACT reads in Db2 with PostgreSQL's numbering.
_EXTRACT_FIELDS = frozenset({"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"})

#: Each PostgreSQL type Db2 holds with PostgreSQL's values, as Db2 names it; None where the type
#: takes its parameters as written (checked in :func:`_type_refusal`).
_TYPES: dict[exp.DType, str | None] = {
    exp.DType.BOOLEAN: "BOOLEAN",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INTEGER",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "REAL",
    exp.DType.DOUBLE: "DOUBLE",
    exp.DType.DECIMAL: None,
    exp.DType.VARCHAR: None,
    exp.DType.CHAR: None,
    exp.DType.TEXT: f"VARCHAR({_VARCHAR_MAX} CODEUNITS32)",
    exp.DType.DATE: "DATE",
    exp.DType.TIMESTAMP: "TIMESTAMP(6)",
}


class Db2Refused(ValueError):
    """A construct with no form on Db2 that gives PostgreSQL's answer (REQ-2236): the statement
    is refused by name. Raised as ``function_macros.FunctionRefused``'s kind (:func:`refused`)."""


@functools.cache
def _refused_class() -> type[Exception]:
    """:class:`Db2Refused` as a ``FunctionRefused``, so every surface reports it as it reports a
    function the routed engine has no form of. Built on first use: the transpiler is not imported
    where the dialect is registered (provisa/core/source_registry.py)."""
    from provisa.transpiler.function_macros import FunctionRefused

    class _Refused(FunctionRefused, Db2Refused):
        def __init__(self, construct: str, sqlstate: str, message: str) -> None:
            FunctionRefused.__init__(self, construct, NAME)
            self.sqlstate = sqlstate
            self.args = (message,)

        def __str__(self) -> str:
            return str(self.args[0])

    return _Refused


def refused(construct: str, **named: str) -> Exception:
    """The refusal of ``construct`` on Db2: the polyfill grid's row that names it, its SQLSTATE and
    message (REQ-2236, THE REFUSAL LIVES IN THE GRID; provisa/transpiler/polyfill/exact_engines.py).
    Where the construct named depends on the statement, ``construct`` is the row's, braces and all
    (``"extract of {field}"``), and ``named`` fills them."""
    from provisa.transpiler.polyfill.exact_engines import refusal

    row = refusal(NAME, construct, **named)
    return _refused_class()(construct.format(**named), row.sqlstate, row.message)


def _name_of(node: exp.Expr) -> str:
    """How a refusal names ``node``: the function's name, or the construct's kind."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    if isinstance(node, exp.Func):
        names = type(node).sql_names()
        if names:
            return names[0].lower()
    return type(node).__name__.lower()


def _positive_integer(node: exp.Expr | None, minimum: int) -> bool:
    """Whether ``node`` is an integer literal of at least ``minimum``."""
    return (
        isinstance(node, exp.Literal)
        and not node.is_string
        and node.this.isdigit()
        and int(node.this) >= minimum
    )


def _param_value(dtype: exp.DataType, index: int) -> int | None:
    """The ``index``-th parameter of ``dtype`` as an integer; None where it has none."""
    params = dtype.expressions
    if len(params) <= index:
        return None
    value = params[index].this
    if isinstance(value, exp.Literal) and value.this.isdigit():
        return int(value.this)
    return None


def _type_refusal(dtype: exp.DataType) -> Exception | None:
    """The refusal of a value of ``dtype``, which has no Db2 form with PostgreSQL's values; None
    where it has one."""
    kind = dtype.this
    if kind not in _TYPES:
        return refused("the type {type}", type=dtype.sql(dialect="postgres").lower())
    if kind == exp.DType.DECIMAL:
        precision = _param_value(dtype, 0)
        if precision is None or precision > _DECIMAL_MAX:
            return refused("numeric without a precision of at most {max}", max=str(_DECIMAL_MAX))
    elif kind in (exp.DType.VARCHAR, exp.DType.CHAR) and dtype.expressions:
        length = _param_value(dtype, 0)
        if length is None or length > _VARCHAR_MAX:
            return refused(
                "{kind} longer than {max}", kind=kind.name.lower(), max=str(_VARCHAR_MAX)
            )
    return None


def _check(node: exp.Expr) -> None:
    """Refuse ``node`` by name where it has no exact Db2 form."""
    if isinstance(node, exp.DataType):
        why = _type_refusal(node)
        if why is not None:
            raise why
        return
    if isinstance(node, exp.Cast):
        if isinstance(node, exp.TryCast):
            raise refused("try_cast")
        if fractional_integer_cast(node):
            raise refused("a cast to an integer type of a value that may have a fraction")
        return
    if isinstance(node, exp.Length):
        if node.args.get("binary"):
            raise refused("octet_length")
        return
    if isinstance(node, exp.Substring):
        if not _positive_integer(node.args.get("start"), 1):
            raise refused("substring with a start that is not a literal of at least 1")
        length = node.args.get("length")
        if length is not None and not _positive_integer(length, 0):
            raise refused("substring with a length that is not a literal")
        return
    if isinstance(node, exp.StrPosition):
        if node.args.get("position") is not None or node.args.get("occurrence") is not None:
            raise refused("strpos from a position")
        return
    if isinstance(node, exp.Trim):
        if node.args.get("expression") is not None or node.args.get("collation") is not None:
            raise refused("trim of characters other than spaces")
        return
    if isinstance(node, exp.Extract):
        field = node.this.name.upper() if isinstance(node.this, (exp.Var, exp.Identifier)) else ""
        if field not in _EXTRACT_FIELDS:
            raise refused("extract of {field}", field=field.lower() or "a computed field")
        return
    if isinstance(node, exp.Ceil):
        if node.args.get("decimals") is not None:
            raise refused("ceil to decimals")
        return
    if isinstance(node, exp.Floor):
        if node.args.get("decimals") is not None:
            raise refused("floor to decimals")
        return
    if isinstance(node, exp.Distinct):
        if node.args.get("on") is not None:
            raise refused("distinct on")
        return
    if isinstance(node, exp.With):
        if node.args.get("recursive"):
            raise refused("with recursive")
        return
    if isinstance(node, exp.Escape):
        escape = node.expression
        if not (isinstance(escape, exp.Literal) and escape.is_string and len(escape.this) == 1):
            raise refused("an escape that is not one character")
        return
    if isinstance(node, _EXACT):
        return
    raise refused(_name_of(node))


def check(expression: exp.Expr) -> None:
    """Refuse ``expression`` by name where any construct in it has no exact Db2 form."""
    for node in expression.walk():
        _check(node)


def _check_parameters(expression: exp.Expr) -> None:
    """Refuse parameters Db2's positional markers cannot bind as PostgreSQL's would: each ``$n``
    is written ``?``, so they must appear once each, in order, and none in a paging clause (which
    Db2 writes in another order than PostgreSQL); a named parameter has no positional form."""
    numbers: list[int] = []
    for node in expression.walk(bfs=False):
        if isinstance(node, exp.Placeholder):
            if node.this is not None:
                raise refused("a named parameter")
            numbers.append(len(numbers) + 1)
        elif isinstance(node, exp.Parameter):
            if node.find_ancestor(exp.Limit, exp.Offset, exp.Fetch) is not None:
                raise refused("a parameter in LIMIT or OFFSET")
            value = node.this
            if not (isinstance(value, exp.Literal) and value.this.isdigit()):
                raise refused("a named parameter")
            numbers.append(int(value.this))
    if numbers != list(range(1, len(numbers) + 1)):
        raise refused("parameters not each used once in order")


class Db2Generator(generator.Generator):
    """Writes a statement :func:`check` accepts in Db2's SQL."""

    LIMIT_FETCH = "FETCH"
    NULL_ORDERING_SUPPORTED = True
    TABLESAMPLE_SIZE_IS_ROWS = False

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.Mod: lambda self, e: self.func("MOD", e.this, e.expression),
        exp.Ceil: lambda self, e: self.func("CEILING", e.this),
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Db2 folds an unquoted name to upper case; Provisa's tables are created quoted lower case.
        kwargs["identify"] = True
        super().__init__(*args, **kwargs)

    def generate(self, expression: exp.Expr, copy: bool = True) -> str:
        check(expression)
        _check_parameters(expression)
        return super().generate(expression, copy=copy)

    def parameter_sql(self, expression: exp.Parameter) -> str:
        return "?"  # ibm_db's paramstyle is qmark; their order is checked in _check_parameters

    def placeholder_sql(self, expression: exp.Placeholder) -> str:
        return "?"

    def offset_sql(self, expression: exp.Offset) -> str:
        return f"{super().offset_sql(expression)} ROWS"

    def select_sql(self, expression: exp.Select) -> str:
        if expression.args.get("from_") is None:
            expression = expression.copy()
            expression.set("from_", exp.From(this=exp.table_("SYSDUMMY1", db="SYSIBM")))
        return super().select_sql(expression)

    def datatype_sql(self, expression: exp.DataType) -> str:
        kind = expression.this
        name = _TYPES.get(kind)
        if name is not None:
            return name
        if kind == exp.DType.DECIMAL:
            return f"DECIMAL({self.expressions(expression, flat=True)})"
        if kind in (exp.DType.VARCHAR, exp.DType.CHAR):
            word = "VARCHAR" if kind == exp.DType.VARCHAR else "CHAR"
            length = _param_value(expression, 0)
            if length is None:  # varchar with no length is PostgreSQL's unbounded text
                return f"VARCHAR({_VARCHAR_MAX} CODEUNITS32)"
            return f"{word}({length} CODEUNITS32)"
        raise refused("the type {type}", type=kind.name.lower())

    def length_sql(self, expression: exp.Length) -> str:
        return f"CHARACTER_LENGTH({self.sql(expression, 'this')}, CODEUNITS32)"

    def substring_sql(self, expression: exp.Substring) -> str:
        args = [self.sql(expression, "this"), self.sql(expression, "start")]
        if expression.args.get("length") is not None:
            args.append(self.sql(expression, "length"))
        return f"SUBSTRING({', '.join(args)}, CODEUNITS32)"

    def strposition_sql(self, expression: exp.StrPosition) -> str:
        needle = self.sql(expression, "substr")
        haystack = self.sql(expression, "this")
        return f"POSITION({needle} IN {haystack} USING CODEUNITS32)"

    def trim_sql(self, expression: exp.Trim) -> str:
        position = (expression.text("position") or "BOTH").upper()
        return f"TRIM({position} FROM {self.sql(expression, 'this')})"

    def like_sql(self, expression: exp.Like) -> str:
        return self._escaped(expression, super().like_sql(expression))

    def ilike_sql(self, expression: exp.ILike) -> str:
        lowered = exp.Like(
            this=exp.Lower(this=expression.this.copy()),
            expression=exp.Lower(this=expression.expression.copy()),
        )
        return self._escaped(expression, super().like_sql(lowered))

    def _escaped(self, expression: exp.Expr, written: str) -> str:
        """``written`` with PostgreSQL's backslash escape, where the LIKE names none."""
        if isinstance(expression.parent, exp.Escape):
            return written
        return f"{written} ESCAPE '\\'"

    def if_sql(self, expression: exp.If) -> str:
        case = exp.Case(ifs=[exp.If(this=expression.this, true=expression.args.get("true"))])
        false = expression.args.get("false")
        if false is not None:
            case.set("default", false)
        return self.case_sql(case)


class Db2(Dialect):
    """IBM Db2 LUW 11.5: NULL sorts after every value ascending, as in PostgreSQL; an unquoted
    name is folded to upper case."""

    NULL_ORDERING = "nulls_are_large"
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    Generator = Db2Generator
