# Copyright (c) 2026 Kenneth Stott
# Canary: 7281d92a-f383-4494-8a2c-4db5681d4f83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``saphana`` SQLGlot dialect: SAP HANA 2.0 and HANA Cloud, the SQL a HANA federation
engine's statements are written in (REQ-2236, REQ-2167). Defining the class registers the name.

SQLGlot has no HANA dialect. This one subclasses SQLGlot's ANSI dialect, the closest: HANA's SQL
is the standard's for what is written here (double-quoted identifiers, ``||``, ``CASE``,
``EXISTS``), and every PostgreSQL-only form SQLGlot's PostgreSQL generator would write is avoided.
Each HANA behaviour relied on is from SAP's HANA SQL reference, not yet verified live.

Exact or refused (REQ-2236): a statement is written for HANA only when each construct in it has a
form there that gives PostgreSQL's answer. Each node of the statement must be of a kind listed in
:data:`_EXACT`, or have its own check; any other construct is refused by name (:class:`HanaRefused`)
before any SQL is written, so no construct reaches HANA in a form whose answer may differ. Among the
refusals, each with its reason:

- ``/``: HANA's quotient of two integers is a decimal; PostgreSQL's is the truncated integer.
- ``avg``: HANA's AVG result type and scale are not PostgreSQL's numeric.
- ``greatest``/``least``/``concat``: HANA returns NULL for any NULL argument; PostgreSQL skips it.
- ``round``/``power``: HANA's result type follows its arguments; PostgreSQL's does not.
- intervals, ``date_trunc``, ``current_timestamp``/``current_date``: HANA has no interval type,
  and its clock is the server's zone, not the session's.
- a cast to an integer type of a value that may have a fraction (HANA truncates; PostgreSQL rounds
  a numeric and rounds a float half to even), to ``boolean`` of anything but a boolean (HANA reads
  another set of words than PostgreSQL), to ``numeric`` without a precision (HANA's is a decimal
  float), to ``char`` (blank padding), ``time``, ``timestamptz``, ``bytea``, ``uuid``, JSON and arrays.
- ``extract`` of ``second`` (its fraction), ``IS TRUE``/``IS FALSE``, ``INTERSECT ALL``/
  ``EXCEPT ALL``, a ``RANGE`` frame with an offset, ``FETCH ... WITH TIES``, ``OFFSET`` with no
  ``LIMIT``, ``UPDATE ... FROM``, ``DELETE ... USING``, and a parameter in ``LIMIT``/``OFFSET``.
- regular expressions: HANA's LIKE_REGEXPR is PCRE's syntax, not PostgreSQL's.

Forms that differ in spelling only: a SELECT with no FROM reads ``DUMMY``; ``a % b`` is ``MOD(a,
b)``; ``ILIKE`` is ``LOWER(a) LIKE LOWER(b)``; a LIKE with no ESCAPE is given PostgreSQL's
backslash; ``strpos`` is ``LOCATE``; ``a IS NOT DISTINCT FROM b`` is ``((a = b AND a IS NOT NULL
AND b IS NOT NULL) OR (a IS NULL AND b IS NULL))``, never unknown, as PostgreSQL's; a predicate
used as a value (HANA takes a predicate only as a condition) is ``CASE WHEN p THEN TRUE WHEN NOT
(p) THEN FALSE END``, unknown where ``p`` is; a ``VALUES`` list is a ``UNION ALL`` of one-row
SELECTs from ``DUMMY`` (HANA has no row constructor list); ``varchar`` and ``text`` are
``NVARCHAR`` (HANA 2.0's ``VARCHAR`` holds ASCII only), ``text`` and an unbounded ``varchar`` at
HANA's longest, ``NVARCHAR(5000)``; every identifier is quoted, for HANA folds an unquoted name to
upper case while Provisa's tables are created with quoted lower-case names
(provisa/federation/materialize_exec.py)."""

# Requirements: REQ-2236, REQ-2167, REQ-1922
from __future__ import annotations

import functools
from typing import Any

import sqlglot.expressions as exp
from sqlglot import generator
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy

#: The dialect's name, as ``source_registry.engine_dialect`` returns it for a HANA engine.
NAME = "saphana"

#: HANA's longest NVARCHAR, in characters.
_NVARCHAR_MAX = 5000
#: HANA's largest DECIMAL precision.
_DECIMAL_MAX = 38

#: The node kinds whose HANA form is the one SQLGlot's ANSI generator (or a method below) writes,
#: and whose answer is PostgreSQL's: the statement's structure, comparisons, logic and the
#: aggregates that agree.
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
    exp.CTE,
    exp.Subquery,
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
    exp.In,
    exp.Between,
    exp.Exists,
    exp.NullSafeEQ,
    exp.NullSafeNEQ,
    exp.Add,
    exp.Sub,
    exp.Mul,
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
    exp.Window,
    exp.RowNumber,
    exp.Rank,
    exp.DenseRank,
    exp.Lower,
    exp.Upper,
    exp.Replace,
    exp.Abs,
    exp.Placeholder,
    exp.Parameter,
    exp.Insert,
    exp.Schema,
    exp.DataTypeParam,
    exp.Var,
)

#: The fields EXTRACT reads in HANA with PostgreSQL's numbering and value.
_EXTRACT_FIELDS = frozenset({"YEAR", "MONTH", "DAY", "HOUR", "MINUTE"})

#: Each PostgreSQL type HANA holds with PostgreSQL's values, as HANA names it; None where the type
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
    exp.DType.TEXT: f"NVARCHAR({_NVARCHAR_MAX})",
    exp.DType.DATE: "DATE",
    exp.DType.TIMESTAMP: "TIMESTAMP",
}

_INTEGER_TYPES = frozenset({exp.DType.SMALLINT, exp.DType.INT, exp.DType.BIGINT})

#: Where a predicate is a condition, as HANA takes one: the clause or argument that holds it.
_CONDITION_ARGS: dict[type[exp.Expr], str] = {
    exp.Where: "this",
    exp.Having: "this",
    exp.If: "this",
    exp.Join: "on",
}


class HanaRefused(ValueError):
    """A construct with no form on HANA that gives PostgreSQL's answer (REQ-2236): the statement
    is refused by name. Raised as ``function_macros.FunctionRefused``'s kind (:func:`refused`)."""


@functools.cache
def _refused_class() -> type[Exception]:
    """:class:`HanaRefused` as a ``FunctionRefused``, so every surface reports it as it reports a
    function the routed engine has no form of. Built on first use: the transpiler is not imported
    where the dialect is registered (provisa/core/source_registry.py)."""
    from provisa.transpiler.function_macros import FunctionRefused

    class _Refused(FunctionRefused, HanaRefused):
        def __init__(self, construct: str) -> None:
            FunctionRefused.__init__(self, construct, NAME)
            self.args = (f"{construct} has no form on SAP HANA that gives PostgreSQL's answer",)

        def __str__(self) -> str:
            return str(self.args[0])

    return _Refused


def refused(construct: str) -> Exception:
    """The refusal of ``construct`` on HANA."""
    return _refused_class()(construct)


def _name_of(node: exp.Expr) -> str:
    """How a refusal names ``node``: the function's name, or the construct's kind."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    if isinstance(node, exp.Div):
        return "/"
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


def _type_refusal(dtype: exp.DataType) -> str | None:
    """Why a value of ``dtype`` has no HANA form with PostgreSQL's values; None where it has one."""
    kind = dtype.this
    if kind not in _TYPES:
        return f"the type {dtype.sql(dialect='postgres').lower()}"
    if kind == exp.DType.DECIMAL:
        precision = _param_value(dtype, 0)
        if precision is None or precision > _DECIMAL_MAX:
            return f"numeric without a precision of at most {_DECIMAL_MAX}"
    elif kind == exp.DType.VARCHAR and dtype.expressions:
        length = _param_value(dtype, 0)
        if length is None or length > _NVARCHAR_MAX:
            return f"varchar longer than {_NVARCHAR_MAX}"
    return None


def _integral(node: exp.Expr) -> bool:
    """Whether ``node``'s value is known to be an integer: an integer literal, a count or rank, or
    a cast to an integer type."""
    if isinstance(node, exp.Paren):
        return _integral(node.this)
    if isinstance(node, exp.Literal):
        return not node.is_string and node.this.isdigit()
    if isinstance(node, (exp.Count, exp.RowNumber, exp.Rank, exp.DenseRank)):
        return True
    return isinstance(node, exp.Cast) and node.to.this in _INTEGER_TYPES


def _cast_refusal(node: exp.Cast) -> str | None:
    """Why ``node``'s conversion has no HANA form with PostgreSQL's answer; None where it has one."""
    if isinstance(node, exp.TryCast):
        return "try_cast"
    target = node.to.this
    if target in _INTEGER_TYPES and not _integral(node.this):
        return "a cast to an integer type of a value that may have a fraction"
    if target == exp.DType.BOOLEAN and not isinstance(node.this, (exp.Boolean, exp.Null)):
        return "a cast to boolean of a value that is not a boolean"
    return None


def _check(node: exp.Expr) -> None:  # noqa: C901 -- one branch per construct with a check
    """Refuse ``node`` by name where it has no exact HANA form."""
    if isinstance(node, exp.DataType):
        why = _type_refusal(node)
        if why is not None:
            raise refused(why)
        return
    if isinstance(node, exp.Cast):
        why = _cast_refusal(node)
        if why is not None:
            raise refused(why)
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
            raise refused(f"extract of {field.lower() or 'a computed field'}")
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
    if isinstance(node, exp.Is):
        if isinstance(node.expression, exp.Boolean):
            raise refused("is true or is false")
        return
    if isinstance(node, exp.SetOperation):
        if isinstance(node, (exp.Intersect, exp.Except)) and not node.args.get("distinct"):
            raise refused(f"{type(node).__name__.lower()} all")
        return
    if isinstance(node, exp.WindowSpec):
        if str(node.args.get("kind") or "").upper() == "RANGE" and any(
            not isinstance(node.args.get(bound), (str, type(None))) for bound in ("start", "end")
        ):
            raise refused("a range frame with an offset")
        return
    if isinstance(node, exp.LimitOptions):
        if node.args.get("with_ties"):
            raise refused("fetch with ties")
        if node.args.get("percent"):
            raise refused("fetch percent")
        return
    if isinstance(node, exp.Select):
        if node.args.get("offset") is not None and node.args.get("limit") is None:
            raise refused("offset without limit")
        return
    if isinstance(node, exp.Update):
        if node.args.get("from_"):
            raise refused("update from")
        return
    if isinstance(node, exp.Delete):
        if node.args.get("using"):
            raise refused("delete using")
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
    """Refuse ``expression`` by name where any construct in it has no exact HANA form."""
    for node in expression.walk():
        _check(node)


def _check_parameters(expression: exp.Expr) -> None:
    """Refuse parameters HANA's positional markers cannot bind as PostgreSQL's would: each ``$n``
    is written ``?``, so they must appear once each, in order, in the statement as written for
    HANA (a form above that repeats an operand repeats its parameter), and none in a paging clause;
    a named parameter has no positional form."""
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


# -- the rewrites to HANA's forms, on a statement check() accepted ----------------------------


def _not_distinct(left: exp.Expr, right: exp.Expr) -> exp.Expr:
    """``left IS NOT DISTINCT FROM right`` as HANA writes it, true or false, never unknown."""
    both_equal = exp.and_(
        exp.EQ(this=left.copy(), expression=right.copy()),
        exp.Not(this=exp.Is(this=left.copy(), expression=exp.Null())),
        exp.Not(this=exp.Is(this=right.copy(), expression=exp.Null())),
    )
    both_null = exp.and_(
        exp.Is(this=left.copy(), expression=exp.Null()),
        exp.Is(this=right.copy(), expression=exp.Null()),
    )
    return exp.Paren(this=exp.or_(both_equal, both_null))


def _null_safe(expression: exp.Expr) -> exp.Expr:
    """Each ``IS [NOT] DISTINCT FROM`` in ``expression`` in HANA's form."""

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.NullSafeEQ):
            return _not_distinct(node.this, node.expression)
        if isinstance(node, exp.NullSafeNEQ):
            return exp.Not(this=_not_distinct(node.this, node.expression))
        return node

    return expression.transform(rewrite, copy=False)


def _is_condition(node: exp.Expr) -> bool:
    """Whether the boolean ``node`` stands where HANA takes a predicate: a clause's or a CASE
    branch's condition, or an operand of AND, OR or NOT that stands there."""
    child, parent = node, node.parent
    while isinstance(parent, (exp.Paren, exp.And, exp.Or, exp.Not)):
        child, parent = parent, parent.parent
    if parent is None:
        return False
    if isinstance(parent, exp.Update) and child.arg_key == "expressions":
        return True  # an assignment, written as EQ
    arg = _CONDITION_ARGS.get(type(parent))
    return arg is not None and child.arg_key == arg


def _predicates_as_values(expression: exp.Expr) -> exp.Expr:
    """Each predicate in ``expression`` used as a value, as a CASE giving its truth value."""
    booleans = (exp.Predicate, exp.Connector, exp.Not)
    for node in list(expression.find_all(*booleans)):
        if isinstance(node.parent, booleans) and not isinstance(node.parent, exp.Paren):
            continue  # an operand of a predicate: its outermost predicate is the one looked at
        if _is_condition(node):
            continue
        case = exp.Case(
            ifs=[
                exp.If(this=node.copy(), true=exp.true()),
                exp.If(this=exp.Not(this=exp.Paren(this=node.copy())), true=exp.false()),
            ]
        )
        node.replace(case)
    return expression


def _one_row(row: exp.Tuple, names: list[str] | None) -> exp.Select:
    """A ``VALUES`` row as a one-row SELECT from ``DUMMY``, its columns named where ``names``."""
    values = [v.copy() for v in row.expressions]
    columns = (
        [exp.alias_(v, n, quoted=True) for v, n in zip(values, names, strict=True)]
        if names is not None
        else values
    )
    return exp.select(*columns).from_(exp.table_("DUMMY"))


def _rows(values: exp.Values, names: list[str] | None) -> exp.Expr:
    """``values``'s rows as a UNION ALL of one-row SELECTs, the first naming the columns."""
    rows = list(values.expressions)
    query: exp.Expr = _one_row(rows[0], names)
    for row in rows[1:]:
        query = exp.union(query, _one_row(row, None), distinct=False)
    return query


def _values_as_selects(expression: exp.Expr) -> exp.Expr:
    """Each ``VALUES`` list as HANA writes it: a single-row INSERT keeps its ``VALUES``; any other
    is a UNION ALL of one-row SELECTs, named as PostgreSQL names a ``VALUES`` list's columns."""
    for values in list(expression.find_all(exp.Values)):
        if isinstance(values.parent, exp.Insert) and len(values.expressions) == 1:
            continue
        if isinstance(values.parent, exp.Insert):
            values.replace(_rows(values, None))
            continue
        alias = values.args.get("alias")
        width = len(values.expressions[0].expressions)
        columns = [c.name for c in alias.columns] if alias is not None else []
        names = columns or [f"column{i}" for i in range(1, width + 1)]
        table_alias = exp.TableAlias(this=alias.this.copy()) if alias is not None else None
        values.replace(exp.Subquery(this=_rows(values, names), alias=table_alias))
    return expression


class SapHanaGenerator(generator.Generator):
    """Writes a statement :func:`check` accepts in HANA's SQL."""

    LIMIT_FETCH = "LIMIT"
    NULL_ORDERING_SUPPORTED = True
    TABLESAMPLE_SIZE_IS_ROWS = False

    TRANSFORMS = {
        **generator.Generator.TRANSFORMS,
        exp.Mod: lambda self, e: self.func("MOD", e.this, e.expression),
        exp.Ceil: lambda self, e: self.func("CEIL", e.this),
        exp.Length: lambda self, e: self.func("LENGTH", e.this),
        exp.StrPosition: lambda self, e: self.func("LOCATE", e.this, e.args.get("substr")),
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # HANA folds an unquoted name to upper case; Provisa's tables are created quoted lower case.
        kwargs["identify"] = True
        super().__init__(*args, **kwargs)

    def generate(self, expression: exp.Expr, copy: bool = True) -> str:
        check(expression)
        written = expression.copy()
        written = _predicates_as_values(_values_as_selects(_null_safe(written)))
        _check_parameters(written)
        return super().generate(written, copy=False)

    def parameter_sql(self, expression: exp.Parameter) -> str:
        return "?"  # hdbcli's paramstyle is qmark; their order is checked in _check_parameters

    def placeholder_sql(self, expression: exp.Placeholder) -> str:
        return "?"

    def select_sql(self, expression: exp.Select) -> str:
        if expression.args.get("from_") is None:
            expression = expression.copy()
            expression.set("from_", exp.From(this=exp.table_("DUMMY")))
        return super().select_sql(expression)

    def datatype_sql(self, expression: exp.DataType) -> str:
        kind = expression.this
        name = _TYPES.get(kind)
        if name is not None:
            return name
        if kind == exp.DType.DECIMAL:
            return f"DECIMAL({self.expressions(expression, flat=True)})"
        if kind == exp.DType.VARCHAR:
            length = _param_value(expression, 0)
            # varchar with no length is PostgreSQL's unbounded text, at HANA's longest
            return f"NVARCHAR({_NVARCHAR_MAX if length is None else length})"
        raise refused(f"the type {kind.name.lower()}")

    def substring_sql(self, expression: exp.Substring) -> str:
        args = [expression.this, expression.args.get("start"), expression.args.get("length")]
        return self.func("SUBSTRING", *args)

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


class SapHana(Dialect):
    """SAP HANA: NULL sorts before every value ascending (PostgreSQL's sorts after, so an ORDER BY
    is written with its NULLS clause); an unquoted name is folded to upper case."""

    NULL_ORDERING = "nulls_are_small"
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    Generator = SapHanaGenerator
