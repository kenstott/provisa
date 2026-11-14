# Copyright (c) 2026 Kenneth Stott
# Canary: 2d72e4da-408d-4aef-af31-339243502f6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic SQL's dialect: PostgreSQL text that keeps what the read means (REQ-778).

A statement is held as PostgreSQL-dialect text and read back and written again at every rewrite
(governance, RLS, routing). sqlglot writes a null-on-failure cast (``TryCast``) in that dialect as
a plain CAST, PostgreSQL having none, and the cast that answers null for a value that does not
convert (Cypher's toInteger, toFloat, toBoolean) then fails the statement instead. So the text
keeps TRY_CAST, which sqlglot reads back as the same cast, and each target writes its own: TRY_CAST,
SAFE_CAST, or for a PostgreSQL target ``pg_input_is_valid`` (``transpile.postgres_try_casts``).

A JSON value read whole (the root path, ``$``: Cypher's property of an unwound map) is written
``JSON_EXTRACT_PATH_TEXT(v, VARIADIC '{}')``, PostgreSQL's spelling, which sqlglot reads back as a
VARIADIC argument rather than the root path; every other target then wrote ``v ->> VARIADIC '{}'``.
So that spelling reads back as the root path again, and each target writes its own.

A filtered list (Cypher's ``[y IN l WHERE p]``) is kept ``FILTER(l, y -> p)``: PostgreSQL's
``ARRAY(SELECT y FROM UNNEST(l) ...)`` reads back as an array of a subquery, which no other target
writes. A PostgreSQL target writes its own (``transpile.postgres_array_filters``)."""

# Requirements: REQ-778

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.dialects.dialect import rename_func
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.presto import Presto
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.trino import Trino
from sqlglot.generator import Generator


Postgres.Generator.TRANSFORMS[exp.TryCast] = Generator.trycast_sql


def _flagged_regex(self: Generator, expression: exp.RegexpILike) -> str:
    """~* where the dialect has no case-insensitive match: sqlglot writes a function none has."""
    flagged = exp.DPipe(this=exp.Literal.string("(?i)"), expression=expression.expression.copy())
    return self.sql(exp.RegexpLike(this=expression.this.copy(), expression=flagged))


for _dialect in (Trino, Presto, Spark, BigQuery):
    _dialect.Generator.TRANSFORMS[exp.RegexpILike] = _flagged_regex
Trino.Generator.TRANSFORMS[exp.IsInf] = rename_func("IS_INFINITE")
Trino.Generator.TRANSFORMS[exp.Log] = lambda self, expression: (
    self.func("LOG", expression.this, expression.expression)
    if expression.expression
    else self.func("LOG", exp.Literal.number(10), expression.this)
)
#: The names that have one spelling on ClickHouse: its other functions take any case.
for _function, _name in (
    (exp.Cbrt, "cbrt"),
    (exp.Sinh, "sinh"),
    (exp.Cosh, "cosh"),
    (exp.Acosh, "acosh"),
    (exp.Asinh, "asinh"),
    (exp.Atanh, "atanh"),
):
    ClickHouse.Generator.TRANSFORMS[_function] = rename_func(_name)


def _split_part(self: Generator, expression: exp.SplitPart) -> str:
    """ClickHouse's parts are an array's elements: past the last one an empty value, as here."""
    value = self.sql(expression.this)
    delimiter = self.sql(expression.args["delimiter"])
    part = self.sql(expression.args["part_index"])
    # An Array cannot be Nullable, so the parts are of the value with its NULL taken out first.
    return (
        f"if(isNull({value}), NULL, "
        f"splitByString(assumeNotNull({delimiter}), assumeNotNull({value}))[{part}])"
    )


ClickHouse.Generator.TRANSFORMS[exp.SplitPart] = _split_part


def _truncated(self: Generator, expression: exp.TimestampTrunc) -> str:
    """A truncated timestamp, as the polyfill grid's builder writes it (a NULL part, which
    dateTrunc takes for no part at all, is a NULL)."""
    # Read when called: the grid's import writes PostgreSQL's SQL, which must follow this module's
    # transforms of it (TryCast above).
    from provisa.transpiler.polyfill import clickhouse

    return self.sql(clickhouse.truncated(expression.text("unit"), expression.this))


ClickHouse.Generator.TRANSFORMS[exp.TimestampTrunc] = _truncated


def _escaped(self: Generator, expression: exp.Escape) -> str:
    """LIKE ... ESCAPE '\\', as a form writes it: ClickHouse's LIKE has no ESCAPE clause, and a
    backslash escapes there already. A statement's own ESCAPE is read before (like_patterns)."""
    escape = expression.expression
    if not (isinstance(escape, exp.Literal) and escape.this == "\\"):
        from provisa.transpiler.function_macros import FunctionRefused

        raise FunctionRefused("like_escape", "clickhouse")
    return self.sql(expression.this)


ClickHouse.Generator.TRANSFORMS[exp.Escape] = _escaped

# REQ-2167: ClickHouse has no bitwise operator: & | ^ ~ << >> are syntax errors there, and a form of
# the old table (provisa.transpiler.function_macros) writes them in PostgreSQL's SQL. Each is
# ClickHouse's function of the same bits, of the operands' type.
for _operator, _name in (
    (exp.BitwiseAnd, "bitAnd"),
    (exp.BitwiseOr, "bitOr"),
    (exp.BitwiseXor, "bitXor"),
    (exp.BitwiseLeftShift, "bitShiftLeft"),
    (exp.BitwiseRightShift, "bitShiftRight"),
):
    ClickHouse.Generator.TRANSFORMS[_operator] = rename_func(_name)
ClickHouse.Generator.TRANSFORMS[exp.BitwiseNot] = lambda self, e: self.func("bitNot", e.this)
#: Integer division toward zero, as PostgreSQL's: ClickHouse's intDiv (it reads no INTDIV).
ClickHouse.Generator.TRANSFORMS[exp.IntDiv] = rename_func("intDiv")
#: The functions sqlglot writes by a name ClickHouse does not read, each by ClickHouse's own.
ClickHouse.Generator.TRANSFORMS[exp.CumeDist] = rename_func("cume_dist")
ClickHouse.Generator.TRANSFORMS[exp.Uuid] = lambda self, e: "generateUUIDv4()"
#: bool_and and bool_or: the least and the greatest of the booleans, NULL of none, as each
#: skips a NULL.
ClickHouse.Generator.TRANSFORMS[exp.LogicalAnd] = rename_func("min")
ClickHouse.Generator.TRANSFORMS[exp.LogicalOr] = rename_func("max")
ClickHouse.Generator.TRANSFORMS[exp.ArrayAppend] = rename_func("arrayPushBack")
ClickHouse.Generator.TRANSFORMS[exp.ArrayPrepend] = rename_func("arrayPushFront")
#: array_position: the first index of the value, NULL where it is not there (indexOf's 0).
ClickHouse.Generator.TRANSFORMS[exp.ArrayPosition] = lambda self, e: (
    f"nullIf(indexOf({self.sql(e.this)}, {self.sql(e.expression)}), 0)"
)


#: ClickHouse's functions whose name it reads in one case only, by the name in any case: a form's
#: call passes through PostgreSQL's text, which writes it upper-cased (pg_money's intDiv).
_CLICKHOUSE_SPELLINGS = {name.lower(): name for name in ("intDiv",)}


def _clickhouse_call(self: Generator, expression: exp.Anonymous) -> str:
    """ROW(...) as ClickHouse's tuple, which it compares field by field as PostgreSQL does a row;
    a call of a name ClickHouse reads in one case only, in that case; any other as it is."""
    name = expression.name.lower()
    if name == "row":
        return self.func("tuple", *expression.expressions)
    if name in _CLICKHOUSE_SPELLINGS:
        return self.func(_CLICKHOUSE_SPELLINGS[name], *expression.expressions, normalize=False)
    return self.anonymous_sql(expression)


ClickHouse.Generator.TRANSFORMS[exp.Anonymous] = _clickhouse_call


def _clickhouse_regexp_replace(self: Generator, expression: exp.RegexpReplace) -> str:
    """A form's regexp_replace(s, p, r, 'g'): every match, ClickHouse's replaceRegexpAll, which
    takes no flags. A statement's own call is read by the grid's row first (polyfill.regex)."""
    modifiers = expression.args.get("modifiers")
    if isinstance(modifiers, exp.Literal) and modifiers.is_string and modifiers.this == "g":
        return self.func(
            "replaceRegexpAll",
            expression.this,
            expression.expression,
            expression.args["replacement"],
        )
    return self.function_fallback_sql(expression)


ClickHouse.Generator.TRANSFORMS[exp.RegexpReplace] = _clickhouse_regexp_replace


def _binding(node: exp.Expr) -> tuple[exp.Expr, exp.Expr, str] | None:
    """A form's value held by a name (pg_forms_types._bound): ``(SELECT x FROM (SELECT v AS n)
    AS bn)``, as (x, v, n); None for any other node."""
    if not isinstance(node, exp.Subquery) or node.alias or not isinstance(node.this, exp.Select):
        return None
    select = node.this
    source = select.args.get("from_") or select.args.get("from")
    if len(select.expressions) != 1 or source is None or select.args.get("joins"):
        return None
    if any(select.args.get(k) for k in ("where", "group", "having", "order", "limit", "with_")):
        return None
    held = source.this
    if not isinstance(held, exp.Subquery) or not isinstance(held.this, exp.Select):
        return None
    inner = held.this
    if len(inner.expressions) != 1 or not isinstance(inner.expressions[0], exp.Alias):
        return None
    if any(inner.args.get(k) for k in ("from_", "from", "where", "joins", "group")):
        return None
    name = inner.expressions[0].alias
    if held.alias != f"b{name}":
        return None
    return select.expressions[0], inner.expressions[0].this, name


def _nested_binding(node: exp.Expr) -> bool:
    """Whether a binding holds another or is held by one: ClickHouse correlates a subquery with the
    query just outside it only."""
    inside = any(_binding(n) is not None for n in node.this.find_all(exp.Subquery))
    outside, held_by = False, node.parent
    while held_by is not None and not outside:
        outside = _binding(held_by) is not None
        held_by = held_by.parent
    return inside or outside


def _throw_of(branch: exp.Expr) -> exp.Anonymous | None:
    """The throwIf(1, ...) a CASE branch answers (provisa.transpiler.function_macros._thrown)."""
    node = branch.this if isinstance(branch, exp.Cast) else branch
    if isinstance(node, exp.Anonymous) and node.name == "throwIf" and node.expressions:
        first = node.expressions[0]
        if isinstance(first, exp.Literal) and first.this == "1":
            return node
    return None


def _thrown_when_chosen(tree: exp.Expr) -> exp.Expr:
    """Each CASE branch that throws, throwing only where the CASE chooses it: ClickHouse computes
    every branch of a CASE within a lambda, so a throwIf(1, ...) threw whatever was chosen."""
    for case in tree.find_all(exp.Case):
        earlier: list[exp.Expr] = []
        for branch in case.args.get("ifs") or []:
            throw = _throw_of(branch.args["true"])
            if throw is not None:
                chosen = exp.and_(
                    branch.this.copy(),
                    *(exp.not_(exp.func("ifNull", c.copy(), exp.false())) for c in earlier),
                )
                throw.set(
                    "expressions", [exp.func("ifNull", chosen, exp.false()), *throw.expressions[1:]]
                )
            earlier.append(branch.this)
    return tree


#: Marks a binding held by one written as a lambda, which is then a lambda too.
_IN_A_LAMBDA = "clickhouse_binding_in_a_lambda"


def _clickhouse_subquery(self: Generator, expression: exp.Subquery) -> str:
    """A form's nested binding of a value to a name, as ClickHouse's lambda of the name applied to
    the value (``arrayElement(arrayMap(n -> x, [v]), 1)``): an expression, which a lambda within it
    reads the name of, where a subquery within a subquery could not."""
    bound = _binding(expression)
    if bound is None or not (expression.meta.get(_IN_A_LAMBDA) or _nested_binding(expression)):
        return self.subquery_sql(expression)
    answer, value, name = bound
    for held in (*answer.find_all(exp.Subquery), *value.find_all(exp.Subquery)):
        if _binding(held) is not None:
            held.meta[_IN_A_LAMBDA] = True
    mapped = exp.Anonymous(
        this="arrayMap",
        expressions=[
            exp.Lambda(
                this=_thrown_when_chosen(answer.copy()), expressions=[exp.to_identifier(name)]
            ),
            exp.Array(expressions=[value.copy()]),
        ],
    )
    return self.sql(exp.Anonymous(this="arrayElement", expressions=[mapped, exp.Literal.number(1)]))


ClickHouse.Generator.TRANSFORMS[exp.Subquery] = _clickhouse_subquery


def _root_read_back(name: str) -> None:
    build = Postgres.Parser.FUNCTIONS[name]

    def _build(args: list) -> exp.Expr:
        whole = (
            len(args) == 2
            and isinstance(args[1], exp.Variadic)
            and isinstance(args[1].this, exp.Literal)
            and args[1].this.name == "{}"
        )
        return build(args[:1] if whole else args)

    Postgres.Parser.FUNCTIONS[name] = _build


_root_read_back("JSON_EXTRACT_PATH")
_root_read_back("JSON_EXTRACT_PATH_TEXT")

Postgres.Generator.TRANSFORMS[exp.ArrayFilter] = lambda self, e: self.func(
    "FILTER", e.this, e.expression
)


class ContainedByOrEqual(exp.Expression, exp.Binary, exp.Predicate):  # REQ-2171
    """PostgreSQL's ``a <<= b``: an address within or equal to a network."""


class ContainsOrEqual(exp.Expression, exp.Binary, exp.Predicate):  # REQ-2171
    """PostgreSQL's ``a >>= b``: a network containing or equal to an address."""


def _reads_containment(parser: type) -> None:
    """REQ-2171: ``<<=`` and ``>>=`` read as their own operators (they tokenize as ``<`` ``<=``
    and ``>`` ``>=``, which sqlglot refused). ``<<`` and ``>>`` read as sqlglot reads them."""
    from sqlglot.tokens import TokenType

    parse_bitwise = parser._parse_bitwise

    def _parse_bitwise_with_containment(self) -> exp.Expr | None:
        this = parse_bitwise(self)
        while True:
            if self._match_pair(TokenType.LT, TokenType.LTE):
                this = self.expression(
                    ContainedByOrEqual(this=this, expression=parse_bitwise(self))
                )
            elif self._match_pair(TokenType.GT, TokenType.GTE):
                this = self.expression(ContainsOrEqual(this=this, expression=parse_bitwise(self)))
            else:
                return this

    parser._parse_bitwise = _parse_bitwise_with_containment


_reads_containment(Postgres.Parser)
Postgres.Generator.TRANSFORMS[ContainedByOrEqual] = lambda self, e: self.binary(e, "<<=")
Postgres.Generator.TRANSFORMS[ContainsOrEqual] = lambda self, e: self.binary(e, ">>=")

# DuckDB's inet extension has the same two operators, which its statements are read back with.
from sqlglot.dialects.duckdb import DuckDB  # noqa: E402

_reads_containment(DuckDB.Parser)
DuckDB.Generator.TRANSFORMS[ContainedByOrEqual] = lambda self, e: self.binary(e, "<<=")
DuckDB.Generator.TRANSFORMS[ContainsOrEqual] = lambda self, e: self.binary(e, ">>=")


def _keyword_lambda_sql(
    self: Generator, expression: exp.Lambda, arrow_sep: str = "->", wrap: bool = True
) -> str:
    """DuckDB 2.0 binds a lambda only in its keyword form, ``LAMBDA x, y : body`` (the arrow form
    is a binder error there), so every lambda written for DuckDB takes it, however it was built.
    It is sqlglot's own spelling of a lambda read with the keyword (its ``colon`` form), written
    here without marking the tree."""
    return "LAMBDA " + Generator.lambda_sql(self, expression, arrow_sep=":", wrap=False)


DuckDB.Generator.lambda_sql = _keyword_lambda_sql  # type: ignore[method-assign]


_duckdb_json_arrow = DuckDB.Generator.TRANSFORMS[exp.JSONExtract]


def _json_extract_call(self: Generator, expression: exp.JSONExtract) -> str:
    """DuckDB's JSON extraction. ``v -> p`` still means extraction on 2.0, a step of no stated
    type keeps DuckDB's own operator, and the arrow answers as JSON_EXTRACT(v, p) does. Only where
    it is a call's argument is it written as the function: sqlglot reads ``f(v -> p)`` back as a
    lambda, which DuckDB 2.0 then binds as one (json_array_elements' unnest of
    json_extract(v, '$[*]') was written ``UNNEST(LAMBDA v : '$[*]')``)."""
    if isinstance(expression.parent, exp.Func):
        return self.func("JSON_EXTRACT", expression.this, expression.expression)
    return _duckdb_json_arrow(self, expression)


DuckDB.Generator.TRANSFORMS[exp.JSONExtract] = _json_extract_call

_postgres_json_arrow = Postgres.Generator.TRANSFORMS[exp.JSONExtract]


def _json_arrow_read_back(self: Generator, expression: exp.JSONExtract) -> str:
    """PostgreSQL's ``v -> p``, parenthesised where it is a call's argument: sqlglot reads
    ``f(v -> p)`` back as a lambda over ``v``, which DuckDB 2.0 writes ``LAMBDA v : p`` and cannot
    read inside a CAST (Cypher keys() of a relationship's properties). ``f((v -> p))`` reads back
    as the extraction, in sqlglot and in PostgreSQL."""
    written = _postgres_json_arrow(self, expression)
    return f"({written})" if isinstance(expression.parent, exp.Func) else written


Postgres.Generator.TRANSFORMS[exp.JSONExtract] = _json_arrow_read_back

from provisa.transpiler import xml_syntax as _xml_syntax  # noqa: E402, F401 -- PostgreSQL's XML
from provisa.transpiler import pg_syntax as _pg_syntax  # noqa: E402, F401 -- what sqlglot misreads

#: REQ-2167: ClickHouse has no inet type: an inet is held as its text there, as a cidr is
#: (pg_held_types), and pgwire names the column by the statement's cast.
ClickHouse.Generator.TYPE_MAPPING[exp.DataType.Type.INET] = "String"


def _clickhouse_cast(self: Generator, expression: exp.Cast) -> str:
    """A NULL cast to an array type as a bare NULL: ClickHouse has no NULL of an array (an Array is
    never Nullable), and its multiIf takes a NULL beside an array (run 43410717058: a server
    function's form of a NULL argument, pg_blocking_pids(NULL), failed on the cast)."""
    if isinstance(expression.this, exp.Null) and expression.to.is_type(exp.DataType.Type.ARRAY):
        return "NULL"
    return self.cast_sql(expression)


ClickHouse.Generator.TRANSFORMS[exp.Cast] = _clickhouse_cast


def _null_literal(node: exp.Expr) -> bool:
    """Whether ``node`` is NULL, through casts and parentheses."""
    while isinstance(node, (exp.Cast, exp.Paren)):
        node = node.this
    return isinstance(node, exp.Null)


def _decided(condition: exp.Expr) -> bool | None:
    """What a condition is whatever the row, where the statement's NULLs decide it: ``x IS NULL``
    of a NULL; None where a row decides it."""
    if isinstance(condition, exp.Paren):
        return _decided(condition.this)
    if isinstance(condition, exp.Is) and isinstance(condition.expression, exp.Null):
        if not _null_literal(condition.this):
            return None
        return not condition.args.get("negate")  # x IS NOT NULL is an Is negated
    if isinstance(condition, exp.Boolean):
        return bool(condition.this)
    if isinstance(condition, exp.Not):
        inner = _decided(condition.this)
        return None if inner is None else not inner
    if isinstance(condition, (exp.Or, exp.And)):
        parts = [_decided(condition.this), _decided(condition.expression)]
        if isinstance(condition, exp.Or):
            return True if True in parts else (False if parts == [False, False] else None)
        return False if False in parts else (True if parts == [True, True] else None)
    return None


def _clickhouse_case(self: Generator, expression: exp.Case) -> str:
    """A CASE whose first conditions the statement's NULLs decide, as the branch they choose:
    ClickHouse folds each branch's constants, a throw among them, whichever is chosen (run
    43410717058: pg_ls_dir(NULL) raised its refusal of a path)."""
    if expression.this is not None:
        return self.case_sql(expression)
    ifs = list(expression.args.get("ifs") or [])
    for at, branch in enumerate(ifs):
        decided = _decided(branch.this)
        if decided is None:
            if at == 0:
                return self.case_sql(expression)
            if _drops_an_aggregate(expression, ifs[at:]):
                return self.case_sql(expression)
            rest = expression.copy()
            rest.set("ifs", [b.copy() for b in ifs[at:]])
            return self.case_sql(rest)
        if decided:
            if _drops_an_aggregate(expression, [branch.args["true"]]):
                return self.case_sql(expression)
            return self.sql(branch.args["true"])
    default = expression.args.get("default")
    if _drops_an_aggregate(expression, [default] if default is not None else []):
        return self.case_sql(expression)
    return self.sql(default) if default is not None else "NULL"


def _drops_an_aggregate(case: exp.Case, kept: list[exp.Expr]) -> bool:
    """Whether folding ``case`` to ``kept`` leaves out an aggregate it holds: the statement would
    then be no aggregate, a row for each row where PostgreSQL answers one for the group
    (percentile_cont of a NULL fraction is CASE WHEN FALSE THEN its aggregate END)."""
    held = {id(n) for n in case.find_all(exp.AggFunc)}
    left = {id(n) for part in kept for n in part.find_all(exp.AggFunc)}
    return bool(held - left)


ClickHouse.Generator.TRANSFORMS[exp.Case] = _clickhouse_case


def _clickhouse_array_to_string(self: Generator, expression: exp.ArrayToString) -> str:
    """array_to_string with a text for NULL: each NULL element as that text, ClickHouse's
    arrayStringConcat taking no third argument (it skips a NULL, as the two-argument form does). A
    NULL text for NULL skips them too, as PostgreSQL's does."""
    null = expression.args.get("null")
    if null is None:
        return self.func("arrayStringConcat", expression.this, expression.expression)
    element = exp.to_identifier("_e")
    shown = exp.Anonymous(
        this="ifNull",
        expressions=[exp.Anonymous(this="toString", expressions=[exp.column("_e")]), null.copy()],
    )
    mapped = exp.Anonymous(
        this="arrayMap",
        expressions=[exp.Lambda(this=shown, expressions=[element]), expression.this.copy()],
    )
    return self.func("arrayStringConcat", mapped, expression.expression)


ClickHouse.Generator.TRANSFORMS[exp.ArrayToString] = _clickhouse_array_to_string
