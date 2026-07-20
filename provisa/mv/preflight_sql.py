# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7b0a52-6f19-4c8e-9a4b-2e5d1f0c7846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Push a SQL-expressible preflight check down to the engine (REQ-1165).

A preflight check whose body is a single quantified row assertion —

    def preflight(rows, ctx):
        if any(r["qty"] < 0 for r in rows):
            return ctx.abort("negative quantity")
        return ctx.ok()

— carries no cross-row state and reduces to one boolean predicate over a row. Such a check is
translated here to a governed-PostgreSQL WHERE fragment and evaluated ENGINE-SIDE as a
``SELECT count(*)`` probe over the MV's SELECT (the REQ-1165 pushdown path, mirroring
``_probe_source_count`` in :mod:`provisa.mv.refresh`), so a billion-row source is never pulled
into Python to be gated.

:func:`translate` returns a :class:`SqlPreflight` for the supported shape and ``None`` for
everything else — an unsupported check falls back to the Python+Arrow streaming path. The
translated predicate MUST be semantically equal to the Python one (REQ-964): only pure,
order-independent constructs are accepted, and anything ambiguous is rejected (returns None),
never guessed.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

from provisa.mv.preflight import CONTINUE, Decision, Verdict
from provisa.mv.preprocess import REQUIRED_FUNC

# Python comparator AST node -> SQL operator (None-compares are special-cased in the visitor).
_CMP_OPS: dict[type[ast.cmpop], str] = {
    ast.Eq: "=",
    ast.NotEq: "<>",
    ast.Lt: "<",
    ast.LtE: "<=",
    ast.Gt: ">",
    ast.GtE: ">=",
}
_BIN_OPS: dict[type[ast.operator], str] = {
    ast.Add: "+",
    ast.Sub: "-",
    ast.Mult: "*",
    ast.Div: "/",
    ast.Mod: "%",
}


class _Untranslatable(Exception):
    """An AST node outside the SQL-expressible subset — the check falls back to streaming."""


@dataclass(frozen=True)
class SqlPreflight:  # REQ-1165
    """A preflight check reduced to an engine-side count probe.

    ``quantifier`` is ``"any"`` or ``"all"``; ``predicate_sql`` is the WHERE fragment for the
    per-row predicate P (governed PostgreSQL dialect); ``violation`` is the verdict returned when
    the quantifier fires and ``passing`` when it does not.
    """

    quantifier: str
    predicate_sql: str
    violation: Verdict
    passing: Verdict

    def count_sql(self, select_sql: str) -> str:
        """The probe query whose scalar count decides the verdict (see :func:`evaluate`).

        ``any``: count rows matching P (violation iff > 0). ``all``: count rows VIOLATING P, i.e.
        matching ``NOT P`` (the ``all`` fires iff that count is 0 — vacuously true on an empty set,
        matching Python's ``all([])``)."""
        where = self.predicate_sql if self.quantifier == "any" else f"NOT ({self.predicate_sql})"
        return f"SELECT count(*) FROM ({select_sql}) AS _preflight WHERE {where}"

    def verdict_for(self, count: int) -> Verdict:
        """Map the probe's scalar count to the verdict."""
        fired = count > 0 if self.quantifier == "any" else count == 0
        return self.violation if fired else self.passing


def _col_ref(node: ast.Subscript) -> str:
    """Translate ``r["col"]`` / ``r['col']`` to a quoted SQL identifier."""
    if not (isinstance(node.value, ast.Name)):
        raise _Untranslatable("subscript base is not the row variable")
    key = node.slice
    if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
        raise _Untranslatable("row subscript is not a string-literal column")
    col = key.value.replace('"', '""')
    return f'"{col}"'


def _literal(value: object) -> str:
    """Translate a Python constant to a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise _Untranslatable(f"unsupported literal {value!r}")


class _PredicateTranslator(ast.NodeVisitor):
    """Translate a boolean/scalar expression over the row variable ``row_var`` to SQL text."""

    def __init__(self, row_var: str) -> None:
        self.row_var = row_var

    def visit(self, node: ast.AST) -> str:  # type: ignore[override]
        return super().visit(node)

    def generic_visit(self, node: ast.AST) -> str:  # type: ignore[override]
        raise _Untranslatable(f"unsupported expression: {type(node).__name__}")

    def visit_BoolOp(self, node: ast.BoolOp) -> str:
        op = " AND " if isinstance(node.op, ast.And) else " OR "
        return "(" + op.join(self.visit(v) for v in node.values) + ")"

    def visit_UnaryOp(self, node: ast.UnaryOp) -> str:
        if isinstance(node.op, ast.Not):
            return f"(NOT {self.visit(node.operand)})"
        if isinstance(node.op, ast.USub):
            return f"(-{self.visit(node.operand)})"
        raise _Untranslatable("unsupported unary operator")

    def visit_BinOp(self, node: ast.BinOp) -> str:
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise _Untranslatable("unsupported arithmetic operator")
        return f"({self.visit(node.left)} {op} {self.visit(node.right)})"

    def visit_Compare(self, node: ast.Compare) -> str:
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise _Untranslatable("chained comparison not supported")
        left, op_node, right = node.left, node.ops[0], node.comparators[0]
        # NULL comparisons: SQL requires IS [NOT] NULL, never = NULL.
        right_is_none = isinstance(right, ast.Constant) and right.value is None
        if right_is_none and isinstance(op_node, ast.Eq):
            return f"({self.visit(left)} IS NULL)"
        if right_is_none and isinstance(op_node, ast.NotEq):
            return f"({self.visit(left)} IS NOT NULL)"
        sql_op = _CMP_OPS.get(type(op_node))
        if sql_op is None:
            raise _Untranslatable("unsupported comparison operator")
        return f"({self.visit(left)} {sql_op} {self.visit(right)})"

    def visit_Subscript(self, node: ast.Subscript) -> str:
        if isinstance(node.value, ast.Name) and node.value.id != self.row_var:
            raise _Untranslatable("subscript on a non-row variable")
        return _col_ref(node)

    def visit_Constant(self, node: ast.Constant) -> str:
        return _literal(node.value)

    def visit_Name(self, node: ast.Name) -> str:
        # A bare name that is not the row variable has no SQL meaning (a free variable / helper).
        raise _Untranslatable(f"bare name {node.id!r} is not translatable")


def _verdict_from_return(node: ast.expr | None) -> Verdict:
    """Translate a preflight ``return`` expression to a verdict (ctx.ok/abort/quarantine, bool, str)."""
    if node is None:
        return CONTINUE
    if isinstance(node, ast.Constant):
        if node.value is None or node.value is True:
            return CONTINUE
        if node.value is False:
            return Verdict(Decision.ABORT)
        if isinstance(node.value, str):
            return Verdict(Decision(node.value))
        raise _Untranslatable("non-verdict constant return")
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        method = node.func.attr
        reason = None
        if node.args:
            if len(node.args) != 1 or not (
                isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)
            ):
                raise _Untranslatable("verdict reason is not a string literal")
            reason = node.args[0].value
        mapping = {
            "ok": Decision.CONTINUE,
            "continue_": Decision.CONTINUE,
            "abort": Decision.ABORT,
            "quarantine": Decision.QUARANTINE,
        }
        if method not in mapping:
            raise _Untranslatable(f"unknown ctx verdict method {method!r}")
        return Verdict(mapping[method], reason)
    raise _Untranslatable("unsupported return expression")


def _quantified(test: ast.expr, row_var_out: list[str]) -> tuple[str, ast.expr] | None:
    """If ``test`` is ``any(P for r in rows)`` / ``all(...)``, return (quantifier, P); else None."""
    if not (isinstance(test, ast.Call) and isinstance(test.func, ast.Name)):
        return None
    if test.func.id not in ("any", "all") or len(test.args) != 1:
        return None
    gen = test.args[0]
    if not (isinstance(gen, ast.GeneratorExp) and len(gen.generators) == 1):
        return None
    comp = gen.generators[0]
    if comp.ifs or comp.is_async:
        return None
    if not (isinstance(comp.target, ast.Name) and isinstance(comp.iter, ast.Name)):
        return None
    if comp.iter.id != "rows":
        return None
    row_var_out.append(comp.target.id)
    return test.func.id, gen.elt


def translate(source: str) -> SqlPreflight | None:
    """Translate a preflight check to a :class:`SqlPreflight`, or ``None`` if not SQL-expressible.

    Recognizes exactly the single-assertion shape ``if any/all(P for r in rows): return <verdict>``
    followed by a trailing ``return <verdict>``. Any other structure — cross-row state, helper
    calls, multiple branches, an untranslatable predicate — returns ``None`` so the caller uses the
    Python+Arrow streaming path. Never guesses: an ambiguous construct is a None, not a wrong SQL.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    func = next(
        (
            s
            for s in tree.body
            if isinstance(s, ast.FunctionDef) and s.name == REQUIRED_FUNC
        ),
        None,
    )
    # A translatable check is self-contained: exactly the guarded-assertion + trailing return, and
    # no sibling defs/consts (those signal a helper-driven check → streaming path).
    if func is None or len(tree.body) != 1 or len(func.body) != 2:
        return None
    guard, tail = func.body
    if not (isinstance(guard, ast.If) and isinstance(tail, ast.Return)):
        return None
    if guard.orelse or len(guard.body) != 1 or not isinstance(guard.body[0], ast.Return):
        return None
    row_var: list[str] = []
    quant = _quantified(guard.test, row_var)
    if quant is None:
        return None
    quantifier, predicate = quant
    try:
        predicate_sql = _PredicateTranslator(row_var[0]).visit(predicate)
        violation = _verdict_from_return(guard.body[0].value)
        passing = _verdict_from_return(tail.value)
    except (_Untranslatable, ValueError):
        return None
    return SqlPreflight(quantifier, predicate_sql, violation, passing)
