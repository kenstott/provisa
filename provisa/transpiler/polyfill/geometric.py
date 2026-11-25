# Copyright (c) 2026 Kenneth Stott
# Canary: 5d3a8f20-7c41-4e9b-a6d2-1f0e8b3c9a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A shape's measures on ClickHouse, DuckDB and Trino (REQ-2236; REQ-2167, AN ANSWER WITHIN A
STATED BOUND COMES BEFORE A REFUSAL).

area, center, diagonal, height, width, radius, isclosed, isopen, ishorizontal, isvertical,
isparallel, isperp, length, lseg_length, path_length, pt_contained_circle and pt_contained_poly are
each the form pg_forms_geometric writes of its geo_ops.c row (pg_forms_geometric.MEASURES) on each of the
three: the same floating-point steps in the same order, a sum added in turn, so the answer is
PostgreSQL's, NaN and infinities included. An answer that is not finite of arguments all finite, an
overflow float8's arithmetic raises 22003 on, is refused by name with 0A000 naming the row: no edge
value reaches it, so the row states no raise it is held to. An overload of a name is told by the
type the statement says of its argument; one it does not say is the old table's (pg_forms), which
answers it on DuckDB and Trino and refuses it by name on ClickHouse. length is a shape's only of an
argument the statement says is an lseg or a path: every other length keeps its own type's row.
Every other dialect keeps what it had."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.transpiler import pg_forms_geometric
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement

FAMILY = "geometric measures"

#: Each call a statement writes of a measure -> its overloads: the types the statement must say of
#: its arguments (None: the name's only overload, which PostgreSQL resolves a value of no said type
#: to) and the row of each.
_CALLS: dict[tuple[str, int], tuple[tuple[tuple[str, ...] | None, str], ...]] = {
    ("area", 1): ((("box",), "box_area"), (("circle",), "circle_area"), (("path",), "path_area")),
    ("center", 1): ((("circle",), "circle_center"),),  # center(box) is the old table's
    ("length", 1): ((("lseg",), "lseg_length"), (("path",), "path_length")),
    ("circle_center", 1): ((None, "circle_center"),),
    ("diagonal", 1): ((None, "box_diagonal"),),
    ("height", 1): ((None, "box_height"),),
    ("width", 1): ((None, "box_width"),),
    ("radius", 1): ((None, "circle_radius"),),
    ("isclosed", 1): ((None, "path_isclosed"),),
    ("isopen", 1): ((None, "path_isopen"),),
    ("ishorizontal", 2): ((None, "point_horiz"),),
    ("isvertical", 2): ((None, "point_vert"),),
    ("ishorizontal", 1): ((("line",), "line_horizontal"), (("lseg",), "lseg_horizontal")),
    ("isvertical", 1): ((("line",), "line_vertical"), (("lseg",), "lseg_vertical")),
    ("isparallel", 2): ((("line", "line"), "line_parallel"), (("lseg", "lseg"), "lseg_parallel")),
    ("isperp", 2): ((("line", "line"), "line_perp"), (("lseg", "lseg"), "lseg_perp")),
    **{
        (name, arity): ((None, name),)
        for name, arity in (
            ("line_horizontal", 1),
            ("line_vertical", 1),
            ("line_parallel", 2),
            ("line_perp", 2),
            ("lseg_horizontal", 1),
            ("lseg_vertical", 1),
            ("lseg_parallel", 2),
            ("lseg_perp", 2),
            ("lseg_length", 1),
            ("path_length", 1),
            ("pt_contained_circle", 2),
            ("pt_contained_poly", 2),
        )
    },
}
#: The names other types share (length of a bytea, a text, a bit string): a call is read as a
#: measure's only where the statement says a shape's type of its argument, before any rule's
#: pattern, so every other call keeps its own row.
_SHARED = frozenset({("length", 1)})
#: A literal's text in a form: a parameter's name is not read inside one.
_QUOTED = re.compile(r"'(?:[^']|'')*'")


def _with_holes(form: str, params: tuple[str, ...]) -> str:
    """``form`` with each parameter named as the hole of its place in the call: $a, then $b."""
    holes = dict(zip(params, "ab"))
    named = re.compile(r"(?<![\w$])(" + "|".join(params) + r")(?!\w)")
    pieces, at = [], 0
    for quoted in _QUOTED.finditer(form):
        pieces.append(named.sub(lambda m: "$" + holes[m.group(1)], form[at : quoted.start()]))
        pieces.append(quoted.group(0))
        at = quoted.end()
    pieces.append(named.sub(lambda m: "$" + holes[m.group(1)], form[at:]))
    return "".join(pieces)


_STATED = (
    "geo_ops.c's {} in the same floating-point steps and order (pg_forms_geometric.MEASURES): "
    "exact, NaN and infinities as float8 and geo_ops.c's FP macros treat them. An answer that is "
    "not finite of arguments all finite, an overflow (PostgreSQL's 22003), is refused by name with "
    "0A000 ({} does not read this value)."
)
_OVERLOADED = (
    " An argument whose type the statement does not say is refused by name: PostgreSQL resolves "
    "no one overload of {}() for it."
)
_SHARED_STATED = (
    " A call of {}() is this row's only where the statement says the shape's type of its argument; "
    "every other keeps its own type's row."
)
_ROWS = {row.name: row for row in pg_forms_geometric.MEASURES}


#: The engines each measure is pg_forms_geometric's form on, by the grid's column.
_ENGINES = {
    "clickhouse": pg_forms_geometric.CLICKHOUSE,
    "duckdb": pg_forms_geometric.DUCKDB,
    "trino": pg_forms_geometric.TRINO,
}


def _written(row_name: str, column: str) -> str:
    row = _ROWS[row_name]
    form, params = pg_forms_geometric.measure_form(_ENGINES[column], row)
    return _with_holes(form, params)


def _held(argument: exp.Expr) -> exp.Expr:
    """A shape as ClickHouse holds it, its text: a cast the statement writes of it is its text
    (function_macros' _argument reads a shape so)."""
    value = argument.unnest()
    return exp.Paren(this=(value.this if isinstance(value, exp.Cast) else value).copy())


def _measured(name: str, arity: int, column: str) -> Callable[[exp.Expr, str], exp.Expr | None]:
    overloads = _CALLS[(name, arity)]
    pattern = f"{name}({', '.join('$' + h for h in 'ab'[:arity])})"

    def measured(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or name not in call[0] or len(call[1]) != arity:
            return None
        said = tuple(trino.said(a.unnest()) for a in call[1])
        row = next(
            (r for types, r in overloads if types is None or types == said),
            None,
        )
        if row is None:
            return None
        tree = replacement(_written(row, column), dialect, pattern)
        return graft(tree, {h: _held(a) for h, a in zip("ab", call[1])})

    measured.__doc__ = f"{name} of a shape: geo_ops.c's row of its type, in {column}'s form."
    measured.__qualname__ = measured.__name__ = f"measured_{name}_{arity}_{column}"
    return measured


def _features() -> tuple[Feature, ...]:
    features = []
    for (name, arity), overloads in _CALLS.items():
        rows = sorted({r for _, r in overloads})
        stated = " ".join(_STATED.format(r, r) for r in rows)
        if (name, arity) in _SHARED:
            stated += _SHARED_STATED.format(name)
        elif any(types is not None for types, _ in overloads):
            stated += _OVERLOADED.format(name)
        holes = ", ".join("$" + h for h in "ab"[:arity])
        if (name, arity) in _SHARED:
            mentions = (
                r"\b(" + "|".join(sorted({t for ts, _ in overloads for t in ts or ()})) + r")\b"
            )
            rule = Rule(
                None,
                {
                    column: Rewrite(_measured(name, arity, column), mentions=mentions, first=True)
                    for column in _ENGINES
                },
            )
        else:
            forms = {
                **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                **{column: Rewrite(_measured(name, arity, column)) for column in _ENGINES},
            }
            rule = Rule(f"{name}({holes})", forms)
        features.append(
            Feature(
                f"{name}({', '.join(['shape'] * arity)})" if arity else name,
                FAMILY,
                f"PostgreSQL's {', '.join(rows)}.",
                read=Side(
                    rules=(rule,),
                    limitations={column: (Limitation(stated),) for column in _ENGINES},
                ),
            )
        )
    return tuple(features)


#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 320
FEATURES: tuple[Feature, ...] = _features()
