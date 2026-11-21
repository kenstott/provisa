# Copyright (c) 2026 Kenneth Stott
# Canary: 5d3a8f20-7c41-4e9b-a6d2-1f0e8b3c9a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A shape's measures on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES
BEFORE A REFUSAL).

The old table answers area, center, diagonal, height, width, radius, isclosed, isopen,
ishorizontal, isvertical, isparallel, isperp, lseg_length, path_length, pt_contained_circle and
pt_contained_poly on DuckDB and Trino only. On ClickHouse each is the form pg_forms_geometric
writes of its geo_ops.c row (pg_forms_geometric.MEASURES): the same floating-point steps in the
same order, a sum added in turn, so the answer is PostgreSQL's. A shape holding NaN or an infinity,
and an answer that is not finite, raises 0A000 naming the row. An overload of a name is told by
the type the statement says of its argument; one it does not say keeps its refusal by name. Every
other dialect keeps what it had."""

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

FAMILY = "geometric measures on ClickHouse"

_CLICKHOUSE = "clickhouse"
#: Each call a statement writes of a measure -> its overloads: the types the statement must say of
#: its arguments (None: the name's only overload, which PostgreSQL resolves a value of no said type
#: to) and the row of each.
_CALLS: dict[tuple[str, int], tuple[tuple[tuple[str, ...] | None, str], ...]] = {
    ("area", 1): ((("box",), "box_area"), (("circle",), "circle_area"), (("path",), "path_area")),
    ("center", 1): ((("circle",), "circle_center"),),  # center(box) is the old table's
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
    "exact. A shape holding NaN or an infinity, and an answer that is not finite, raises 0A000 "
    "({} does not read this value)."
)
_OVERLOADED = (
    " An argument whose type the statement does not say is refused by name: PostgreSQL resolves "
    "no one overload of {}() for it."
)
_ROWS = {row.name: row for row in pg_forms_geometric.MEASURES}


def _written(row_name: str) -> str:
    row = _ROWS[row_name]
    form, params = pg_forms_geometric.measure_form(pg_forms_geometric.CLICKHOUSE, row)
    return _with_holes(form, params)


def _held(argument: exp.Expr) -> exp.Expr:
    """A shape as ClickHouse holds it, its text: a cast the statement writes of it is its text
    (function_macros' _argument reads a shape so)."""
    value = argument.unnest()
    return exp.Paren(this=(value.this if isinstance(value, exp.Cast) else value).copy())


def _measured(name: str, arity: int) -> Callable[[exp.Expr, str], exp.Expr | None]:
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
        tree = replacement(_written(row), dialect, pattern)
        return graft(tree, {h: _held(a) for h, a in zip("ab", call[1])})

    measured.__doc__ = f"{name} of a shape: geo_ops.c's row of its type, in ClickHouse's form."
    measured.__qualname__ = measured.__name__ = f"measured_{name}_{arity}"
    return measured


def _features() -> tuple[Feature, ...]:
    features = []
    for (name, arity), overloads in _CALLS.items():
        rows = sorted({r for _, r in overloads})
        stated = " ".join(_STATED.format(r, r) for r in rows)
        if any(types is not None for types, _ in overloads):
            stated += _OVERLOADED.format(name)
        forms = {
            **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
            _CLICKHOUSE: Rewrite(_measured(name, arity)),
        }
        holes = ", ".join("$" + h for h in "ab"[:arity])
        features.append(
            Feature(
                f"{name}({', '.join(['shape'] * arity)})" if arity else name,
                FAMILY,
                f"PostgreSQL's {', '.join(rows)}.",
                read=Side(
                    rules=(Rule(f"{name}({holes})", forms),),
                    limitations={_CLICKHOUSE: (Limitation(stated),)},
                ),
            )
        )
    return tuple(features)


FEATURES: tuple[Feature, ...] = _features()
