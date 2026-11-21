# Copyright (c) 2026 Kenneth Stott
# Canary: 6f3318ca-96d3-4ac4-bd2d-a0d13a39c2e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrays of more than one dimension on ClickHouse (REQ-2236; the function support measure's edge
values, REQ-2171). PostgreSQL's array is one value of any number of dimensions, read element by
element in storage order; ClickHouse holds '{{1,2},{3,4}}'::integer[] as an array of arrays
(provisa.transpiler.pg_literals.array_input). cardinality counts every element (4), unnest answers
each element (1, 2, 3, 4) and array_to_string joins every element: each reads the flattened array.

Only an array the statement writes with nested braces has more than one dimension on ClickHouse:
any other call keeps its own form."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.matching import Bindings, graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement

FAMILY = "arrays"


def _nested(value: exp.Expr) -> bool:
    """Whether ``value`` is a cast to an array of arrays, or an array constructor of arrays: an
    array of more than one dimension."""
    node = value.unnest()
    if isinstance(node, exp.Array):
        return any(isinstance(item.unnest(), exp.Array) for item in node.expressions)
    if not isinstance(node, exp.Cast) or not node.to.is_type(exp.DataType.Type.ARRAY):
        return False
    return bool(node.to.expressions) and node.to.expressions[0].is_type(exp.DataType.Type.ARRAY)


#: Each call's form of an array of more than one dimension, by its name and argument count.
_FORMS: dict[tuple[str, int], tuple[str, str]] = {
    ("cardinality", 1): ("cardinality($a)", "length(arrayFlatten($a))"),
    ("unnest", 1): ("unnest($a)", "arrayJoin(arrayFlatten($a))"),
    ("array_to_string", 2): (
        "array_to_string($a, $s)",
        "arrayStringConcat(arrayFlatten($a), $s)",
    ),
    ("array_to_string", 3): (
        "array_to_string($a, $s, $n)",
        "arrayStringConcat(arrayMap(_e -> ifNull(toString(_e), $n), arrayFlatten($a)), $s)",
    ),
}


def of_dimensions(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """cardinality, unnest or array_to_string of an array of more than one dimension: of every
    element. Read before any row of the call's name; any other call is left to its rows."""
    call = trino.called(node)
    if call is None:
        return None
    names, args = call
    for name in sorted(names):
        written = _FORMS.get((name, len(args)))
        if written is not None and _nested(args[0]):
            pattern, form = written
            holes: Bindings = dict(zip(("a", "s", "n"), args))
            return graft(replacement(form, dialect, pattern), holes)
    return None


FEATURE = Feature(
    "an array of dimensions",
    FAMILY,
    "cardinality, unnest and array_to_string of an array of more than one dimension: of every "
    "element, in storage order.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(
                        of_dimensions,
                        mentions=r"\b(?:cardinality|unnest|array_to_string)\b",
                        first=True,
                    )
                },
            ),
        )
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
#: Order: an array of dimensions is read before any row of its call's name.
ORDER = 10
FEATURES: tuple[Feature, ...] = (FEATURE,)
