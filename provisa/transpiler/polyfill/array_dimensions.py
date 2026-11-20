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
any other call keeps its own form. Trino holds it as an array of arrays too: the same calls read
its flattened array there (of two dimensions; a deeper one keeps its own form)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Rule, Side, written_in
from provisa.transpiler.polyfill.matching import Bindings, graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement

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


#: Trino's forms: an array of arrays flattened (flatten reads one level of nesting, as a
#: two-dimensional literal is). array_to_string's NULL text of a NULL element is none: the element
#: is left out, as of two arguments.
_TRINO_FORMS: dict[tuple[str, int], tuple[str, str]] = {
    ("cardinality", 1): ("cardinality($a)", "cardinality(flatten($a))"),
    ("array_to_string", 2): ("array_to_string($a, $s)", "array_join(flatten($a), $s)"),
    ("array_to_string", 3): (
        "array_to_string($a, $s, $n)",
        "CASE WHEN $n IS NULL THEN array_join(flatten($a), $s) "
        "ELSE array_join(flatten($a), $s, $n) END",
    ),
}


_ONE_DIMENSION_NULL_TEXT = (
    "CASE WHEN $n IS NULL THEN array_join($a, $s) ELSE array_join($a, $s, $n) END"
)


def _depth(value: exp.Expr) -> int:
    """The dimensions of a cast to an array of arrays (2 or more), as its type says."""
    node = value.unnest()
    depth, to = 0, node.to if isinstance(node, exp.Cast) else None
    while to is not None and to.is_type(exp.DataType.Type.ARRAY):
        depth += 1
        to = to.expressions[0] if to.expressions else None
    return depth


#: PostgreSQL's refusal of a call of an array of more than one dimension: the call's name, the
#: place of the array among its arguments (as PostgreSQL writes the call), the SQLSTATE and the
#: message.
_REFUSED_OF_DIMENSIONS = {
    "array_append": (0, "22000", "argument must be empty or one-dimensional array"),
    "array_prepend": (1, "22000", "argument must be empty or one-dimensional array"),
    "array_position": (
        0,
        "0A000",
        "searching for elements in multidimensional arrays is not supported",
    ),
    "array_positions": (
        0,
        "0A000",
        "searching for elements in multidimensional arrays is not supported",
    ),
    "width_bucket": (1, "2202E", "thresholds must be one-dimensional array"),
}


def _elements(value: exp.Expr) -> list[exp.Expr] | None:
    """The elements of an array a statement writes (a cast of an array constructor); None for any
    other value."""
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return list(node.expressions) if isinstance(node, exp.Array) else None


def _answered(node: exp.Expr) -> bool:
    """Whether ``node`` is a column of its select list, the answer itself: no call reads it, which
    would take the array of arrays it writes for an array of one dimension."""
    parent = node.parent
    while isinstance(parent, (exp.Alias, exp.Paren)):
        parent = parent.parent
    return isinstance(parent, exp.Select) and node.arg_key != "where"


def _trino_of_dimensions(
    name: str, args: list[exp.Expr], node: exp.Expr | None = None
) -> exp.Expr | None:
    """On Trino: PostgreSQL's refusal of a call it refuses of an array of two dimensions, and
    array_eq, array_ne and array_cat of a two-dimensional and a one-dimensional array the statement
    writes, as array.c answers them; None for any other call."""
    refusal = _REFUSED_OF_DIMENSIONS.get(name)
    # sqlglot's typed node may hold the array first whatever its place in the call (ArrayPrepend)
    if refusal is not None and len(args) == 2 and any(_nested(a) for a in args):  # noqa: PLR2004
        raise Refused(Refusal(refusal[1], refusal[2]), ruling=True)
    if name not in ("array_eq", "array_ne", "array_cat") or len(args) != 2:  # noqa: PLR2004
        return None
    deep, flat = (args[0], args[1]) if _nested(args[0]) else (args[1], args[0])
    if not _nested(deep) or _nested(flat):
        return None
    if name in ("array_eq", "array_ne"):  # arrays of other dimensions are not equal
        return exp.Boolean(this=name == "array_ne")
    if node is not None and not _answered(node):
        raise Refused(
            Refusal(
                "0A000",
                "array_cat() of an array of two dimensions read by another call has "
                "no form on trino",
            ),
            ruling=True,
        )
    rows, row = _elements(deep), _elements(flat)
    if rows is None or row is None or not rows:
        return None
    width = _elements(rows[0])
    if width is None or (row and len(row) != len(width)):
        raise Refused(Refusal("2202E", "cannot concatenate incompatible arrays"), ruling=True)
    if not row:
        return deep.copy()
    pair = [deep.copy(), exp.Array(expressions=[flat.copy()])]
    if flat is args[0]:
        pair.reverse()
    return exp.Anonymous(this="concat", expressions=pair)


def of_dimensions(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """cardinality, unnest or array_to_string of an array of more than one dimension: of every
    element. Read before any row of the call's name; any other call is left to its rows."""
    call = trino.called(node)
    if call is None:
        return None
    names, args = call
    on_trino = dialect in trino.TRINO
    if on_trino and args and _nested(args[0]) and _depth(args[0]) != 2:  # noqa: PLR2004
        return None  # flatten reads one level: deeper keeps what it had
    if on_trino:
        for name in sorted(names):
            answered = _trino_of_dimensions(name, args, node)
            if answered is not None:
                return answered
    for name in sorted(names):
        if on_trino and name == "unnest" and len(args) == 1 and _nested(args[0]):
            # sqlglot's Explode, which Trino's writer lowers to UNNEST in a select list
            return exp.Explode(this=exp.Anonymous(this="flatten", expressions=[args[0]]))
        if on_trino and name == "array_to_string" and len(args) == 3 and not _nested(args[0]):  # noqa: PLR2004
            # array_to_string's NULL text of a NULL element is none: the element is left out,
            # where Trino's array_join of a NULL text answers NULL.
            holes = dict(zip(("a", "s", "n"), args))
            return trino.written(_ONE_DIMENSION_NULL_TEXT, "array_to_string($a, $s, $n)", holes)
        written = (_TRINO_FORMS if on_trino else _FORMS).get((name, len(args)))
        if written is not None and _nested(args[0]):
            pattern, form = written
            holes: Bindings = dict(zip(("a", "s", "n"), args))
            return graft(replacement(form, written_in(dialect), pattern), holes)
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
                    ),
                    **trino.on_trino(
                        Rewrite(
                            of_dimensions,
                            mentions=r"\b(?:cardinality|unnest|array_to_string|array_append|array_prepend|"
                            r"array_positions?|width_bucket|array_eq|array_ne|array_cat)\b",
                            first=True,
                        )
                    ),
                },
            ),
        )
    ),
)

FEATURES: tuple[Feature, ...] = (FEATURE,)
