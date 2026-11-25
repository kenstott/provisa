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
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side, written_in
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


# DuckDB (REQ-2236): it holds an array of more than one dimension as a list of lists, as Trino
# does. PostgreSQL reads each of these functions' arrays element by element in storage order, its
# dimensions dropped: each nested array the statement writes is given them as that flattened array.
_READ_FLAT = frozenset(
    {
        "cardinality",
        "unnest",
        "explode",
        "hash_array",
        "hash_array_extended",
        "arraycontains",
        "arraycontained",
        "arrayoverlap",
        "array_contains_all",
        "array_has_all",
        "array_contained_by",
        "array_overlaps",
        "array_to_tsvector",
        "setweight",
        "ts_delete",
        "jsonb_exists_any",
        "jsonb_exists_all",
    }
)

#: array.c's array_cmp, of an array of more than one dimension, by the comparison each call reads
#: of its result.
_ORDERED: dict[str, str] = {
    "array_eq": "=",
    "array_ne": "<>",
    "array_lt": "<",
    "array_le": "<=",
    "array_gt": ">",
    "array_ge": ">=",
}

_INTEGERS = (
    exp.DataType.Type.SMALLINT,
    exp.DataType.Type.INT,
    exp.DataType.Type.BIGINT,
)


def _shape(value: exp.Expr) -> tuple[int, ...] | None:
    """The lengths of the dimensions of an array the statement writes, outermost first (() for
    an empty one); None for any other value and for one whose sub-arrays differ in shape."""
    elements = _elements(value)
    if elements is None:
        return None
    if not elements:
        return ()
    inner = [_elements(e) is not None for e in elements]
    if not any(inner):
        return (len(elements),)
    if not all(inner):
        return None
    shapes = {_shape(e) for e in elements}
    if len(shapes) != 1:
        return None
    sub = shapes.pop()
    return None if not sub else (len(elements), *sub)


def _leaves(value: exp.Expr) -> list[exp.Expr]:
    """The elements of an array the statement writes (one of a shape), in storage order."""
    leaves: list[exp.Expr] = []
    for element in _elements(value) or []:
        leaves.extend(_leaves(element) if _elements(element) is not None else [element])
    return leaves


def _element_type(value: exp.Expr) -> exp.DataType | None:
    """The element type of an array cast, None for a value no cast types."""
    node = value.unnest()
    if not isinstance(node, exp.Cast):
        return None
    to = node.to
    while to.is_type(exp.DataType.Type.ARRAY) and to.expressions:
        to = to.expressions[0]
    return to


def _flat(value: exp.Expr) -> exp.Expr:
    """``value``, an array of more than one dimension, as the array of one dimension of its
    elements in storage order: of the elements a statement writes, else DuckDB's flatten (of one
    level a call) of the value."""
    element = _element_type(value)
    if _shape(value) is not None:
        flat: exp.Expr = exp.Array(expressions=[leaf.copy() for leaf in _leaves(value)])
    else:
        flat = value.copy()
        for _ in range(_depth(value) - 1):
            flat = exp.Anonymous(this="flatten", expressions=[flat])
    if element is None:
        return flat
    to = exp.DataType(this=exp.DataType.Type.ARRAY, expressions=[element.copy()], nested=True)
    return exp.Cast(this=flat, to=to)


def _deep(value: exp.Expr) -> bool:
    """Whether ``value`` is an array of more than one dimension the statement writes or casts."""
    shape = _shape(value)
    return len(shape) > 1 if shape is not None else _nested(value)


def _ties(left: tuple[int, ...], right: tuple[int, ...]) -> int:
    """array_cmp of two arrays whose elements are the same: more dimensions is larger, then the
    longer of the first dimension that differs (each lower bound is 1)."""
    if len(left) != len(right):
        return 1 if len(left) > len(right) else -1
    for a, b in zip(left, right):
        if a != b:
            return 1 if a > b else -1
    return 0


def _integer_leaves(value: exp.Expr) -> list[int] | None:
    """The elements of an integer array the statement writes, each a constant; None otherwise."""
    element = _element_type(value)
    if element is None or not element.is_type(*_INTEGERS):
        return None
    numbers: list[int] = []
    for leaf in _leaves(value):
        node = leaf.unnest()
        while isinstance(node, exp.Cast):
            node = node.this.unnest()
        if not isinstance(node, exp.Literal):
            return None
        try:
            numbers.append(int(node.this))
        except ValueError:
            return None
    return numbers


def _static_cmp(left: exp.Expr, right: exp.Expr) -> int | None:
    """array_cmp of two integer arrays the statement writes of constants; None otherwise."""
    a, b = _integer_leaves(left), _integer_leaves(right)
    shapes = _shape(left), _shape(right)
    if a is None or b is None or shapes[0] is None or shapes[1] is None:
        return None
    for x, y in zip(a, b):
        if x != y:
            return 1 if x > y else -1
    if len(a) != len(b):
        return 1 if len(a) > len(b) else -1
    return _ties(shapes[0], shapes[1])


#: array_larger and array_smaller of arrays of different dimensions, where the statement does not
#: decide which is larger.
_UNDECIDED = Refusal(
    "0A000",
    "array_larger() and array_smaller() of arrays of different dimensions have no form on duckdb "
    "unless both are integer arrays of constants",
)


def _ordered(name: str, left: exp.Expr, right: exp.Expr) -> exp.Expr | None:
    """array_eq, array_ne, array_lt, array_le, array_gt, array_ge, btarraycmp, array_larger or
    array_smaller of two arrays the statement writes, one of more than one dimension: array_cmp
    reads their elements in storage order (btarraycmp of them flattened, which reads the longer as
    larger), then the dimensions (_ties)."""
    shapes = _shape(left), _shape(right)
    if shapes[0] is None or shapes[1] is None:
        return None
    if name in ("array_larger", "array_smaller"):
        if len(shapes[0]) != len(shapes[1]):
            # Each answers one of its arguments, of a list type of its own nesting on DuckDB: the
            # choice is the statement's, of constants, or none.
            decided = _static_cmp(left, right)
            if decided is None:
                raise Refused(_UNDECIDED, ruling=True)
            larger = decided > 0 if name == "array_larger" else decided < 0
            return (left if larger else right).copy()
    flat_cmp: exp.Expr = exp.Anonymous(this="btarraycmp", expressions=[_flat(left), _flat(right)])
    tie = _ties(shapes[0], shapes[1])
    if tie:  # flat_cmp is -1, 0 or 1: twice it, plus the tie, has its sign unless it is 0
        flat_cmp = exp.Add(
            this=exp.Mul(this=exp.Literal.number(2), expression=exp.Paren(this=flat_cmp)),
            expression=exp.Literal.number(tie),
        )
    zero = exp.Literal.number(0)
    if name == "btarraycmp":
        if not tie:
            return flat_cmp
        signed = exp.Anonymous(this="sign", expressions=[flat_cmp])
        return exp.Cast(this=signed, to=exp.DataType.build("INT"))
    if name in ("array_larger", "array_smaller"):
        kind = exp.GT if name == "array_larger" else exp.LT
        return exp.Case(
            ifs=[
                exp.If(this=kind(this=exp.Paren(this=flat_cmp), expression=zero), true=left.copy())
            ],
            default=right.copy(),
        )
    operator = {"=": exp.EQ, "<>": exp.NEQ, "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE}
    return exp.Paren(this=operator[_ORDERED[name]](this=exp.Paren(this=flat_cmp), expression=zero))


_INCOMPATIBLE = Refusal("2202E", "cannot concatenate incompatible arrays")


def _concatenated(left: exp.Expr, right: exp.Expr) -> exp.Expr | None:
    """array_cat of two arrays the statement writes, one of more than one dimension, as array.c
    reads them: an empty one answers the other; arrays of as many dimensions are joined when their
    inner dimensions agree; an array of one dimension fewer is joined as one more element of the
    other when its dimensions are the other's inner ones. Any other pair raises."""
    a, b = _shape(left), _shape(right)
    if a is None or b is None:
        return None
    if not a or not b:
        return (right if not a else left).copy()
    if len(a) == len(b):
        if a[1:] != b[1:]:
            raise Refused(_INCOMPATIBLE, ruling=True)
        return None  # DuckDB's own concatenation of two lists of one nesting
    if len(a) == len(b) + 1 and a[1:] == b:
        return exp.Anonymous(
            this="array_cat", expressions=[left.copy(), exp.Array(expressions=[right.copy()])]
        )
    if len(b) == len(a) + 1 and b[1:] == a:
        return exp.Anonymous(
            this="array_cat", expressions=[exp.Array(expressions=[left.copy()]), right.copy()]
        )
    raise Refused(_INCOMPATIBLE, ruling=True)


def _paired(args: list[exp.Expr]) -> None:
    """json_object or jsonb_object of an array of more than one dimension: PostgreSQL reads an
    array of two columns as its pairs, and raises for any other (json.c, ARRAY_SUBSCRIPT_ERROR).
    The pairs are read flattened, as the one-dimensional array of keys and values; in place."""
    if len(args) == 2:  # noqa: PLR2004
        if any(_deep(a) for a in args):
            raise Refused(Refusal("2202E", "wrong number of array subscripts"), ruling=True)
        return
    if len(args) != 1 or not _deep(args[0]):
        return
    shape = _shape(args[0])
    if shape is None:
        return  # a cast of a value: its columns are not the statement's
    if len(shape) != 2:  # noqa: PLR2004
        raise Refused(Refusal("2202E", "wrong number of array subscripts"), ruling=True)
    if shape[-1] != 2:  # noqa: PLR2004
        raise Refused(Refusal("2202E", "array must have two columns"), ruling=True)
    args[0].replace(_flat(args[0]))


def duckdb_of_dimensions(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """On DuckDB, a call of an array of more than one dimension the statement writes or casts, as
    PostgreSQL reads it: each array a function reads element by element flattened (in place), an
    ordering of two by array_cmp, array_cat as array.c joins them, and PostgreSQL's refusal of a
    call it refuses of one. The call written anew, or None where it stays as it is."""
    del dialect
    call = trino.called(node)
    if call is None:
        return None
    names, args = call
    if not any(_deep(a) for a in args):
        return None
    refused = next((_REFUSED_OF_DIMENSIONS[n] for n in names if n in _REFUSED_OF_DIMENSIONS), None)
    if refused is not None and len(args) == 2:  # noqa: PLR2004
        raise Refused(Refusal(refused[1], refused[2]), ruling=True)
    if "array_remove" in names:
        raise Refused(
            Refusal("0A000", "removing elements from multidimensional arrays is not supported"),
            ruling=True,
        )
    # hash_array's rows are named by the element type (pg_forms_hash.OVERLOADS)
    if names & _READ_FLAT or any(n.startswith("hash_array_") for n in names):
        for argument in args:
            if _deep(argument):
                argument.replace(_flat(argument))
        return None
    if names & {"json_object", "jsonb_object", "pg_json_object"}:  # pg_functions names the first
        _paired(args)
        return None
    if len(args) != 2:  # noqa: PLR2004
        return None
    for name in sorted(names):
        if name in _ORDERED or name in ("btarraycmp", "array_larger", "array_smaller"):
            return _ordered(name, args[0], args[1])
        if name in ("array_cat", "array_concat"):
            return _concatenated(args[0], args[1])
    return None


class CallRefused(Refused):
    """PostgreSQL's refusal of a call of an array of more than one dimension, with the name of the
    function called."""

    def __init__(self, called: str, refused: Refused) -> None:
        super().__init__(refused.refusal, ruling=refused.ruling)
        self.called = called


def on_duckdb(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each call of an array of more than one dimension written as DuckDB answers it
    as PostgreSQL does (duckdb_of_dimensions), innermost first; before any function's form reads
    the arrays. Raises CallRefused where PostgreSQL raises."""
    for node in reversed(list(tree.walk())):
        try:
            written = duckdb_of_dimensions(node, "duckdb")
        except Refused as refused:
            called = node.name.lower() if isinstance(node, exp.Anonymous) else type(node).key
            raise CallRefused(called, refused) from refused
        if written is None:
            continue
        parent = node.parent
        if (
            isinstance(parent, exp.Dot)
            and node.arg_key == "expression"
            and isinstance(parent.this, exp.Identifier)
            and parent.this.name.lower() == "pg_catalog"
        ):
            node = parent  # pg_catalog's call: the call written anew stands in its place
        if node is tree:
            tree = written
        else:
            node.replace(written)
    return tree


FEATURE = Feature(
    "an array of dimensions",
    FAMILY,
    "cardinality, unnest and array_to_string of an array of more than one dimension: of every "
    "element, in storage order. On DuckDB as well each function that reads an array element by "
    "element, the orderings by array_cmp (elements, then dimensions) and array_cat as array.c "
    "joins arrays; the calls PostgreSQL refuses of one raise its error.",
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
                    "duckdb": Rewrite(
                        duckdb_of_dimensions,
                        mentions=r"\b(?:cardinality|unnest|hash_array|hash_array_extended|"
                        r"arraycontains|arraycontained|arrayoverlap|array_to_tsvector|setweight|"
                        r"ts_delete|jsonb_exists_any|jsonb_exists_all|jsonb?_object|array_append|"
                        r"array_prepend|array_positions?|array_remove|width_bucket|array_cat|"
                        r"array_(?:eq|ne|lt|le|gt|ge|larger|smaller)|btarraycmp)\b|@>|<@|&&",
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
        ),
        limitations={
            "duckdb": (
                Limitation(
                    "Only an array the statement writes, or casts to an array of arrays, is read by"
                    " its dimensions: a column of DuckDB's list of lists keeps DuckDB's own answer."
                ),
                Limitation(
                    "array_larger and array_smaller of arrays of different dimensions answer only"
                    " for integer arrays of constants, as each answers a list of its own nesting:"
                    " any other pair is refused.",
                    _UNDECIDED,
                ),
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
#: Order: an array of dimensions is read before any row of its call's name.
ORDER = 10
FEATURES: tuple[Feature, ...] = (FEATURE,)
