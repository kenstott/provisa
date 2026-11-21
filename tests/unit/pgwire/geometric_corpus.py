# Copyright (c) 2026 Kenneth Stott
# Canary: 1c9e5f02-7a3d-4b66-a2e8-d40f7b13c586
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements PostgreSQL 17 answers for the geometric forms (REQ-2171): each overload pg_proc
lists, over values chosen to reach its branches, and a NULL of each argument."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from itertools import product

from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS
from provisa.transpiler import pg_forms_geometric as geometric

VALUES: dict[str, tuple[str, ...]] = {
    "point": ("(1,2)", "(0,0)", "(-3.5,4)", "(0.1,0.2)", "(1e+20,1e-05)", "(1,2.0000001)"),
    "lseg": ("[(0,0),(3,4)]", "[(1,1),(1,5)]", "[(-1,0),(2,0)]", "[(0.1,0.7),(0.3,0.2)]"),
    "box": ("(3,4),(0,0)", "(1,1),(-1,-1)", "(5,2),(2,1)", "(0,0),(3,4)", "(1.5,1.5),(1.5,1.5)"),
    "path": (
        "((0,0),(1,0),(1,1))",
        "[(0,0),(2,2)]",
        "((1,1),(4,1),(4,5))",
        "[(0,1),(0.5,3),(7,1)]",
    ),
    "polygon": ("((0,0),(1,0),(1,1))", "((0,0),(4,0),(4,4),(0,4))", "((1,1),(2,3),(3,1))"),
    "circle": ("<(0,0),1>", "<(1,2),3>", "<(-1,-1),0.5>", "<(10,10),0>"),
    "double precision": ("1.5", "0", "-2", "0.1"),
    "integer": ("2", "3", "7"),
    "line": ("{1,-1,0}", "{0,-1,2}", "{-1,0,1}", "{2,3,-6}", "{1,-1,-5}", "{0.5,0.25,1}"),
}
#: What the functions of two shapes meet, beside VALUES: lines the same, parallel and crossing,
#: segments that touch, overlap, run along each other or are one point, boxes and shapes apart.
NEAR: dict[str, tuple[str, ...]] = {
    "point": ("(1,2)", "(0,0)", "(2,2)", "(1,0)", "(3,4)"),
    "line": ("{1,-1,0}", "{2,-2,0}", "{1,-1,-5}", "{0,-1,2}", "{-1,0,1}", "{3,1,0}"),
    "lseg": (
        "[(0,0),(3,4)]",
        "[(3,4),(0,0)]",
        "[(0,0),(1,1)]",
        "[(2,2),(3,3)]",
        "[(5,5),(5,5)]",
        "[(0,2),(4,2)]",
        "[(1,1),(1,5)]",
    ),
    "box": ("(3,4),(0,0)", "(1,1),(-1,-1)", "(10,10),(8,8)", "(1.5,1.5),(1.5,1.5)"),
    "path": (
        "((0,0),(1,0),(1,1))",
        "[(0,0),(2,2)]",
        "((1,1),(4,1),(4,5))",
        "[(0,1),(0.5,3),(7,1)]",
        "[(9,9)]",
        "((5,5))",
    ),
    "polygon": (
        "((0,0),(1,0),(1,1))",
        "((1,0),(1,1),(0,0))",
        "((1,1),(0,0),(1,0))",
        "((0,0),(4,0),(4,4),(0,4))",
        "((1,1),(2,3),(3,1))",
        "((10,10),(11,10),(11,11))",
        "((3,3),(5,3),(5,5),(3,5))",
    ),
    "circle": ("<(0,0),1>", "<(1,2),3>", "<(10,10),0>"),
}
#: The functions of NEAR. A ClickHouse plan of the longest forms takes half a minute, so each
#: overload takes twelve of its pairs, walked diagonally so that a shape meets itself.
NEARBY = frozenset(
    {
        *(r.name for r in geometric.ROWS if r.name.startswith(("close_", "inter_", "on_"))),
        *(r.name for r in geometric.ROWS if r.name.startswith("dist_") and r.params.count("point")),
        *("dist_sl", "dist_ls", "dist_sb", "dist_bs", "dist_cpoly", "dist_polyc"),
        *("line_distance", "line_eq", "line_interpt", "line_intersect"),
        *("lseg_distance", "lseg_interpt", "lseg_intersect"),
        *("path_contain_pt", "path_distance", "path_inter"),
        *("poly_contain_pt", "poly_distance", "poly_overlap", "poly_same"),
    }
)
_NEARBY_PER_OVERLOAD = 12
_TYPES = {"point", "lseg", "box", "path", "polygon", "circle", "line"}
#: Every pair of these, for containment: inside, outside, crossing, a notch, shared edges, vertices on
#: an edge, and a polygon holding a vertex on the other's edge.
CONTAINED = (
    "((0,0),(4,0),(4,4),(0,4))",
    "((1,1),(2,3),(3,1))",
    "((0,0),(2,0),(2,2),(0,2))",
    "((2,0),(4,2),(2,4),(0,2))",
    "((0,0),(4,0),(4,4),(2,2),(0,4))",
    "((1,2.5),(3,2.5),(3,3.5),(1,3.5))",
    "((3,3),(5,3),(5,5),(3,5))",
    "((0,4),(6,4),(3,2),(6,0),(0,0))",
    "((2,1),(2,3),(3,3),(3,1))",
)
#: The branches a sample of the values can miss: what PostgreSQL refuses, and a line's three shapes.
BRANCHES = (
    "SELECT point_div('(1,2)'::point, '(0,0)'::point) AS v",
    "SELECT box_div('(3,4),(0,0)'::box, '(0,0)'::point) AS v",
    "SELECT circle_div_pt('<(1,2),3>'::circle, '(0,0)'::point) AS v",
    "SELECT path_div_pt('[(0,1),(0.5,3),(7,1)]'::path, '(0,0)'::point) AS v",
    "SELECT line('(1,2)'::point, '(1,2)'::point) AS v",
    "SELECT line('(1,1)'::point, '(1,5)'::point) AS v",
    "SELECT line('(1,1)'::point, '(4,1)'::point) AS v",
    "SELECT line('(0,0)'::point, '(1,1)'::point) AS v",
    "SELECT polygon(1::integer, '<(1,2),3>'::circle) AS v",
    "SELECT polygon(1::integer, '<(10,10),0>'::circle) AS v",
    "SELECT polygon('<(10,10),0>'::circle) AS v",
    "SELECT polygon('[(0,0),(2,2)]'::path) AS v",
    "SELECT path_add('[(0,0),(2,2)]'::path, '[(0,1),(0.5,3),(7,1)]'::path) AS v",
    "SELECT path_add('((0,0),(1,0),(1,1))'::path, '[(0,0),(2,2)]'::path) AS v",
    "SELECT circle('(1,2)'::point, -2::double precision) AS v",
    "SELECT line_eq('[(0,0),(1,1)]'::line, '{1,-1,0}'::line) AS v",
    "SELECT dist_ppath('(0,0)'::point, '[(9,9)]'::path) AS v",
    "SELECT path_distance('[(9,9)]'::path, '[(0,0),(1,1)]'::path) AS v",
    "SELECT poly_distance('((0,0),(4,0),(4,4),(0,4))'::polygon, '((1,1),(2,1),(2,2))'::polygon) AS v",
    "SELECT poly_overlap('((0,0),(4,0),(4,4),(0,4))'::polygon, '((1,1),(2,1),(2,2))'::polygon) AS v",
    "SELECT poly_same('((0,0),(1,0),(1,1))'::polygon, '((0,0),(1,0))'::polygon) AS v",
    "SELECT lseg_interpt('[(0,0),(2,2)]'::lseg, '[(0,2),(2,0)]'::lseg) AS v",
    "SELECT close_lseg('[(0,0),(2,2)]'::lseg, '[(3,0),(5,1)]'::lseg) AS v",
    "SELECT close_sb('[(-1,2),(5,2)]'::lseg, '(3,4),(0,0)'::box) AS v",
    "SELECT inter_lb('{1,-1,0}'::line, '(10,1),(8,0)'::box) AS v",
)
_FLOAT = "double precision"
#: width_bucket takes no shape: float8's and numeric's, through each path of each, what a bound or a
#: count may not be, and its list, in order and not.
BUCKETS = (
    *(
        f"SELECT width_bucket({x}::{kind}, {low}::{kind}, {high}::{kind}, {count}) AS v"
        for kind in (_FLOAT, "numeric")
        for x, low, high, count in (
            ("5.35", "0.024", "10.06", 5),
            ("0.3", "0", "0.9", 3),
            ("0.7", "0", "2.1", 3),
            ("-1", "0", "10", 5),
            ("10", "0", "10", 5),
            ("5", "10", "0", 5),
            ("0", "10", "0", 5),
            ("11", "10", "0", 5),
            ("7.5", "10", "0", 4),
            ("-5.2", "-25", "25", 10),
            ("1", "0", "1", 2147483647),
            ("0.99999999999", "0", "1", 1),
            ("1", "1", "1", 4),
            ("1", "0", "1", 0),
            ("1", "0", "1", -1),
        )
    ),
    "SELECT width_bucket(5.35, 0.024, 10.06, 5) AS v",
    "SELECT width_bucket(0.3, 0, 0.9, 3) AS v",
    "SELECT width_bucket(5, 0, 10, 5) AS v",
    "SELECT width_bucket(-1, 0, 10, 5) AS v",
    f"SELECT width_bucket(NULL::{_FLOAT}, 0::{_FLOAT}, 1::{_FLOAT}, 4) AS v",
    "SELECT width_bucket(1::numeric, 0::numeric, NULL::numeric, 4) AS v",
    "SELECT width_bucket(5, ARRAY[1, 3, 4, 6]) AS v",
    "SELECT width_bucket(5, ARRAY[9, 1, 7, 2, 3]) AS v",
    "SELECT width_bucket(5, ARRAY[3]) AS v",
    "SELECT width_bucket(0, ARRAY[3]) AS v",
    "SELECT width_bucket(5, ARRAY[]::integer[]) AS v",
    "SELECT width_bucket(5, ARRAY[3, 4, NULL]) AS v",
    "SELECT width_bucket(5, ARRAY[ARRAY[1, 2], ARRAY[3, 4]]) AS v",
    "SELECT width_bucket(NULL::integer, ARRAY[1, 2]) AS v",
    f"SELECT width_bucket(5.5::{_FLOAT}, ARRAY[1.5, 3.5, 9.5]::{_FLOAT}[]) AS v",
    "SELECT width_bucket('b'::text, ARRAY['a', 'c', 'e']) AS v",
)


def _overloads(name: str) -> list[tuple[str, ...]]:
    """The argument types of each overload of ``name`` that takes a shape."""
    overloads = FUNCTION_OVERLOADS.get(name, ())
    return [args for args, _ in overloads if name == "point" or _TYPES & set(args)]


def _literal(kind: str, text: str) -> str:
    if kind in ("double precision", "integer"):
        return f"{text}::{kind}"
    return f"'{text}'::{kind}"


def _diagonal(calls: list, types: tuple[str, ...], most: int, nearby: bool) -> list:
    """Evenly over ``calls``: a product's stride of one more than its width walks its diagonal."""
    if not nearby or len(types) < 2:
        return calls[:: max(1, len(calls) // most)]
    return calls[:: max(1, (len(calls) // most) | 1)]


def statements() -> list[str]:
    """Each statement, in a fixed order: values two at a time at most eight per overload."""
    names = sorted(
        {r.name for r in geometric.ROWS}
        | {n for n, _ in geometric.ALIASES}
        | {n for n, _, _ in geometric.OVERLOADS}
    )
    names.remove("width_bucket")
    seen: list[str] = []
    for name in names:
        for types in _overloads(name):
            if any(t not in VALUES for t in types):
                continue
            pairs = name in ("poly_contain", "poly_contained")
            nearby = name in NEARBY and all(t in NEAR for t in types)
            kinds = NEAR if nearby else VALUES
            calls = list(
                product(CONTAINED, CONTAINED) if pairs else product(*map(kinds.get, types))
            )
            per_overload = _NEARBY_PER_OVERLOAD if nearby else 8
            for values in calls if pairs else _diagonal(calls, types, per_overload, nearby):
                args = ", ".join(_literal(t, v) for t, v in zip(types, values))
                seen.append(f"SELECT {name}({args}) AS v")
            nulls = ", ".join(f"NULL::{t}" for t in types)
            seen.append(f"SELECT {name}({nulls}) AS v")
    return list(dict.fromkeys((*seen, *BRANCHES, *BUCKETS)))
