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
}
_TYPES = {"point", "lseg", "box", "path", "polygon", "circle", "line"}


def _overloads(name: str) -> list[tuple[str, ...]]:
    """The argument types of each overload of ``name`` that takes a shape."""
    return [args for args, _ in FUNCTION_OVERLOADS.get(name, ()) if _TYPES & set(args)]


def _literal(kind: str, text: str) -> str:
    if kind in ("double precision", "integer"):
        return f"{text}::{kind}"
    return f"'{text}'::{kind}"


def statements() -> list[str]:
    """Each statement, in a fixed order: values two at a time at most eight per overload."""
    names = sorted(
        {r.name for r in geometric.ROWS}
        | {n for n, _ in geometric.ALIASES}
        | {n for n, _, _ in geometric.OVERLOADS}
    )
    seen: list[str] = []
    for name in names:
        for types in _overloads(name):
            if any(t not in VALUES for t in types):
                continue
            calls = list(product(*(VALUES[t] for t in types)))
            for values in calls[:: max(1, len(calls) // 8)]:
                args = ", ".join(_literal(t, v) for t, v in zip(types, values))
                seen.append(f"SELECT {name}({args}) AS v")
            nulls = ", ".join(f"NULL::{t}" for t in types)
            seen.append(f"SELECT {name}({nulls}) AS v")
    return list(dict.fromkeys(seen))
