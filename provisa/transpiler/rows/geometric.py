# Copyright (c) 2026 Kenneth Stott
# Canary: 9cac89cf-0ef0-4189-b44c-278e137372ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of geometric shapes (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms, pg_forms_geometric
from provisa.transpiler.rows.base import _TRINO, Macro, _macro, strict

#: A shape's functions. Trino's form is its geometry where a geometry function answers
#: (REQ-2171: Trino uses its geometry, not inline arithmetic).
_NOT_OPEN = "NOT starts_with(ltrim(CAST(value AS VARCHAR)), '(')"
_SHAPES: tuple[Macro, ...] = (
    _macro("area", "value", pg_forms.AREA, own={_TRINO: pg_forms.TRINO_AREA}),
    _macro("box_area", "value", pg_forms.BOX_AREA, own={_TRINO: pg_forms.TRINO_BOX_AREA}),
    _macro("path_area", "value", pg_forms.PATH_AREA, own={_TRINO: pg_forms.TRINO_PATH_AREA}),
    _macro("circle_area", "value", pg_forms.CIRCLE_AREA, {_TRINO: pg_forms.CIRCLE_AREA}),
    _macro(
        "center",
        "value",
        pg_forms.CENTER,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_CENTER)},
    ),
    _macro(
        "box_center",
        "value",
        pg_forms.BOX_CENTER,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_BOX_CENTER)},
    ),
    _macro(
        "circle_center",
        "value",
        pg_forms.CIRCLE_CENTER,
        {_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_CIRCLE_CENTER)},
    ),
    _macro(
        "diagonal",
        "value",
        pg_forms.BOX_DIAGONAL,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_BOX_DIAGONAL)},
    ),
    _macro("lseg_length", "value", pg_forms.LSEG_LENGTH, own={_TRINO: pg_forms.TRINO_LSEG_LENGTH}),
    _macro("path_length", "value", pg_forms.PATH_LENGTH, own={_TRINO: pg_forms.TRINO_PATH_LENGTH}),
    _macro(
        "height",
        "value",
        f"abs({pg_forms.number('value', 4)} - {pg_forms.number('value', 2)})",
        own={_TRINO: pg_forms.TRINO_HEIGHT},
    ),
    _macro(
        "width",
        "value",
        f"abs({pg_forms.number('value', 3)} - {pg_forms.number('value', 1)})",
        own={_TRINO: pg_forms.TRINO_WIDTH},
    ),
    _macro("radius", "value", pg_forms.number("value", 3), {_TRINO: pg_forms.number("value", 3)}),
    _macro("isclosed", "value", pg_forms.is_closed("value"), {_TRINO: pg_forms.is_closed("value")}),
    _macro("isopen", "value", _NOT_OPEN, {_TRINO: _NOT_OPEN}),
    *(
        _macro(name, params, form, {_TRINO: form})
        for of in (None, "lseg", "line")
        for name, params, form in (
            (f"{of}_horizontal" if of else "ishorizontal", "value", pg_forms.horizontal(of)),
            (f"{of}_vertical" if of else "isvertical", "value", pg_forms.vertical(of)),
            (f"{of}_parallel" if of else "isparallel", "a, b", pg_forms.parallel(of)),
            (f"{of}_perp" if of else "isperp", "a, b", pg_forms.perpendicular(of)),
        )
    ),
    _macro(
        "ishorizontal",
        "a, b",
        pg_forms.points_differ_by_nothing(2),
        {_TRINO: pg_forms.points_differ_by_nothing(2)},
    ),
    _macro(
        "isvertical",
        "a, b",
        pg_forms.points_differ_by_nothing(1),
        {_TRINO: pg_forms.points_differ_by_nothing(1)},
    ),
    _macro(
        "pt_contained_circle",
        "p, c",
        pg_forms.POINT_IN_CIRCLE,
        own={_TRINO: pg_forms.TRINO_POINT_IN_CIRCLE},
    ),
    _macro(
        "pt_contained_poly",
        "p, poly",
        pg_forms.POINT_IN_POLYGON,
        own={_TRINO: pg_forms.TRINO_POINT_IN_POLYGON},
    ),
)


def _geometric() -> tuple[Macro, ...]:
    """Each geometric row, and its plain name, on each engine of pg_forms_geometric."""
    rows: dict[tuple[str, str], dict[frozenset[str], str]] = {}
    for engine in pg_forms_geometric.ENGINES:
        for key, form in pg_forms_geometric.forms(engine).items():
            rows.setdefault(key, {})[frozenset({engine.name})] = form
    plain = {
        alias: rows[next(key for key in rows if key[0] == row)]
        for alias, row in pg_forms_geometric.ALIASES.items()
    }
    bare = (Macro(n, (p,)) for n, p in pg_forms_geometric.BARE)
    return (
        *bare,
        *(Macro(n, tuple(p.split(", ")), f) for (n, p), f in {**rows, **plain}.items()),
    )


ROWS: tuple[Macro, ...] = (
    *_SHAPES,
    *_geometric(),
)
