# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher point as a value of its own (Bolt's Point2D and Point3D): its coordinate reference
system (SRID: 7203 cartesian, 9157 cartesian 3D, 4326 WGS-84, 4979 WGS-84 3D) and coordinates.
The engine holds one as a list of doubles, its SRID first: [srid, x, y] or [srid, x, y, z]."""

# Requirements: REQ-802

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from sqlglot import exp

from provisa.gql.expr_ast import CypherExpr, Literal, MapLiteral
from provisa.gql.translator_types import CypherTranslateError

#: The kind a point has among the kinds of values the translator tracks (temporal.kind_of).
POINT = "point"
#: The engine type a point is held as.
SQL_TYPE = "DOUBLE[]"

_GEOGRAPHIC = (4326, 4979)
_CRS = {7203: "cartesian", 9157: "cartesian-3d", 4326: "wgs-84", 4979: "wgs-84-3d"}
_SRID_OF_CRS = {name: srid for srid, name in _CRS.items()}
# A property of a point -> its place in the engine's list (1-based, the SRID first).
_COORDINATES = {"x": 2, "y": 3, "z": 4, "longitude": 2, "latitude": 3, "height": 4}


@dataclass(frozen=True)
class Point:
    srid: int
    coordinates: tuple[float, ...]

    def as_map(self) -> dict:
        """The point as a JSON answer writes it: its SRID and named coordinates."""
        names = ("longitude", "latitude", "height") if self.srid in _GEOGRAPHIC else "xyz"
        return {"srid": self.srid, **dict(zip(names, self.coordinates))}


def _number(value: exp.Expression) -> exp.Expression:
    return exp.Cast(this=value, to=exp.DataType.build("DOUBLE"))


def constructed(
    args: list[CypherExpr], lower: Callable[[CypherExpr], exp.Expression]
) -> exp.Expression:
    """point({x, y[, z]}) or point({longitude, latitude[, height]}), with an optional crs or
    srid: the engine's list for it."""
    if len(args) != 1 or not isinstance(args[0], MapLiteral):
        raise CypherTranslateError("point() takes one map of coordinates")
    entries = dict(args[0].entries)
    geographic = "longitude" in entries or "latitude" in entries
    names = ("longitude", "latitude", "height") if geographic else ("x", "y", "z")
    given = [entries[n] for n in names if n in entries]
    if len(given) not in (2, 3) or any(n not in entries for n in names[:2]):
        raise CypherTranslateError(f"point() needs {names[0]} and {names[1]}")
    srid = _srid(entries, geographic, len(given) == 3)
    return exp.Array(
        expressions=[_number(exp.Literal.number(srid)), *(_number(lower(v)) for v in given)]
    )


def _srid(entries: dict, geographic: bool, three: bool) -> int:
    named = entries.get("srid")
    if isinstance(named, Literal) and named.kind == "number":
        return int(named.value)  # type: ignore[arg-type]
    crs = entries.get("crs")
    if isinstance(crs, Literal) and crs.kind == "string":
        if str(crs.value).lower() not in _SRID_OF_CRS:
            raise CypherTranslateError(f"Unknown coordinate reference system {crs.value!r}")
        return _SRID_OF_CRS[str(crs.value).lower()]
    if geographic:
        return _GEOGRAPHIC[three]
    return 9157 if three else 7203


def point_property(value: exp.Expression, name: str) -> exp.Expression:
    """A point's own part: a coordinate, its srid, or its crs."""

    def at(place: int) -> exp.Expression:
        return exp.Anonymous(
            this="element_at", expressions=[value.copy(), exp.Literal.number(place)]
        )

    if name in _COORDINATES:
        return at(_COORDINATES[name])
    srid = exp.Cast(this=at(1), to=exp.DataType.build("BIGINT"))
    if name == "srid":
        return srid
    if name == "crs":
        return exp.Case(
            this=srid,
            ifs=[
                exp.If(this=exp.Literal.number(code), true=exp.Literal.string(crs))
                for code, crs in _CRS.items()
            ],
        )
    raise CypherTranslateError(f"A point has no property {name!r}")


def returned(value: list) -> Point:
    """The engine's list for a point, as the point."""
    srid, *coordinates = value
    return Point(int(srid), tuple(float(c) for c in coordinates))


def bindable(value: Point) -> list[float]:
    """A client's point as the engine is handed it."""
    return [float(value.srid), *value.coordinates]
