# Copyright (c) 2026 Kenneth Stott
# Canary: 7f1a9c62-3b48-4e07-8d21-5a6c9f0b2e14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable PostgreSQL extension surfaces over the federation engine (REQ-892).

Opt-in-per-deployment: the ``PROVISA_PGWIRE_EXT_SURFACES`` env var selects which
surfaces a pgwire endpoint presents (comma-separated, e.g. ``pgvector,json-ops``).
Each enabled surface (a) advertises itself in ``pg_extension`` so clients see it
"installed", (b) declares its types/OIDs through the single normalization module
(``catalog_data``), and (c) maps its operators/functions to a federation-engine
function OR rejects them loudly.

Convert-or-reject-loudly: a surface never claims a capability it does not
implement. pgvector presents the distance operators and exact ORDER BY similarity
scan but MUST NOT advertise ivfflat/hnsw index acceleration — a CREATE INDEX that
asks for one is rejected with a clear error rather than silently ignored.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

_ENV_VAR = "PROVISA_PGWIRE_EXT_SURFACES"

# pg_trgm's default similarity threshold; the `%` operator is `similarity(a,b) >= 0.3`.
_PG_TRGM_THRESHOLD = 0.3

# Surface type OIDs live in a private high range (8300-8399) so they never collide
# with the 8001+ pg_catalog-table / 9001+ information_schema-view OIDs.
_VECTOR_OID = 8300
_GEOMETRY_OID = 8310
_GEOGRAPHY_OID = 8311

# pgcrypto digest() algorithm → federation-engine hash function.
_DIGEST_FUNCS = {"md5": "md5", "sha1": "sha1", "sha256": "sha256", "sha512": "sha512"}


@dataclass(frozen=True)
class SurfaceType:
    """One type a surface declares in the single normalization module."""

    oid: int
    name: str
    # _PG_TYPE_ROWS columns: typlen, typtype, typcategory, typnotnull, typbasetype,
    # typbyval, typalign, typstorage.
    typlen: int = -1
    typtype: str = "b"
    typcategory: str = "U"
    typalign: str = "i"
    typstorage: str = "x"


@dataclass(frozen=True)
class Surface:
    """A pluggable extension surface: extensions advertised + types declared."""

    key: str
    extensions: tuple[tuple[str, str], ...]  # (extname, extversion)
    types: tuple[SurfaceType, ...] = field(default=())


_SURFACES: dict[str, Surface] = {
    # pgvector: vector type + distance operators. NO ivfflat/hnsw index acceleration.
    "pgvector": Surface(
        key="pgvector",
        extensions=(("vector", "0.7.0"),),
        types=(SurfaceType(_VECTOR_OID, "vector", typcategory="A"),),
    ),
    # PostGIS-subset: geometry/geography types + ST_* function pass-through (partial —
    # geography is presented as an alias of geometry; no SRID transforms).
    "postgis": Surface(
        key="postgis",
        extensions=(("postgis", "3.4.0"),),
        types=(
            SurfaceType(_GEOMETRY_OID, "geometry"),
            SurfaceType(_GEOGRAPHY_OID, "geography"),
        ),
    ),
    # JSON operators are core PostgreSQL, not an extension — nothing to advertise in
    # pg_extension. The surface only remaps -> ->> #> #>> to JSON_QUERY/JSON_VALUE.
    "json-ops": Surface(key="json-ops", extensions=()),
    # Compatibility functions: pg_trgm similarity(), gen_random_uuid(), pgcrypto digest().
    "compat-fns": Surface(
        key="compat-fns",
        extensions=(("pg_trgm", "1.6"), ("pgcrypto", "1.3")),
    ),
}


def enabled_surface_keys() -> frozenset[str]:
    """Return the surface keys enabled for this deployment (env opt-in)."""
    raw = os.environ.get(_ENV_VAR, "")
    keys = {k.strip().lower() for k in raw.split(",") if k.strip()}
    unknown = keys - _SURFACES.keys()
    if unknown:
        raise ValueError(
            f"{_ENV_VAR}: unknown surface(s) {sorted(unknown)}; "
            f"known surfaces are {sorted(_SURFACES)}"
        )
    return frozenset(keys)


def enabled_surfaces() -> list[Surface]:
    """Return the Surface objects enabled for this deployment."""
    return [_SURFACES[k] for k in sorted(enabled_surface_keys())]


def extension_rows() -> list[tuple]:
    """Rows for _pg_extension advertising every enabled surface's extension(s)."""
    rows: list[tuple] = []
    oid = 90000  # private extension OID range
    for surface in enabled_surfaces():
        for extname, extversion in surface.extensions:
            # oid, extname, extowner, extnamespace, extrelocatable, extversion,
            # extconfig, extcondition
            rows.append((oid, extname, 10, 11, False, extversion, None, None))
            oid += 1
    return rows


def surface_pg_type_rows() -> list[tuple]:
    """_PG_TYPE_ROWS-shaped rows for every enabled surface type (normalization module)."""
    rows: list[tuple] = []
    for surface in enabled_surfaces():
        for t in surface.types:
            rows.append(
                (
                    t.oid,
                    t.name,
                    11,
                    t.typlen,
                    t.typtype,
                    t.typcategory,
                    False,
                    0,
                    False,
                    t.typalign,
                    t.typstorage,
                )
            )
    return rows


def surface_typeinfo() -> dict[int, tuple]:
    """_TYPEINFO-shaped {oid: (ns, name, kind, base, elem, delim, range)} for enabled types."""
    info: dict[int, tuple] = {}
    for surface in enabled_surfaces():
        for t in surface.types:
            info[t.oid] = ("public", t.name, t.typtype, None, None, None, None)
    return info


# --- Convert-or-reject-loudly: unimplemented capabilities --------------------

# pgvector ANN index access methods Provisa does NOT implement. Similarity search
# is an exact sequential scan; a client asking for an ANN index is rejected, never
# silently accepted (which would falsely claim index acceleration).
_ANN_INDEX_RE = re.compile(r"\bUSING\s+(ivfflat|hnsw)\b", re.IGNORECASE)


def reject_unimplemented(sql: str, keys: frozenset[str]) -> None:
    """Raise if *sql* asks for a capability an enabled surface does not implement."""
    if "pgvector" in keys:
        m = _ANN_INDEX_RE.search(sql)
        if m:
            raise ValueError(
                f"pgvector surface does not implement {m.group(1).lower()} index "
                "acceleration; similarity search runs as an exact sequential scan. "
                "Remove the USING clause (a plain index or none)."
            )


# --- Operator / function mapping ---------------------------------------------

# `<#>` (negative inner product) does not parse in sqlglot, so it is rewritten
# textually before parsing. Operands cover pgvector usage: params, ARRAY[...],
# casted literals, and (qualified) column references.
_OPERAND = r"(\$\d+|ARRAY\[[^\]]*\]|'[^']*'(?:::\w+)?|[A-Za-z_][\w.]*(?:::\w+)?)"
_INNER_PROD_RE = re.compile(rf"{_OPERAND}\s*<#>\s*{_OPERAND}")


def _rewrite_inner_product(sql: str) -> str:
    """Textually rewrite ``a <#> b`` → ``array_negative_inner_product(a, b)``."""
    prev = None
    while prev != sql:
        prev = sql
        sql = _INNER_PROD_RE.sub(r"array_negative_inner_product(\1, \2)", sql)
    return sql


def _json_path(node: exp.Expression) -> str:
    """Build a SQL/JSON path (``$.a.b``) from a -> / #> right operand."""
    if isinstance(node, exp.JSONPath):
        parts = [p.name for p in node.expressions if isinstance(p, exp.JSONPathKey)]
        return "$." + ".".join(parts) if parts else "$"
    if isinstance(node, exp.Literal) and node.is_string:
        keys = [k for k in node.this.strip("{}").split(",") if k]
        return "$." + ".".join(keys) if keys else "$"
    return "$"


def _map_json(node: exp.Expression) -> exp.Expression:
    """Map JSON operators to JSON_QUERY (object/array) or JSON_VALUE (scalar text)."""
    fn = None
    if isinstance(node, (exp.JSONExtract, exp.JSONBExtract)):
        fn = "JSON_QUERY"
    elif isinstance(node, (exp.JSONExtractScalar, exp.JSONBExtractScalar)):
        fn = "JSON_VALUE"
    if fn is None:
        return node
    path = _json_path(node.expression)
    return exp.Anonymous(this=fn, expressions=[node.this, exp.Literal.string(path)])


def _map_vector(node: exp.Expression) -> exp.Expression:
    """Map parsed pgvector distance operators to engine array-distance functions."""
    if isinstance(node, exp.Distance):  # a <-> b  (L2)
        return exp.Anonymous(this="array_distance", expressions=[node.this, node.expression])
    if isinstance(node, exp.NullSafeEQ):  # a <=> b  (cosine); sqlglot mis-tags <=>
        return exp.Anonymous(this="array_cosine_distance", expressions=[node.this, node.expression])
    return node


def _map_compat(node: exp.Expression) -> exp.Expression:
    """Map compatibility functions; reject an unknown digest algorithm loudly."""
    if isinstance(node, exp.Uuid):  # gen_random_uuid()
        return exp.Anonymous(this="uuid", expressions=[])
    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        if name == "similarity":  # pg_trgm trigram similarity
            return exp.Anonymous(this="jaccard", expressions=list(node.expressions))
        if name == "digest":
            return _map_digest(node)
    return node


def _map_digest(node: exp.Anonymous) -> exp.Expression:
    args = list(node.expressions)
    algo_node = args[1] if len(args) > 1 else None
    algo = algo_node.this.lower() if isinstance(algo_node, exp.Literal) else None
    engine_fn = _DIGEST_FUNCS.get(algo) if algo else None
    if engine_fn is None:
        raise ValueError(
            f"pgcrypto digest(): algorithm {algo!r} is not implemented by the "
            f"compat-fns surface; supported algorithms are {sorted(_DIGEST_FUNCS)}."
        )
    return exp.Anonymous(this=engine_fn, expressions=[args[0]])


def _map_postgis(node: exp.Expression) -> exp.Expression:
    """Map the && bbox-overlap operator to ST_Intersects (partial: bbox ≈ geometry)."""
    if isinstance(node, exp.ArrayOverlaps):
        return exp.Anonymous(this="ST_Intersects", expressions=[node.this, node.expression])
    return node


_MAPPERS = {
    "pgvector": _map_vector,
    "json-ops": _map_json,
    "compat-fns": _map_compat,
    "postgis": _map_postgis,
}


def rewrite_surface_operators(sql: str) -> str:
    """Rewrite enabled-surface operators/functions to engine equivalents.

    Rejects loudly any request for an unimplemented capability. When no surface is
    enabled, returns *sql* unchanged (passthrough).
    """
    keys = enabled_surface_keys()
    if not keys:
        return sql
    reject_unimplemented(sql, keys)
    mappers = [_MAPPERS[k] for k in keys if k in _MAPPERS]
    if not mappers:
        return sql
    if "pgvector" in keys:
        sql = _rewrite_inner_product(sql)
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except SqlglotError:
        return sql

    def _transform(node: exp.Expression) -> exp.Expression:
        for mapper in mappers:
            new = mapper(node)
            if new is not node:
                return new
        return node

    return tree.transform(_transform).sql(dialect="postgres")
