# Copyright (c) 2026 Kenneth Stott
# Canary: 375261a6-e2fe-46f0-8d09-794f4f8f9c6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB SQL rewriting for the pg_catalog proxy (REQ-127, REQ-532).

Rewrites intercepted information_schema / pg_catalog queries to run against the
in-memory DuckDB catalog tables. Extracted from catalog.py; leaf module.
"""

# complexity-gate: allow-cc=89 allow-ble=2 reason="_rewrite_for_duckdb relocated verbatim from catalog.py (REQ-127); the high CC is the single pg_catalog->DuckDB rewrite dispatch (incl. pg_is_in_recovery/txid_current), and the two broad excepts fail-open to the raw SQL so a catalog query never crashes the pgwire session"

from __future__ import annotations

import itertools

import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.catalog_data import (
    _CATALOG_TABLE_NAMES,
    _INTERCEPT_SCHEMAS,
    _KNOWN_SETTINGS,
    _TABLE_MAP,
)
from provisa.pgwire.catalog_pg17 import FUNCTION_RETURNS, RELATIONS

# Monotonic process-lifetime transaction-id counter. Provisa has no real MVCC
# transaction ids; txid_current() clients only require a stable increasing bigint.
_TXID_COUNTER = itertools.count(1)


def next_txid() -> int:
    """Return the next monotonic bigint transaction id for txid_current()."""
    return next(_TXID_COUNTER)


# REQ-2167: what an emulated pg_catalog function answers, in the DuckDB type that is sent as the
# type PostgreSQL 17 says it returns (catalog_types names the column that type): a count is 0, as
# where nothing has happened; anything else is NULL, as for a time that never was.
_DUCKDB_TYPES = {
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "smallint": "SMALLINT",
    "oid": "UINTEGER",
    "xid": "UINTEGER",
    "double precision": "DOUBLE",
    "real": "FLOAT",
    "boolean": "BOOLEAN",
    "timestamp with time zone": "TIMESTAMPTZ",
    "text": "VARCHAR",
    "name": "VARCHAR",
    "inet": "VARCHAR",
}
_COUNTS = frozenset({"BIGINT", "INTEGER", "SMALLINT", "UINTEGER", "DOUBLE", "FLOAT"})


# REQ-2167: PostgreSQL subscripts an int2vector or oidvector (pg_index.indkey, indclass, ...) from
# 0 and any other array from 1; DuckDB's lists are subscripted from 1. A subscript of one of
# these columns is moved up by one, and array_lower / array_upper (which DuckDB lacks) and
# generate_subscripts answer as PostgreSQL does for each.
_VECTOR_COLUMNS = frozenset(
    name
    for columns in RELATIONS.values()
    for name, pg_type, _ in columns
    if pg_type in ("int2vector", "oidvector")
)


def _is_vector(node: exp.Expression, vectors: frozenset[str] = _VECTOR_COLUMNS) -> bool:
    while isinstance(node, exp.Paren):
        node = node.this
    return isinstance(node, exp.Column) and node.name.lower() in vectors


def _vector_names(tree: exp.Expr) -> frozenset[str]:
    """The vector columns, and the names the statement gives them (pgjdbc reads pg_index.indoption
    as tmp.I_INDOPTION), however many times renamed."""
    names = _VECTOR_COLUMNS
    while True:
        renamed = names | {
            alias.alias.lower()
            for alias in tree.find_all(exp.Alias)
            if _is_vector(alias.this, names)
        }
        if renamed == names:
            return names
        names = renamed


def _plus_one(node: exp.Expression) -> exp.Expression:
    return exp.Add(this=exp.Paren(this=node), expression=exp.Literal.number(1))


def _from_zero(subscript: exp.Expression) -> exp.Expression:
    """A vector's subscript (or slice bounds), counted from 0, as the list subscript from 1."""
    if isinstance(subscript, exp.Slice):
        return exp.Slice(
            this=_plus_one(subscript.this) if subscript.this else None,
            expression=_plus_one(subscript.expression) if subscript.expression else None,
        )
    return _plus_one(subscript)


def _array_bound(fn: str, array: exp.Expression, vectors: frozenset[str]) -> exp.Expression:
    """array_lower / array_upper of a one-dimensional array: NULL for an empty one, as in
    PostgreSQL; else its first or last subscript."""
    length = exp.Anonymous(this="len", expressions=[array])
    first = 0 if _is_vector(array, vectors) else 1
    bound = (
        exp.Literal.number(first)
        if fn == "array_lower"
        else exp.Sub(this=length, expression=exp.Literal.number(1 - first))
    )
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=length.copy(), expression=exp.Literal.number(0)), true=exp.null()
            )
        ],
        default=bound,
    )


def _expandarray_call(node: exp.Expression) -> exp.Anonymous | None:
    """``information_schema._pg_expandarray(a)`` (a setof (x, n): each element and its ordinal from
    1), when ``node`` is that call."""
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Dot):
        node = node.expression
    if isinstance(node, exp.Anonymous) and node.name.lower() == "_pg_expandarray":
        return node
    return None


def _expanded(array: exp.Expression, field: str) -> exp.Expression:
    """A field of _pg_expandarray(array): x, each element (unnest), or n, its ordinal from 1
    (generate_subscripts of the list, which DuckDB counts from 1 for any list)."""
    if field == "x":
        return exp.Anonymous(this="unnest", expressions=[array])
    return exp.Anonymous(this="generate_subscripts", expressions=[array, exp.Literal.number(1)])


def _index_column_name(index_oid: exp.Expression, column: exp.Expression) -> exp.Expression:
    """pg_get_indexdef(index, column, pretty) for a column from 1: that key column's name; for 0,
    the whole definition, which the catalog does not keep (NULL)."""
    name = (
        exp.select(exp.column("attname", table="a"))
        .from_("_pg_index AS ix")
        .join(
            "_pg_attribute AS a",
            on="a.attrelid = ix.indrelid",
            join_type="inner",
        )
        .where(
            exp.EQ(
                this=exp.column("attnum", table="a"),
                expression=exp.Anonymous(
                    this="list_extract", expressions=[exp.column("indkey", table="ix"), column]
                ),
            )
        )
        .where(exp.EQ(this=exp.column("indexrelid", table="ix"), expression=index_oid))
    )
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=column.copy(), expression=exp.Literal.number(0)), true=exp.null()
            )
        ],
        default=exp.Subquery(this=name),
    )


def _emulated(fn: str, untyped: exp.Expression) -> exp.Expression:
    """The answer of a pg_catalog function Provisa has nothing to report for; ``untyped`` where
    PostgreSQL's return type has no DuckDB counterpart (a record, or overloads that differ)."""
    duckdb_type = _DUCKDB_TYPES.get(FUNCTION_RETURNS.get(fn, ""))
    if duckdb_type is None:
        return untyped
    value = exp.Literal.number(0) if duckdb_type in _COUNTS else exp.null()
    return exp.cast(value, exp.DataType.build(duckdb_type, dialect="duckdb"))


# _pg_type.typname stores PG's internal type names (int4, bool, varchar, ...).
# Real format_type() returns the SQL display name (integer, boolean, character
# varying, ...); SQLAlchemy's PG dialect parses that display name against
# ischema_names and falls back to NullType — breaking DDL generation — for any
# name it doesn't recognize, so internal names must be mapped before they reach it.
_PG_TYPE_DISPLAY_NAMES: dict[str, str] = {
    "bool": "boolean",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "float4": "real",
    "float8": "double precision",
    "varchar": "character varying",
    "bpchar": "character",
    "timestamptz": "timestamp with time zone",
    "timetz": "time with time zone",
}

# Real PG relation oids for catalog classes. `'pg_extension'::regclass::oid`
# must land as a number — _pg_depend.refclassid is INTEGER — so the reg-cast
# name literal is mapped through this table instead of CAST('name' AS BIGINT).
_CATALOG_CLASS_OIDS: dict[str, int] = {
    "pg_default_acl": 826,
    "pg_tablespace": 1213,
    "pg_type": 1247,
    "pg_proc": 1255,
    "pg_class": 1259,
    "pg_authid": 1260,
    "pg_database": 1262,
    "pg_foreign_server": 1417,
    "pg_user_mapping": 1418,
    "pg_foreign_data_wrapper": 2328,
    "pg_am": 2601,
    "pg_attrdef": 2604,
    "pg_constraint": 2606,
    "pg_conversion": 2607,
    "pg_language": 2612,
    "pg_largeobject": 2613,
    "pg_namespace": 2615,
    "pg_opclass": 2616,
    "pg_operator": 2617,
    "pg_rewrite": 2618,
    "pg_trigger": 2620,
    "pg_opfamily": 2753,
    "pg_extension": 3079,
    "pg_policy": 3256,
    "pg_statistic_ext": 3381,
    "pg_collation": 3456,
    "pg_event_trigger": 3466,
    "pg_ts_dict": 3600,
    "pg_ts_config": 3602,
}

_REG_CAST_TYPES = frozenset(
    {
        "regclass",
        "regtype",
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regconfig",
        "regdictionary",
        "regrole",
        "regnamespace",
    }
)


def _rewrite_pg_cast(node, _transform):
    """Rewrite a PG-catalog-only cast into a DuckDB-compatible expression.

    Returns the replacement node, or None when the cast needs no rewrite.
    ``_transform`` is the enclosing ``_rewrite_for_duckdb`` walk, needed to
    recursively rewrite operands (e.g. a ``pg_get_serial_sequence(...)`` call)
    that this function splices into a brand-new subtree instead of returning
    unchanged for the top-down `.transform()` walk to revisit.
    """
    import sqlglot.expressions as exp

    dtype = node.args.get("to")
    dtype_str = str(dtype).lower() if dtype else ""
    if dtype_str in _REG_CAST_TYPES:
        # `'[schema.]name'::regclass` → unqualified `'name'`. Provisa's synthetic
        # pg_description stores classoid as the short relation name (e.g.
        # 'pg_class'), and DataGrip's comment queries filter
        # `classoid = 'pg_catalog.pg_class'::regclass`. Map the literal to its last
        # dotted component so the comparison matches — else every table/column
        # description drops. Non-literal operands (a real oid column, or a call
        # like pg_get_serial_sequence(...)) pass through untouched by the
        # top-down walk that got us here — transform explicitly so a rewrite
        # inside the operand (e.g. pg_get_serial_sequence → NULL) is not lost.
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_string:
            return exp.Literal.string(inner.this.rsplit(".", 1)[-1])
        return inner.transform(_transform)
    if dtype_str in ("oid", "xid", "tid", "cid"):
        # DuckDB has no oid/xid/tid/cid types. Preserve the operand's value by
        # re-casting to BIGINT instead of dropping it — e.g. DataGrip emits
        # `relnamespace = 2215::oid`, and collapsing to a literal 0 would silently
        # break the predicate. UBIGINT would match PG's unsigned oid domain, but
        # BIGINT covers every real catalog oid (< 2^31) and interops with signed
        # columns.
        inner = node.this
        if isinstance(inner, exp.Cast):
            # Chained cast (e.g. 'pg_extension'::regclass::oid): the transform
            # visits this outer cast before the inner one, and sqlglot's
            # exp.cast() builder raises on the not-yet-rewritten reg type.
            # Rewrite the inner cast first; _rewrite_pg_cast's own return paths
            # are already fully transformed, so no further transform is needed.
            inner = _rewrite_pg_cast(inner, _transform) or inner.transform(_transform)
        elif isinstance(inner, exp.Literal) and inner.is_string:
            class_oid = _CATALOG_CLASS_OIDS.get(inner.this.rsplit(".", 1)[-1])
            if class_oid is not None:
                return exp.Literal.number(class_oid)
        else:
            inner = inner.transform(_transform)
        if isinstance(inner, exp.Literal) and inner.is_string:
            class_oid = _CATALOG_CLASS_OIDS.get(inner.this.rsplit(".", 1)[-1])
            if class_oid is not None:
                return exp.Literal.number(class_oid)
        return exp.cast(inner, "BIGINT")
    if dtype_str == "name":
        return exp.cast(node.this.transform(_transform), "VARCHAR")
    return None


def _rewrite_for_duckdb(sql: str, role_id: str, database: str) -> str:
    """Rewrite catalog table refs for DuckDB and transpile from postgres dialect."""
    import sqlglot.expressions as exp
    import re as _pre_re

    # Convert PG array literals with type casts to DuckDB array syntax before
    # sqlglot parses — sqlglot misparses e.g. '{16395}'::oid[] (treats [] as
    # bracket indexing, not array type), causing a silent fallback to original SQL.
    # '{a,b,c}'::type[] → [a,b,c]
    sql = _pre_re.sub(
        r"'\{([^}]*)\}'::\w+(?:\[\])+",
        lambda m: "[" + m.group(1) + "]",
        sql,
    )
    # ARRAY[a,b]::type[] → ARRAY[a,b] (strip redundant cast; DuckDB infers type)
    sql = _pre_re.sub(r"(ARRAY\[[^\]]*\])::\w+(?:\[\])+", r"\1", sql, flags=_pre_re.IGNORECASE)

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql
    vectors = _vector_names(tree)

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Dot) and node.expression.name.lower() in ("x", "n"):
            call = _expandarray_call(node.this)
            if call is not None:
                return _expanded(
                    call.expressions[0].transform(_transform), node.expression.name.lower()
                )
        if isinstance(node, exp.Table) and _expandarray_call(node.this) is not None:
            # information_schema._pg_expandarray(a) [AS e[(x, n)]] in FROM: a lateral derived table.
            array = node.this.expressions[0].transform(_transform)
            alias = node.args.get("alias")
            names = [c.name for c in alias.columns] if alias and alias.columns else ["x", "n"]
            return exp.Subquery(
                this=exp.select(
                    exp.alias_(_expanded(array, "x"), names[0]),
                    exp.alias_(_expanded(array.copy(), "n"), names[1]),
                ),
                alias=exp.TableAlias(
                    this=exp.Identifier(this=alias.name if alias else "_pg_expandarray")
                ),
            )
        if isinstance(node, exp.Table):
            # Schema-qualified scalar function used as a TVF (e.g. pg_catalog.pg_indexam_has_property(...) amcanorder).
            # Rewrite to a lateral subquery so DuckDB can parse it.
            if isinstance(node.this, exp.Anonymous):
                if node.this.name.lower() == "pg_available_extension_versions":
                    # No DuckDB counterpart. DataGrip's RetrieveExtensions joins
                    # it for (name, version) rows; serve the installed surface
                    # versions from _pg_extension.
                    inner = exp.select(
                        exp.alias_(exp.column("extname"), "name"),
                        exp.alias_(exp.column("extversion"), "version"),
                    ).from_("_pg_extension")
                    return exp.Subquery(
                        this=inner,
                        alias=exp.TableAlias(
                            this=exp.Identifier(
                                this=node.alias or "pg_available_extension_versions",
                                quoted=False,
                            )
                        ),
                    )
                fn_result = _transform(node.this)
                col_name = node.alias if node.alias else node.this.name.lower()
                if fn_result is node.this:
                    # Function not rewritten to a scalar — strip schema qualifier so
                    # DuckDB can call its own TABLE macro (e.g. pg_available_extensions).
                    new_tbl = exp.Table(this=node.this)
                    if node.alias:
                        new_tbl.set("alias", node.args.get("alias"))
                    return new_tbl
                # Function rewritten to a scalar — wrap in a derived table so it is
                # valid in FROM/JOIN position (e.g. pg_indexam_has_property → false).
                inner = exp.select(exp.alias_(fn_result, col_name))
                return exp.Subquery(
                    this=inner,
                    alias=exp.TableAlias(
                        this=exp.Identifier(this=f"_tvf_{col_name}", quoted=False)
                    ),
                )
            db = node.db.lower() if node.db else ""
            name = node.name.lower() if node.name else ""
            mapped = _TABLE_MAP.get((db, name)) or (
                _TABLE_MAP.get(("pg_catalog", name))
                if not db and name in _CATALOG_TABLE_NAMES
                else None
            )
            if mapped:
                new_tbl = exp.Table(this=exp.Identifier(this=mapped, quoted=False))
                if node.alias:
                    new_tbl.set("alias", node.args.get("alias"))
                else:
                    # Preserve original name as alias so unqualified column refs
                    # like `pg_opclass.oid` continue to resolve after rename.
                    new_tbl.set(
                        "alias", exp.TableAlias(this=exp.Identifier(this=name, quoted=False))
                    )
                return new_tbl
        if isinstance(node, exp.Bracket) and _is_vector(node.this, vectors):
            return node.__class__(
                **{
                    **node.args,
                    "this": node.this.transform(_transform),
                    "expressions": [_from_zero(e.transform(_transform)) for e in node.expressions],
                }
            )
        if isinstance(node, exp.Anonymous):
            fn = node.name.lower()
            args = node.args.get("expressions", [])
            first_dimension = len(args) == 2 and args[1] == exp.Literal.number(1)
            if fn in ("array_lower", "array_upper") and first_dimension:
                return _array_bound(fn, args[0].transform(_transform), vectors)
            if fn == "generate_subscripts" and first_dimension and _is_vector(args[0], vectors):
                return exp.Sub(
                    this=exp.Anonymous(
                        this="generate_subscripts",
                        expressions=[args[0].transform(_transform), exp.Literal.number(1)],
                    ),
                    expression=exp.Literal.number(1),
                )
            if fn == "json_build_object":
                # DuckDB has no json_build_object; json_object takes the same
                # alternating key/value argument list.
                args = node.args.get("expressions", [])
                return exp.Anonymous(
                    this="json_object", expressions=[a.transform(_transform) for a in args]
                )
            if fn == "array_length":
                args = node.args.get("expressions", [])
                return exp.Anonymous(this="len", expressions=[args[0]] if args else [exp.null()])
            if fn == "current_schemas":
                args = node.args.get("expressions", [])
                include_implicit = True
                if args and isinstance(args[0], exp.Boolean):
                    include_implicit = args[0].this
                elif args and isinstance(args[0], exp.false().__class__):
                    include_implicit = False
                base = exp.select(
                    exp.Anonymous(this="list", expressions=[exp.column("nspname")])
                ).from_("_pg_namespace")
                if not include_implicit:
                    base = base.where("nspname NOT IN ('pg_catalog', 'information_schema')")
                return exp.Subquery(this=base)
            if "pg_get_userbyid" in fn or "pg_get_role_name" in fn:
                return exp.Literal.string("provisa")
            if fn == "pg_get_indexdef" and len(args) >= 2:
                return _index_column_name(
                    args[0].transform(_transform), args[1].transform(_transform)
                )
            if "pg_get_constraintdef" in fn:
                # The definition catalog_constraints recorded for the constraint, as PostgreSQL
                # writes it; the pretty flag changes nothing for a key or a foreign key.
                args = node.args.get("expressions", [])
                oid_expr = args[0].transform(_transform) if args else exp.null()
                subq = (
                    exp.select(exp.column("definition"))
                    .from_("_pg_constraintdef")
                    .where(exp.EQ(this=exp.column("oid"), expression=oid_expr))
                )
                return exp.Subquery(this=subq)
            if fn.startswith("pg_get_") or "pg_tablespace_location" in fn:
                return _emulated(fn, exp.null())
            if "pg_encoding_to_char" in fn:
                return exp.Literal.string("UTF8")
            if "format_type" in fn:
                # sqlglot visits parents before children, so args[0] here is still
                # the untransformed original expr (e.g. a schema-qualified column
                # like pg_catalog.pg_attribute.atttypid) — embedding it as-is into
                # the replacement subquery skips the db-qualifier strip below and
                # DuckDB then fails to bind it. Transform it explicitly first.
                args = node.args.get("expressions", [])
                typid_expr = args[0].transform(_transform) if args else exp.null()
                display_name = exp.Case(
                    this=exp.column("typname"),
                    ifs=[
                        exp.If(
                            this=exp.Literal.string(internal),
                            true=exp.Literal.string(display),
                        )
                        for internal, display in _PG_TYPE_DISPLAY_NAMES.items()
                    ],
                    default=exp.column("typname"),
                )
                subq = (
                    exp.select(display_name)
                    .from_("_pg_type")
                    .where(exp.EQ(this=exp.column("oid"), expression=typid_expr))
                )
                return exp.Subquery(this=subq)
            if "obj_description" in fn or "shobj_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0].transform(_transform) if args else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=exp.Literal.number(0)))
                )
                return exp.Subquery(this=subq)
            if "col_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0].transform(_transform) if args else exp.null()
                attnum_expr = args[1].transform(_transform) if len(args) > 1 else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=attnum_expr))
                )
                return exp.Subquery(this=subq)
            if any(
                p in fn
                for p in (
                    "pg_get_indexdef",
                    "pg_get_partkeydef",
                    "pg_get_partition",
                    "pg_get_serial_sequence",
                    "pg_get_userbyid",
                    "pg_get_ruledef",
                    "pg_get_triggerdef",
                    "pg_get_viewdef",
                )
            ):
                return exp.null()
            if "pg_postmaster_start_time" in fn or "pg_conf_load_time" in fn:
                return exp.null()
            if "pg_is_in_recovery" in fn:
                return exp.false()
            if "txid_current" in fn:
                return exp.Literal.number(next_txid())
            if "pg_is_other_temp_schema" in fn:
                return exp.false()
            if (
                "pg_function_is_visible" in fn
                or "pg_opclass_is_visible" in fn
                or "pg_type_is_visible" in fn
                or "pg_ts_config_is_visible" in fn
                or "pg_ts_dict_is_visible" in fn
                or "pg_ts_parser_is_visible" in fn
                or "pg_ts_template_is_visible" in fn
                or "pg_operator_is_visible" in fn
            ):
                return exp.true()
            if (
                "pg_relation_size" in fn
                or "pg_total_relation_size" in fn
                or "pg_indexes_size" in fn
            ):
                return exp.Literal.number(0)
            if "pg_stat_get" in fn:
                return _emulated(fn, exp.Literal.number(0))
            if "pg_table_is_visible" in fn or "pg_has_role" in fn:
                return exp.true()
            if fn == "encode":
                # PG encode(bytea, format) → return NULL; our catalog columns are VARCHAR not bytea
                return exp.null()
            if fn in (
                "pg_indexam_has_property",
                "pg_am_has_property",
                "pg_index_has_property",
                "pg_index_column_has_property",
            ):
                return exp.false()
            if fn in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            if fn in ("current_database",):
                return exp.Literal.string(database)
            if fn == "version":
                return exp.Literal.string("PostgreSQL 14.0 on Provisa")
            if "set_config" in fn:
                # set_config answers the value it set.
                args = node.args.get("expressions", [])
                return args[1].transform(_transform) if len(args) > 1 else exp.null()
            if "current_setting" in fn:
                args = node.args.get("expressions", [])
                key = args[0].name.lower() if args and isinstance(args[0], exp.Literal) else ""
                return exp.Literal.string(_KNOWN_SETTINGS.get(key, ""))
        if type(node).__name__ == "CurrentUser":
            return exp.Literal.string(role_id)
        if type(node).__name__ == "CurrentDatabase":
            return exp.Literal.string(database)
        if type(node).__name__ == "CurrentSchema":
            return exp.Literal.string("public")
        if isinstance(node, exp.Dot):
            # Strip schema qualifier from schema-qualified expressions: pg_catalog.TRUE → TRUE
            left = node.this
            if isinstance(left, exp.Identifier) and left.name.lower() in _INTERCEPT_SCHEMAS:
                # Re-apply transform to inner node so schema-qualified function calls
                # like pg_catalog.pg_encoding_to_char(...) are fully handled
                return _transform(node.expression)
        if isinstance(node, exp.EQ):
            # Rewrite `val = ANY(arr)` → `list_contains(arr, val)` so DuckDB
            # does not expand ANY into an internal subquery, which it rejects
            # on the outer side of non-inner JOINs.
            lhs, rhs = node.this, node.expression
            if isinstance(rhs, exp.Any):
                arr = rhs.this
                return exp.Anonymous(
                    this="list_contains",
                    expressions=[arr.transform(_transform), lhs.transform(_transform)],
                )
            if isinstance(lhs, exp.Any):
                arr = lhs.this
                return exp.Anonymous(
                    this="list_contains",
                    expressions=[arr.transform(_transform), rhs.transform(_transform)],
                )
        if isinstance(node, exp.Cast):
            rewritten_cast = _rewrite_pg_cast(node, _transform)
            if rewritten_cast is not None:
                return rewritten_cast
        if isinstance(node, exp.Column):
            if node.name.lower() in ("xmin", "xmax", "cmin", "cmax", "ctid"):
                return exp.cast(exp.Literal.number(0), "INTEGER")
            if node.name.lower() in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            # Rewrite schema-qualified column refs: pg_catalog.pg_class.col → _pg_class.col
            db_node = node.args.get("db") or node.args.get("catalog")
            db = db_node.name.lower() if db_node and hasattr(db_node, "name") else ""
            tbl = node.args.get("table")
            tname = tbl.name.lower() if tbl and hasattr(tbl, "name") else ""
            if db in _INTERCEPT_SCHEMAS and tname:
                return exp.column(node.name, table=tname)
        return node

    try:
        rewritten = tree.transform(_transform)
        # Move INNER JOINs before LEFT/RIGHT/FULL JOINs so DuckDB does not reject
        # forward alias references (e.g. `LEFT JOIN dsc ON c.oid=...` before
        # `INNER JOIN pg_class c` — c is not yet in scope).
        _outer_sides = {"LEFT", "RIGHT", "FULL"}
        for _sel in rewritten.find_all(exp.Select):
            _joins = _sel.args.get("joins") or []
            if len(_joins) > 1:
                _inner = [
                    j for j in _joins if (j.args.get("side") or "").upper() not in _outer_sides
                ]
                _outer = [j for j in _joins if (j.args.get("side") or "").upper() in _outer_sides]
                if _inner and _outer:
                    _sel.set("joins", _inner + _outer)
        sql_out = rewritten.sql(dialect="duckdb")
        # In real PG, oid is a hidden system column excluded from *. In our DuckDB tables,
        # oid is an explicit regular column, so "x.oid, x.*" returns oid twice. Remove the
        # duplicate by adding EXCLUDE on the star expression.
        import re as _re

        sql_out = _re.sub(
            r"(\w+)\.oid\s*,\s*\1\.\*",
            lambda m: f"{m.group(1)}.oid, {m.group(1)}.* EXCLUDE (oid)",
            sql_out,
        )
        return sql_out
    except Exception:
        return sql
