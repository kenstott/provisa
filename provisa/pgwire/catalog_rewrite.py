# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
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

# Monotonic process-lifetime transaction-id counter. Provisa has no real MVCC
# transaction ids; txid_current() clients only require a stable increasing bigint.
_TXID_COUNTER = itertools.count(1)


def next_txid() -> int:
    """Return the next monotonic bigint transaction id for txid_current()."""
    return next(_TXID_COUNTER)


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


def _rewrite_pg_cast(node):
    """Rewrite a PG-catalog-only cast into a DuckDB-compatible expression.

    Returns the replacement node, or None when the cast needs no rewrite.
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
        # description drops. Non-literal operands (a real oid column) pass through.
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_string:
            return exp.Literal.string(inner.this.rsplit(".", 1)[-1])
        return inner
    if dtype_str in ("oid", "xid", "tid", "cid"):
        # DuckDB has no oid/xid/tid/cid types. Preserve the operand's value by
        # re-casting to BIGINT instead of dropping it — e.g. DataGrip emits
        # `relnamespace = 2215::oid`, and collapsing to a literal 0 would silently
        # break the predicate. UBIGINT would match PG's unsigned oid domain, but
        # BIGINT covers every real catalog oid (< 2^31) and interops with signed
        # columns.
        return exp.cast(node.this, "BIGINT")
    if dtype_str == "name":
        return exp.cast(node.this, "VARCHAR")
    return None


def _rewrite_for_duckdb(sql: str, role_id: str = "") -> str:
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

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Table):
            # Schema-qualified scalar function used as a TVF (e.g. pg_catalog.pg_indexam_has_property(...) amcanorder).
            # Rewrite to a lateral subquery so DuckDB can parse it.
            if isinstance(node.this, exp.Anonymous):
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
        if isinstance(node, exp.Anonymous):
            fn = node.name.lower()
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
            if fn.startswith("pg_get_") or "pg_tablespace_location" in fn:
                return exp.null()
            if "pg_encoding_to_char" in fn:
                return exp.Literal.string("UTF8")
            if "format_type" in fn:
                args = node.args.get("expressions", [])
                typid_expr = args[0] if args else exp.null()
                subq = (
                    exp.select(exp.column("typname"))
                    .from_("_pg_type")
                    .where(exp.EQ(this=exp.column("oid"), expression=typid_expr))
                )
                return exp.Subquery(this=subq)
            if "obj_description" in fn or "shobj_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=exp.Literal.number(0)))
                )
                return exp.Subquery(this=subq)
            if "col_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                attnum_expr = args[1] if len(args) > 1 else exp.null()
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
                    "pg_get_constraintdef",
                    "pg_get_expr",
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
                or "pg_stat_get" in fn
            ):
                return exp.Literal.number(0)
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
                return exp.Literal.string("provisa")
            if fn == "version":
                return exp.Literal.string("PostgreSQL 14.0 on Provisa")
            if "set_config" in fn:
                return exp.null()
            if "current_setting" in fn:
                args = node.args.get("expressions", [])
                key = args[0].name.lower() if args and isinstance(args[0], exp.Literal) else ""
                return exp.Literal.string(_KNOWN_SETTINGS.get(key, ""))
        if type(node).__name__ == "CurrentUser":
            return exp.Literal.string(role_id)
        if type(node).__name__ == "CurrentDatabase":
            return exp.Literal.string("provisa")
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
            rewritten_cast = _rewrite_pg_cast(node)
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
