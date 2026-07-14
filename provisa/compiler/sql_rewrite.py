# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL identifier/type helpers and semantic<->physical rewrites (REQ-641).

Extracted from sql_gen.py: identifier quoting, join-column expression builders,
type-compatibility helpers, and semantic-name <-> physical/catalog SQL rewriting.
Leaf module: depends only on sql_types and sqlglot, never on sql_gen.
"""

from __future__ import annotations

import re as _re

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_types import CompilationContext, TableMeta

# --- Type coercion for cross-source JOINs ---

_NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "int",
    "bigint",
    "real",
    "double",
    "decimal",
    "numeric",
}
_STRING_TYPES = {"varchar", "char", "text", "varbinary", "bytea", "uuid"}
_TEMPORAL_TYPES = {"date", "time", "timestamp", "time with time zone", "timestamp with time zone"}


def _q(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


def _base_type(column_type: str) -> str:
    """Normalize parameterized types: varchar(100) → varchar, decimal(10,2) → decimal."""
    return column_type.lower().split("(")[0].strip()


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Check if two the engine types are implicitly coercible (no CAST needed)."""
    a, b = _base_type(type_a), _base_type(type_b)
    if a == b:
        return True
    for group in (_NUMERIC_TYPES, _STRING_TYPES, _TEMPORAL_TYPES):
        if a in group and b in group:
            return True
    return False


def _common_cast_type(type_a: str, type_b: str) -> str:
    """Pick a common type to CAST both sides to when types are incompatible."""
    a, b = _base_type(type_a), _base_type(type_b)
    # If one side is string, cast the other to VARCHAR
    if a in _STRING_TYPES:
        return "VARCHAR"
    if b in _STRING_TYPES:
        return "VARCHAR"
    # Numeric vs temporal — use VARCHAR as safe fallback
    return "VARCHAR"


def _sql_str_literal(val: str) -> str:
    """Escape and quote a string as a SQL VARCHAR literal."""
    return "VARCHAR '" + val.replace("'", "''") + "'"


def _join_column_expr_for(alias: str | None, column: str, my_type: str, other_type: str) -> str:
    """Build a column expression, adding CAST only when types are incompatible."""
    col = _q(column) if alias is None else f"{_q(alias)}.{_q(column)}"
    if _types_compatible(my_type, other_type):
        return col
    cast_type = _common_cast_type(my_type, other_type)
    return f"CAST({col} AS {cast_type})"


def _join_column_expr(alias: str, column: str, my_type: str, other_type: str) -> str:
    return _join_column_expr_for(alias, column, my_type, other_type)


# --- Table reference helpers ---


def _table_ref(meta: TableMeta, use_catalog: bool) -> str:
    """Build a fully qualified table reference."""
    if use_catalog:
        return f"{_q(meta.catalog_name)}.{_q(meta.schema_name)}.{_q(meta.table_name)}"
    return f"{_q(meta.schema_name)}.{_q(meta.table_name)}"


def semantic_table_name(meta: TableMeta) -> str:  # REQ-641
    """Bare (unquoted) semantic table name — central naming authority always."""
    from provisa.compiler.naming import apply_sql_name

    raw = (
        meta.display_name
        if meta.display_name
        else (meta.field_name.split("__", 1)[1] if "__" in meta.field_name else meta.field_name)
    )
    return apply_sql_name(raw)


def _semantic_table_ref(meta: TableMeta) -> str:
    """Semantic table reference: domain_schema.table_name (as JDBC clients see it)."""
    from provisa.compiler.naming import domain_to_sql_name

    return f"{_q(domain_to_sql_name(meta.domain_id))}.{_q(semantic_table_name(meta))}"


def _apply_replacements(sql: str, replacements: dict[str, str]) -> str:
    """Apply replacements to sql, longest match first (single-pass, no substring clobbering)."""
    if not replacements:
        return sql
    keys_sorted = sorted(replacements, key=len, reverse=True)

    pattern = _re.compile("|".join(_re.escape(k) for k in keys_sorted))
    return pattern.sub(lambda m: replacements[m.group(0)], sql)


def make_semantic_sql(sql: str, ctx: CompilationContext) -> str:  # REQ-641
    """Replace physical table refs with semantic (domain.field_name) refs."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        replacements[_table_ref(meta, use_catalog=False)] = _semantic_table_ref(meta)
        replacements[_table_ref(meta, use_catalog=True)] = _semantic_table_ref(meta)
    return _apply_replacements(sql, replacements)


def normalize_table_refs(sql: str, ctx: CompilationContext) -> str:  # REQ-641
    """Qualify and quote all table references using CompilationContext.

    For each exp.Table node in the parsed SQL:
    - Already schema-qualified (schema.table or "schema"."table") → re-emit quoted.
    - Unqualified (table only) + unique match in ctx → add schema + quote.
    - Unqualified + ambiguous (multiple schemas) → leave unchanged.
    - Unqualified + no match → leave unchanged (governance will reject).
    """
    import sqlglot.expressions as exp

    # Build lookup structures from ctx
    # table_name (lower) → list of (schema_name, table_name) physical pairs
    by_name: dict[str, list[tuple[str, str]]] = {}
    # (schema_lower, name_lower) → (schema_name, table_name) canonical pair
    by_schema_name: dict[tuple[str, str], tuple[str, str]] = {}

    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        nl = meta.table_name.lower()
        sl = meta.schema_name.lower()
        by_name.setdefault(nl, []).append((meta.schema_name, meta.table_name))
        by_schema_name[(sl, nl)] = (meta.schema_name, meta.table_name)
        # Also map pre-alias name → physical name (e.g. "registered_tables" → "registered_tables_meta")
        orig_nl: str | None = meta.original_table_name.lower() if meta.original_table_name else None
        if orig_nl is not None:
            by_name.setdefault(orig_nl, []).append((meta.schema_name, meta.table_name))
            by_schema_name[(sl, orig_nl)] = (meta.schema_name, meta.table_name)
        # Map domain-name schema variant → physical (e.g. "shelter"."shelter__animal_breeds" → "graphql_remote"."shelter__animal_breeds")
        if meta.domain_id:
            from provisa.compiler.naming import domain_to_sql_name

            domain_sl = domain_to_sql_name(meta.domain_id).lower()
            if domain_sl != sl:
                by_schema_name[(domain_sl, nl)] = (meta.schema_name, meta.table_name)
                if orig_nl is not None:
                    by_schema_name[(domain_sl, orig_nl)] = (meta.schema_name, meta.table_name)

    # Parse failure must fail loud: returning un-rewritten SQL skips physical/catalog qualification.
    tree = sqlglot.parse_one(sql, read="postgres")

    def _rewrite(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if not isinstance(node, exp.Table):
            return node
        name = node.name
        db = node.db  # schema
        alias = node.alias

        if db:
            # Already schema-qualified — just ensure quoting
            canonical = by_schema_name.get((db.lower(), name.lower()))
            if canonical:
                schema_q, table_q = canonical
            else:
                schema_q, table_q = db, name
        else:
            # Unqualified — try unique match
            matches = by_name.get(name.lower(), [])
            if len(matches) == 1:
                schema_q, table_q = matches[0]
            else:
                return node  # ambiguous or unknown — leave unchanged

        new_tbl = exp.Table(
            this=exp.Identifier(this=table_q, quoted=True),
            db=exp.Identifier(this=schema_q, quoted=True),
            alias=exp.TableAlias(this=exp.Identifier(this=alias)) if alias else None,
        )
        return new_tbl

    tree = tree.transform(_rewrite)
    return tree.sql(dialect="postgres")


def rewrite_semantic_to_physical(sql: str, ctx: CompilationContext) -> str:  # REQ-641
    """Replace semantic (domain.field_name) refs with physical (schema.table) refs."""
    from provisa.compiler.naming import domain_to_sql_name

    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        physical = _table_ref(meta, use_catalog=False)
        replacements[semantic] = physical
        if "__" in meta.field_name:
            table_part = meta.field_name.split("__", 1)[1]
        else:
            table_part = meta.field_name
        domain_sql = domain_to_sql_name(meta.domain_id)
        ref = f"{_q(domain_sql)}.{_q(table_part)}"
        if ref not in replacements:
            replacements[ref] = physical
    sql = _apply_replacements(sql, replacements)
    return normalize_table_refs(sql, ctx)


def _all_table_metas(ctx: CompilationContext) -> list[TableMeta]:
    """Return all TableMeta instances from ctx: root tables plus all join targets."""
    metas: list[TableMeta] = list(ctx.tables.values())
    for jm in ctx.joins.values():
        metas.append(jm.target)
    return metas


def rewrite_semantic_to_catalog_physical(sql: str, ctx: CompilationContext) -> str:  # REQ-641
    """Replace semantic and physical table refs with catalog-qualified physical refs.

    Handles both semantic refs (domain.field_name, produced by make_semantic_sql for root
    tables) and physical refs without catalog (schema.table, left by make_semantic_sql for
    join targets that are not in ctx.tables). Distinct from ``rewrite_semantic_to_physical``,
    which only rewrites semantic → uncatalogued schema.table.
    """
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str, str]] = set()
    for meta in _all_table_metas(ctx):
        key = (meta.catalog_name, meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        physical_no_catalog = _table_ref(meta, use_catalog=False)
        physical_with_catalog = _table_ref(meta, use_catalog=True)
        replacements[semantic] = physical_with_catalog
        # Also replace bare physical refs (e.g. "signals"."queries") that make_semantic_sql
        # did not convert because join targets are not in ctx.tables.
        if physical_no_catalog not in replacements:
            replacements[physical_no_catalog] = physical_with_catalog
    return _apply_replacements(sql, replacements)


def qualify_with_catalogs(sql: str, ctx: CompilationContext) -> str:  # REQ-641
    """Add catalog prefix to physical table refs: "schema"."table" → "catalog"."schema"."table"."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in _all_table_metas(ctx):
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        replacements[_table_ref(meta, use_catalog=False)] = _table_ref(meta, use_catalog=True)
    return _apply_replacements(sql, replacements)


def strip_catalog(sql: str) -> str:  # REQ-863
    """Drop the catalog segment from every table ref: "cat"."schema"."table" → "schema"."table".

    Structural, AST-only (REQ-913): used to lower a post-governance-optimized catalog-physical
    query onto the DIRECT route, where a native driver addresses schema.table (no catalog).
    VALUES-CTE relations left by inlining carry no catalog and are untouched.
    """
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(sql, read="postgres")
    for tbl in tree.find_all(exp.Table):
        if tbl.args.get("catalog") is not None:
            tbl.set("catalog", None)
    return tree.sql(dialect="postgres")


# --- Main compilation ---
