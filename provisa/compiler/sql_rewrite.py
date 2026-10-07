# Copyright (c) 2026 Kenneth Stott
# Canary: f44c4f72-a8e5-4f4e-a326-1f5eb12cc716
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

from provisa.compiler.sql_literals import sql_literal

import re as _re
from datetime import datetime, timezone

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.core.ir_types import EPOCH_UNITS


def split_sql_statements(sql: str) -> list[str]:
    """Split a batch into statements on TOP-LEVEL semicolons ONLY, statement-aware.

    Uses sqlglot's tokenizer so a ``;`` inside a string literal, quoted identifier, comment, or a
    dollar-quoted block does NOT mis-split (a naive ``str.split(';')`` turns ``SELECT 'a;b'`` into two
    malformed fragments). Original statement text is preserved (sliced between top-level semicolon
    tokens, not re-rendered), so COPY/CTAS regex matching and governance see EXACTLY what executes —
    closing the parser-differential where the batch was tokenized one way for governance and another
    at execution. Blank fragments (a trailing ``;``) are dropped."""
    tokens = sqlglot.tokenize(sql, read="postgres")
    stmts: list[str] = []
    start = 0
    for tok in tokens:
        if tok.token_type == sqlglot.TokenType.SEMICOLON:
            seg = sql[start : tok.start].strip()
            if seg:
                stmts.append(seg)
            start = tok.end + 1
    tail = sql[start:].strip()
    if tail:
        stmts.append(tail)
    return stmts


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
    """Escape and quote a string as a SQL VARCHAR literal (Trino)."""
    return "VARCHAR " + sql_literal(val, "trino")


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
    # (schema_lower, name_lower) → (catalog_name, schema_name, table_name) canonical triple
    by_schema_name: dict[tuple[str, str], tuple[str, str, str]] = {}

    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        nl = meta.table_name.lower()
        sl = meta.schema_name.lower()
        by_name.setdefault(nl, []).append((meta.schema_name, meta.table_name))
        by_schema_name[(sl, nl)] = (meta.catalog_name, meta.schema_name, meta.table_name)
        # Also map pre-alias name → physical name (e.g. "registered_tables" → "registered_tables_meta")
        orig_nl: str | None = meta.original_table_name.lower() if meta.original_table_name else None
        if orig_nl is not None:
            by_name.setdefault(orig_nl, []).append((meta.schema_name, meta.table_name))
            by_schema_name[(sl, orig_nl)] = (meta.catalog_name, meta.schema_name, meta.table_name)
        # Map domain-name schema variant → physical (e.g. "shelter"."shelter__animal_breeds" → "graphql_remote"."shelter__animal_breeds")
        if meta.domain_id:
            from provisa.compiler.naming import domain_to_sql_name

            domain_sl = domain_to_sql_name(meta.domain_id).lower()
            if domain_sl != sl:
                by_schema_name[(domain_sl, nl)] = (
                    meta.catalog_name,
                    meta.schema_name,
                    meta.table_name,
                )
                if orig_nl is not None:
                    by_schema_name[(domain_sl, orig_nl)] = (
                        meta.catalog_name,
                        meta.schema_name,
                        meta.table_name,
                    )

    # Parse failure must fail loud: returning un-rewritten SQL skips physical/catalog qualification.
    tree = sqlglot.parse_one(sql, read="postgres")
    quoted_aliases: set[str] = set()

    def _rewrite(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if not isinstance(node, exp.Table):
            return node
        name = node.name
        db = node.db  # schema
        catalog = node.catalog
        alias = node.alias

        if db:
            # Already schema-qualified — just ensure quoting
            canonical = by_schema_name.get((db.lower(), name.lower()))
            if canonical:
                catalog_q, schema_q, table_q = canonical
            else:
                # No canonical match — preserve whatever catalog the input already carried
                # (e.g. hand-authored view_sql already written as "catalog"."schema"."table")
                # rather than silently dropping it.
                catalog_q, schema_q, table_q = catalog, db, name
        else:
            # Unqualified — try unique match
            matches = by_name.get(name.lower(), [])
            if len(matches) == 1:
                schema_q, table_q = matches[0]
                catalog_q = None
            else:
                return node  # ambiguous or unknown — leave unchanged

        # An unaliased ref's own name is what column qualifiers in the surrounding query bind
        # to (e.g. `"inventory"."count"` with no explicit `AS`). rewrite_semantic_to_catalog_physical
        # runs after this and renames the ref to its physical name via text substitution — without
        # an explicit alias here, that later rename silently strands those column qualifiers,
        # producing "missing FROM-clause entry" / "no such table" at execution.
        alias_q = alias if alias else name
        quoted_aliases.add(alias_q)
        # REQ-1942: a TRUNCATE's target takes no alias -- its grammar has no place for one, and
        # it has no column qualifiers to keep bound.
        truncated = isinstance(node.parent, exp.TruncateTable)
        new_tbl = exp.Table(
            this=exp.Identifier(this=table_q, quoted=True),
            db=exp.Identifier(this=schema_q, quoted=True),
            catalog=exp.Identifier(this=catalog_q, quoted=True) if catalog_q else None,
            # Quoted like the table/schema/catalog identifiers beside it: the alias carries a
            # SEMANTIC name, which is free to be a reserved word ("order", "user", "table") and
            # to differ in case from the physical one. Unquoted, `FROM "public"."orders" AS order`
            # is a syntax error. Column qualifiers referencing this alias are quoted separately
            # below (they're built unquoted upstream, e.g. graph_rewriter._build_row_cast) —
            # Postgres folds an unquoted qualifier to lowercase, which stops matching a quoted
            # mixed-case alias like "mRegisteredTa".
            alias=None
            if truncated
            else exp.TableAlias(this=exp.Identifier(this=alias_q, quoted=True)),
            # REQ-1934: a TABLESAMPLE on the ref is part of what it reads; rebuilt without it, a
            # block-sampled profile statement became a whole-table read.
            sample=node.args.get("sample"),
        )
        return new_tbl

    tree = tree.transform(_rewrite)
    _lower_column_aliases(tree, ctx)

    for col in tree.find_all(exp.Column):
        tbl_id = col.args.get("table")
        if isinstance(tbl_id, exp.Identifier) and tbl_id.this in quoted_aliases:
            tbl_id.set("quoted", True)

    return tree.sql(dialect="postgres")


def _column_renames(ctx: CompilationContext) -> dict[tuple[str, str], dict[str, str]]:
    """Per physical ``(schema, table)`` (lowercased), each published column name that is not the
    column's physical name, lowercased, to that physical name -- an ``alias``, or a SQL naming
    convention's rename (``CompilationContext.physical_to_sql``). A published name that is also a
    physical column of the table is left out: lowering never renames a physical name, so it is
    idempotent and safe on SQL that is already physical."""
    by_id: dict[int, tuple[str, str]] = {
        m.table_id: (m.schema_name.lower(), m.table_name.lower()) for m in _all_table_metas(ctx)
    }
    physical: dict[tuple[str, str], set[str]] = {}
    for (table_id, phys), _ in ctx.physical_to_sql.items():
        if table_id in by_id:
            physical.setdefault(by_id[table_id], set()).add(phys.lower())
    out: dict[tuple[str, str], dict[str, str]] = {}
    for (table_id, phys), exposed in ctx.physical_to_sql.items():
        key = by_id.get(table_id)
        if key is None or exposed == phys or exposed.lower() in physical[key]:
            continue
        out.setdefault(key, {})[exposed.lower()] = phys
    return out


# A derived source whose column names are unknown.
_ANY_NAME: set[str] = set()


def _lower_column_aliases(tree: exp.Expression, ctx: CompilationContext) -> None:
    """Rename, IN PLACE, every reference to a registered table's published column name to its
    physical name (issue #140): select lists, WHERE, GROUP/ORDER BY, function arguments, joins,
    subqueries and CTEs, qualified or not. Runs after the table refs are resolved to physical
    ``(schema, table)``. A renamed column selected bare keeps its published name as its output
    name, so the result -- and an outer query reading a derived table -- sees the published name.
    An unqualified name is renamed only where exactly one source of its scope publishes it."""
    from sqlglot.optimizer.scope import traverse_scope

    renames = _column_renames(ctx)
    if not renames or not isinstance(tree, exp.Query):
        return
    for scope in traverse_scope(tree):
        tables: dict[str, dict[str, str]] = {}
        derived: list[set[str]] = []
        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                found = renames.get((source.db.lower(), source.name.lower()))
                if found:
                    tables[name] = found
            elif isinstance(source.expression, exp.Query):
                derived.append({n.lower() for n in source.expression.named_selects})
            else:
                # A source whose columns are not a query's named selects (VALUES, a table
                # function): it may hold any name, so no unqualified name is resolved past it.
                derived.append(_ANY_NAME)
        if not tables:
            continue
        renamed: dict[int, str] = {}
        for col in scope.columns:
            published = col.name
            key = published.lower()
            if col.table:
                mapping = tables.get(col.table)
                physical = None if mapping is None else mapping.get(key)
            else:
                owners = [m[key] for m in tables.values() if key in m]
                plain = [
                    s
                    for n, s in scope.sources.items()
                    if isinstance(s, exp.Table) and n not in tables
                ]
                ambiguous = (
                    len(owners) != 1
                    or any(d is _ANY_NAME or key in d for d in derived)
                    or bool(plain)
                )
                physical = None if ambiguous else owners[0]
            if physical is None:
                continue
            col.set("this", exp.Identifier(this=physical, quoted=True))
            renamed[id(col)] = published
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        for projection in list(select.expressions):
            if isinstance(projection, exp.Column) and id(projection) in renamed:
                published = renamed[id(projection)]
                projection.replace(exp.alias_(projection.copy(), published, quoted=True))


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
    # normalize_table_refs FIRST — the same order the ENGINE lowering uses. It pins an unaliased
    # ref's own name as an explicit alias, which is what the query's column qualifiers bind to
    # ("query_audit_log"."id" against `FROM "ops"."query_audit_log"`). Substituting first renamed
    # the ref to its physical name before that alias existed, so normalize then aliased it to the
    # PHYSICAL name and the qualifiers were stranded — "missing FROM-clause entry for table
    # query_audit_log" on every ops/meta table whose physical name differs from its semantic one.
    sql = _apply_replacements(normalize_table_refs(sql, ctx), replacements)
    # DIRECT route: a native driver addresses schema.table, not catalog.schema.table — strip the
    # catalog normalize_table_refs re-attaches for already-qualified refs (REQ-641/REQ-863).
    return translate_epoch_temporal_columns(strip_catalog(sql), ctx.epoch_columns)  # REQ-1908


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
    from provisa.compiler.naming import source_to_catalog

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
        # Anchor already-catalog-qualified refs so the shorter no-catalog key cannot match
        # them as a substring (longest-first regex wins; self-mapping is a no-op).
        replacements.setdefault(physical_with_catalog, physical_with_catalog)
        # Hand-authored SQL (a view_sql / MV body in config) addresses a source by its BASE
        # catalog name — the org-prefixed name is a runtime fact of REQ-1266, not something an
        # author writes. Without this key the bare-catalog spelling is unanchored, so the shorter
        # "schema"."table" key matches its TAIL and splices the org catalog into the middle:
        # pet_store_sqlite.org_kstott__pet_store_sqlite.pet_store.pets ("Too many dots").
        base_catalog_ref = f"{_q(source_to_catalog(meta.source_id))}.{physical_no_catalog}"
        replacements.setdefault(base_catalog_ref, physical_with_catalog)
    return translate_epoch_temporal_columns(  # REQ-1908
        _apply_replacements(sql, replacements), ctx.epoch_columns
    )


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


def fold_catalog_into_schema(sql: str) -> str:  # REQ-1730
    """Merge the catalog segment into the schema name: "cat"."schema"."table" -> "cat_schema".
    "table" — for an engine whose SQL dialect cannot express a catalog-qualified reference at all
    (verified live: real PostgreSQL has no cross-database queries, so ``pg`` declares
    ``catalog_qualified=False``, FederationEngine's own doc has the reproduction) but where the
    catalog still carries REAL per-source disambiguating information a plain drop
    (``strip_catalog``) would lose — two different sources whose registered tables happen to share
    a native ``schema_name`` (e.g. two elasticsearch-type sources both reporting "default") would
    otherwise collide once the catalog segment vanished. ``strip_catalog`` stays safe for its own
    callers (the DIRECT route's single live-attached source, where the catalog is genuinely
    redundant, not disambiguating); this is for the ENGINE route on a catalog-INCAPABLE engine,
    where the information must be preserved, just relocated to the one addressable dimension left.
    A table ref with no catalog (already source-less, e.g. a VALUES-CTE relation) is untouched."""
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(sql, read="postgres")
    for tbl in tree.find_all(exp.Table):
        catalog = tbl.args.get("catalog")
        if catalog is None:
            continue
        schema = tbl.args.get("db")
        from provisa.compiler.naming import live_view_schema

        merged = live_view_schema(catalog.name, schema.name) if schema is not None else catalog.name
        tbl.set("catalog", None)
        tbl.set("db", exp.to_identifier(merged, quoted=True))
    return tree.sql(dialect="postgres")


# Source types with no real schema namespace: the DIRECT connection is already scoped to one
# specific physical file/database, so a schema qualifier is purely Provisa's own catalog
# convention and has nothing to resolve against on the wire (REQ-1361).
FLAT_NAMESPACE_SOURCES: frozenset[str] = frozenset({"sqlite"})


def strip_schema(sql: str) -> str:  # REQ-1361
    """Drop the schema segment from every table ref: "schema"."table" → "table".

    Structural, AST-only, mirrors strip_catalog: used to lower a DIRECT-routed mutation/query onto
    a FLAT_NAMESPACE_SOURCES driver (sqlite via the generic SQLAlchemy fallback), whose connection
    is already scoped to the one physical file — the schema segment is unresolvable there.
    """
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(sql, read="postgres")
    for tbl in tree.find_all(exp.Table):
        if tbl.args.get("db") is not None:
            tbl.set("db", None)
    return tree.sql(dialect="postgres")


# --- Literal predicate propagation across equi-joins (REQ-1880) ---


def _is_literal(node: exp.Expr | None, allow_params: bool = False) -> bool:
    """A true constant: a literal, or a negated literal (``-1``) -- never a function/subquery/column.
    With ``allow_params``, a numbered bind parameter (``$1``) too: it is one fixed value for the
    whole execution, so copying it is as safe as copying a literal -- but only on an engine that
    binds parameters BY NUMBER, where a reused ``$1`` still means the same value (a positional
    ``?`` binder would shift every later parameter). The postgres reader parses ``$1`` as a
    Parameter, the duckdb reader as a numbered Placeholder (REQ-899 reads DuckDB SQL); a bare
    ``?`` Placeholder has no number and is never a literal."""
    if allow_params and isinstance(node, exp.Parameter):
        return True
    if allow_params and isinstance(node, exp.Placeholder) and str(node.this or "").isdigit():
        return True
    return isinstance(node, exp.Literal) or (
        isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal)
    )


def _split_and(node: exp.Expr) -> list[exp.Expr]:
    """Top-level AND conjuncts only -- a predicate reached only via an OR is never returned, since
    an OR does not guarantee the predicate holds for every row a propagated copy would rely on."""
    if isinstance(node, exp.Paren):
        return _split_and(node.this)
    if isinstance(node, exp.And):
        return _split_and(node.left) + _split_and(node.right)
    return [node]


def _literal_predicate_target(node: exp.Expr, allow_params: bool = False) -> tuple[str, str] | None:
    """(alias, column) for a WHERE conjunct of the form ``alias.col <op> <literal(s)>`` for
    ``=``/``IN``/``BETWEEN``/``<``/``<=``/``>``/``>=`` -- ``None`` for anything else (a function
    call, a subquery, a second column, an unqualified column). Never guessed: a shape this doesn't
    recognize is simply not propagated, same posture as ``query_residency._join_key_column``. A
    range comparison is as safe to carry across an equality as ``=`` is: ``a = b`` and ``a >= 1``
    imply ``b >= 1`` (the GraphQL compiler emits a range as separate ``>=``/``<=`` conjuncts)."""
    if isinstance(node, (exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        left, right = node.left, node.right
        if isinstance(left, exp.Column) and left.table and _is_literal(right, allow_params):
            return left.table, left.name
        if isinstance(right, exp.Column) and right.table and _is_literal(left, allow_params):
            return right.table, right.name
        return None
    if isinstance(node, exp.In):
        col = node.this
        if not (isinstance(col, exp.Column) and col.table):
            return None
        if node.args.get("query") is not None:  # IN (SELECT ...) is not a literal set
            return None
        exprs = node.args.get("expressions") or []
        if not exprs or not all(_is_literal(e, allow_params) for e in exprs):
            return None
        return col.table, col.name
    if isinstance(node, exp.Between):
        col = node.this
        if not (isinstance(col, exp.Column) and col.table):
            return None
        if _is_literal(node.args.get("low"), allow_params) and _is_literal(
            node.args.get("high"), allow_params
        ):
            return col.table, col.name
        return None
    return None


def _equi_join_columns(join: exp.Join) -> tuple[tuple[str, str], tuple[str, str]] | None:
    """The two ``alias.column`` sides of a genuine INNER equi-join's ON clause, or ``None`` for
    anything else -- a LEFT/RIGHT/FULL OUTER join (``join.side`` set) is excluded categorically:
    that join kind preserves a row from the non-matching side with the far column NULL-extended, so
    pinning the same literal onto the far column would filter those preserved rows out and change
    the result set. An INNER join already drops any row where the two join columns don't match
    (including NULL-vs-NULL, standard SQL join semantics), so propagating changes zero result rows.
    A CROSS/USING/composite-ON/non-equality join is also excluded -- not guessed."""
    if join.side:
        return None
    kind = (join.kind or "").upper()
    if kind not in ("", "INNER"):
        return None
    on = join.args.get("on")
    if not isinstance(on, exp.EQ):
        return None
    left, right = on.left, on.right
    if not (
        isinstance(left, exp.Column)
        and isinstance(right, exp.Column)
        and left.table
        and right.table
    ):
        return None
    return (left.table, left.name), (right.table, right.name)


def _collect_table_aliases(select: exp.Select) -> dict[str, tuple[str, str]]:
    """alias (lowercased) -> (schema, table) lowercased, for this SELECT's own FROM + JOIN tables
    only -- never descends into a nested subquery's own FROM/JOIN (each is its own scope, visited
    separately by the caller's own ``find_all(exp.Select)``)."""
    aliases: dict[str, tuple[str, str]] = {}
    from_ = select.args.get("from_") or select.args.get("from")
    tables: list[exp.Table] = []
    if from_ is not None and isinstance(from_.this, exp.Table):
        tables.append(from_.this)
    for j in select.args.get("joins") or []:
        if isinstance(j.this, exp.Table):
            tables.append(j.this)
    for tbl in tables:
        if tbl.db:
            aliases[tbl.alias_or_name.lower()] = (tbl.db.lower(), tbl.name.lower())
    return aliases


def propagate_literal_join_predicates(  # REQ-1880
    sql: str,
    dialect: str,
    eligible_targets: set[tuple[str, str]],
    column_types: dict[tuple[str, str, str], str] | None = None,
    *,
    allow_params: bool = False,
) -> str:
    """Propagate a literal WHERE predicate across an INNER equi-join onto the far table's own join
    column, so a connector whose pushdown is limited to literal predicates (no join-pushdown --
    e.g. ``PgWrappersMongoDbConnector``, ``predicate_pushdown=True, join_pushdown=False``,
    connector_duckdb.py) gets a chance to push the copy down too (REQ-1880, originating context
    REQ-1871: live-verified, a join-derived predicate never reaches that connector's FDW quals,
    while a literal one directly on its own table does).

    ``eligible_targets`` is the set of lowercased ``(schema, table)`` pairs allowed to receive a
    propagated predicate -- the caller computes this from each join target's OWN connector
    capability before calling in; this function never touches FederationEngine/Connector (leaf
    module rule, see module docstring). ``column_types`` is ``(schema, table, column) -> engine
    type`` for the driving and target sides of a candidate join column; a pair missing from it, or
    incompatible per ``_types_compatible``, is skipped -- conservative by design (a missed
    propagation is always safe; an incorrect one is not), not a swallowed error.

    Only ``=``/``IN (...)``/``BETWEEN`` conjuncts reached by splitting the WHERE clause on
    top-level AND are propagated (never one embedded in an OR, which doesn't hold for every row);
    only when the non-column side(s) are literal constants (a function call, a subquery, or another
    column is left alone). Only a genuine INNER (or unqualified, same thing) equi-join on exactly
    two plain columns is propagated across -- see ``_equi_join_columns`` for why LEFT/RIGHT/FULL
    OUTER must not be.
    """
    if not eligible_targets:
        return sql
    column_types = column_types or {}
    tree = sqlglot.parse_one(sql, read=dialect)
    changed = False
    for select in tree.find_all(exp.Select):
        where = select.args.get("where")
        joins = select.args.get("joins") or []
        if where is None or not joins:
            continue
        alias_to_phys = _collect_table_aliases(select)
        if not alias_to_phys:
            continue
        literal_preds: dict[tuple[str, str], list[exp.Expr]] = {}
        for conjunct in _split_and(where.this):
            target = _literal_predicate_target(conjunct, allow_params)
            if target is not None:
                key = (target[0].lower(), target[1].lower())
                literal_preds.setdefault(key, []).append(conjunct)
        if not literal_preds:
            continue
        new_preds: list[exp.Expr] = []
        for join in joins:
            kc = _equi_join_columns(join)
            if kc is None:
                continue
            (a_alias, a_col), (b_alias, b_col) = kc
            for (drv_alias, drv_col), (tgt_alias, tgt_col) in (
                ((a_alias, a_col), (b_alias, b_col)),
                ((b_alias, b_col), (a_alias, a_col)),
            ):
                preds = literal_preds.get((drv_alias.lower(), drv_col.lower()))
                if not preds:
                    continue
                tgt_phys = alias_to_phys.get(tgt_alias.lower())
                drv_phys = alias_to_phys.get(drv_alias.lower())
                if tgt_phys is None or drv_phys is None or tgt_phys not in eligible_targets:
                    continue
                drv_type = column_types.get((*drv_phys, drv_col.lower()))
                tgt_type = column_types.get((*tgt_phys, tgt_col.lower()))
                if (
                    drv_type is None
                    or tgt_type is None
                    or not _types_compatible(drv_type, tgt_type)
                ):
                    continue
                for pred in preds:
                    propagated = pred.copy()
                    for col in propagated.find_all(exp.Column):
                        if (
                            col.table.lower() == drv_alias.lower()
                            and col.name.lower() == drv_col.lower()
                        ):
                            col.set("table", exp.to_identifier(tgt_alias))
                    new_preds.append(propagated)
        if new_preds:
            combined = where.this
            for pred in new_preds:
                combined = exp.and_(combined, pred, copy=False)
            where.set("this", combined)
            changed = True
    if _propagate_into_correlated_subqueries(tree, eligible_targets, column_types, allow_params):
        changed = True
    return tree.sql(dialect=dialect) if changed else sql


def _propagate_into_correlated_subqueries(
    tree: exp.Expr,
    eligible_targets: set[tuple[str, str]],
    column_types: dict[tuple[str, str, str], str],
    allow_params: bool,
) -> bool:
    """REQ-1880 (amended): the same literal propagation into a CORRELATED scalar subquery in a
    SELECT list -- the shape a GraphQL nested relationship compiles to (``(SELECT ... FROM docs t2
    WHERE t2.order_id = t0.order_id)``), which has no JOIN for the join rule above to see.

    Safe for the same reason as the join rule: a select-list subquery is evaluated only for outer
    rows that already passed the outer WHERE, so an outer literal conjunct on ``t0.k`` plus a
    top-level inner ``t2.k = t0.k`` conjunct pins ``t2.k`` to the same literal set for every
    evaluation -- adding it to the subquery's own WHERE changes zero results. Only top-level AND
    conjuncts on both sides, only plain ``alias.col = alias.col`` correlation, only a subquery
    whose FROM names an eligible target (a connector that pushes literal predicates but not join/
    parameterized paths), only type-compatible columns. Returns whether anything was added."""
    changed = False
    for outer in list(tree.find_all(exp.Select)):
        where = outer.args.get("where")
        if where is None:
            continue
        outer_aliases = _collect_table_aliases(outer)
        if not outer_aliases:
            continue
        literal_preds: dict[tuple[str, str], list[exp.Expr]] = {}
        for conjunct in _split_and(where.this):
            target = _literal_predicate_target(conjunct, allow_params)
            if target is not None and target[0].lower() in outer_aliases:
                literal_preds.setdefault((target[0].lower(), target[1].lower()), []).append(
                    conjunct
                )
        if not literal_preds:
            continue
        for projection in outer.expressions:
            for sub in projection.find_all(exp.Select):
                sub_where = sub.args.get("where")
                if sub_where is None:
                    continue
                sub_aliases = _collect_table_aliases(sub)
                additions: list[exp.Expr] = []
                for conjunct in _split_and(sub_where.this):
                    if not isinstance(conjunct, exp.EQ):
                        continue
                    left, right = conjunct.left, conjunct.right
                    if not (
                        isinstance(left, exp.Column)
                        and isinstance(right, exp.Column)
                        and left.table
                        and right.table
                    ):
                        continue
                    for inner_col, outer_col in ((left, right), (right, left)):
                        inner_alias = inner_col.table.lower()
                        outer_alias = outer_col.table.lower()
                        if inner_alias not in sub_aliases or outer_alias in sub_aliases:
                            continue
                        if outer_alias not in outer_aliases:
                            continue
                        preds = literal_preds.get((outer_alias, outer_col.name.lower()))
                        if not preds:
                            continue
                        tgt_phys = sub_aliases[inner_alias]
                        if tgt_phys not in eligible_targets:
                            continue
                        drv_type = column_types.get(
                            (*outer_aliases[outer_alias], outer_col.name.lower())
                        )
                        tgt_type = column_types.get((*tgt_phys, inner_col.name.lower()))
                        if (
                            drv_type is None
                            or tgt_type is None
                            or not _types_compatible(drv_type, tgt_type)
                        ):
                            continue
                        for pred in preds:
                            propagated = pred.copy()
                            for col in propagated.find_all(exp.Column):
                                if (
                                    col.table.lower() == outer_alias
                                    and col.name.lower() == outer_col.name.lower()
                                ):
                                    # Copy the inner column's own identifier nodes so its
                                    # quoting is preserved.
                                    col.set("table", inner_col.args["table"].copy())
                                    col.set("this", inner_col.args["this"].copy())
                            additions.append(propagated)
                if additions:
                    combined = sub_where.this
                    for pred in additions:
                        combined = exp.and_(combined, pred, copy=False)
                    sub_where.set("this", combined)
                    changed = True
    return changed


# --- ClickHouse LowCardinality(String) decode wrapping (REQ-1881) ---


def is_clickhouse_lowcardinality_string(data_type: str | None) -> bool:
    """True for ClickHouse's ``LowCardinality(String)`` or ``LowCardinality(Nullable(String))``
    (REQ-1881) -- Trino's ClickHouse JDBC connector/driver reports these columns as raw
    dictionary-encoded VARBINARY bytes with zero type-mapping for the LowCardinality wrapper
    (verified live by inspecting the connector's own bytecode: zero "LowCardinality" references);
    ``CAST(col AS varchar)`` does not work either (Trino: "Cannot cast varbinary to varchar" --
    the two types aren't cast-compatible). The only verified decode is ``from_utf8(col)``.

    Whitespace/case-insensitive (ClickHouse's ``system.columns.type`` renders the wrapper text
    verbatim, but exact casing/spacing isn't a documented contract). Anything else --
    ``LowCardinality(UInt64)``, plain ``String``, a bare ``LowCardinality`` with no inner type,
    ``None``/empty -- is not this bug and returns False. Never guessed True: a missed wrap
    surfaces as visible, debuggable garbage bytes; a wrong wrap on an unaffected column risks an
    error or silently corrupting correct data, the worse failure mode.
    """
    if not data_type:
        return False
    normalized = _re.sub(r"\s+", "", data_type.lower())
    return normalized in ("lowcardinality(string)", "lowcardinality(nullable(string))")


def wrap_lowcardinality_columns(  # REQ-1881
    sql: str,
    dialect: str,
    affected_columns: dict[tuple[str, str], set[str]],
) -> str:
    """Wrap every read-context reference to a ClickHouse ``LowCardinality(String)``-family column
    in ``from_utf8(...)`` (REQ-1881), so a query reading such a column through Trino's ClickHouse
    catalog gets decoded text back instead of raw dictionary-encoded bytes -- see
    ``is_clickhouse_lowcardinality_string`` for why. ``affected_columns`` is lowercased
    ``(schema, table) -> {column_name, ...}``; the caller computes this from each referenced
    table's OWN registered source (Trino target + clickhouse-type source, REQ-1881 gating) and
    each column's ``data_type`` before calling in -- this function never touches
    FederationEngine/Connector/Column (leaf module rule, see module docstring).

    Every ``exp.Column`` reference is resolved to its nearest enclosing ``SELECT`` (so a nested
    subquery's own table aliases are never confused with an outer query's), covering the SELECT
    list, WHERE/HAVING comparisons, ORDER BY, GROUP BY, and function arguments alike -- anywhere a
    column appears as a VALUE expression. A column reference with no table qualifier (e.g. an
    output-alias reference in ORDER BY) is left alone: it cannot be resolved to a physical table,
    so wrapping it would be a guess, not a decision this function is allowed to make.

    Idempotent: a reference already wrapped in ``from_utf8(...)`` -- by this pass on a prior run,
    or already written that way in the caller's own SQL -- is left alone, never double-wrapped.
    """
    if not affected_columns:
        return sql
    tree = sqlglot.parse_one(sql, read=dialect)
    changed = False
    alias_cache: dict[int, dict[str, tuple[str, str]]] = {}
    for col in list(tree.find_all(exp.Column)):
        if not col.table:
            continue
        select = col.find_ancestor(exp.Select)
        if select is None:
            continue
        cache_key = id(select)
        alias_to_phys = alias_cache.get(cache_key)
        if alias_to_phys is None:
            alias_to_phys = _collect_table_aliases(select)
            alias_cache[cache_key] = alias_to_phys
        phys = alias_to_phys.get(col.table.lower())
        if phys is None:
            continue
        cols_for_table = affected_columns.get(phys)
        if not cols_for_table or col.name.lower() not in cols_for_table:
            continue
        parent = col.parent
        if (
            isinstance(parent, exp.Anonymous)
            and isinstance(parent.this, str)
            and parent.this.lower() == "from_utf8"
            and len(parent.expressions) == 1
        ):
            continue  # already wrapped -- idempotency
        col.replace(exp.Anonymous(this="from_utf8", expressions=[col.copy()]))
        changed = True
    return tree.sql(dialect=dialect) if changed else sql


# --- Epoch-stored temporal columns (REQ-1908) ---

_EPOCH_SCALE = {"s": None, "ms": exp.UnixToTime.MILLIS, "us": exp.UnixToTime.MICROS}
_EPOCH_POWER = {"s": 0, "ms": 3, "us": 6}
_EPOCH_ORIGIN = datetime(1970, 1, 1, tzinfo=timezone.utc)
_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)

# (schema, table) lowercased -> {column lowercased: (unit, registered data_type)}
EpochColumns = dict[tuple[str, str], dict[str, tuple[str, str]]]


def iso_to_epoch(text: str, unit: str) -> int:
    """An ISO 8601 date/time as an exact epoch count in ``unit``. Epoch storage is a UTC instant by
    definition, so text without an offset is read as UTC — the same zone a read renders it in. A
    value finer than the unit is refused, never truncated: truncating would silently move a bound."""
    # The GraphQL filter compiler (sql_where._timestamp_literal_or_param) renders an ISO operand as
    # TIMESTAMP '<date> <time> UTC' / '<date> <time> +05:30' — its zone after a space.
    canonical = _re.sub(r"\s+UTC$", "+00:00", text.strip())
    canonical = _re.sub(r"\s+([+-]\d{2}:?\d{2})$", r"\1", canonical)
    try:
        moment = datetime.fromisoformat(canonical)
    except ValueError as exc:
        raise ValueError(f"{text!r} is not an ISO 8601 date/time") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    count, rest = divmod(moment - _EPOCH_ORIGIN, EPOCH_UNITS[unit])
    if rest:
        raise ValueError(f"{text!r} is finer than the column's epoch unit {unit!r}")
    return count


def _epoch_read(col: exp.Expr, unit: str, data_type: str) -> exp.Expr:
    """The stored number as the registered temporal type."""
    scale = _EPOCH_SCALE[unit]
    instant = (
        exp.UnixToTime(this=col, scale=scale) if scale is not None else exp.UnixToTime(this=col)
    )
    kind = data_type.lower()
    if kind in ("timestamptz", "timestamp with time zone"):
        return instant
    utc = exp.AtTimeZone(this=instant, zone=exp.Literal.string("UTC"))
    if kind == "date":
        return exp.Cast(this=utc, to=exp.DataType.build("DATE"))
    return utc


def _epoch_operand(value: exp.Expr, unit: str) -> exp.Expr | None:
    """A value compared with (or written to) the stored number, as that number: a string literal
    becomes its exact epoch count now; a written parameter (or any temporal expression) is
    converted in SQL. None when the value is already a number — including one this pass produced."""
    if isinstance(value, exp.Literal):
        return exp.Literal.number(iso_to_epoch(value.this, unit)) if value.is_string else None
    if isinstance(value, exp.Null) or _is_epoch_operand(value):
        return None
    # Built as EXTRACT(EPOCH ...) — the form its rendered SQL parses back to, so a second pass
    # recognizes it. Only writes reach here, and a write always routes DIRECT to the source.
    seconds = exp.Extract(
        this=exp.var("EPOCH"),
        expression=exp.Cast(this=value.copy(), to=exp.DataType.build("TIMESTAMPTZ")),
    )
    power = _EPOCH_POWER[unit]
    if power == 0:
        return seconds
    return exp.Mul(this=seconds, expression=exp.Literal.number(10**power))


def _temporal_type(data_type: str) -> str:
    kind = data_type.lower()
    if kind in ("timestamptz", "timestamp with time zone"):
        return "TIMESTAMPTZ"
    return "DATE" if kind == "date" else "TIMESTAMP"


def _comparison_operands(col: exp.Column, parent: exp.Expr | None) -> list[exp.Expr] | None:
    """The value operands ``col`` is compared with, when ``parent`` is a comparison, ``IN`` or
    ``BETWEEN`` whose other side(s) are all literals or bound values; None otherwise (the column
    is then read as the temporal)."""
    if isinstance(parent, _COMPARISONS):
        operands = [parent.expression if col.arg_key == "this" else parent.this]
    elif isinstance(parent, exp.In) and col.arg_key == "this" and not parent.args.get("query"):
        operands = list(parent.expressions)
    elif isinstance(parent, exp.Between) and col.arg_key == "this":
        operands = [parent.args["low"], parent.args["high"]]
    else:
        return None
    # A cast of a literal or bound value (the GraphQL filter emits CAST('<iso>' AS TIMESTAMP)) is
    # that value: the cast is dropped here and the value converted like a bare one.
    operands = [_uncast_value(o) for o in operands]
    bound = (exp.Literal, exp.Parameter, exp.Placeholder)
    return operands if operands and all(isinstance(o, bound) for o in operands) else None


def _uncast_value(node: exp.Expr) -> exp.Expr:
    if isinstance(node, exp.Cast) and isinstance(
        node.this, (exp.Literal, exp.Parameter, exp.Placeholder)
    ):
        inner = node.this.copy()
        node.replace(inner)
        return inner
    return node


def _is_epoch_operand(node: exp.Expr) -> bool:
    """True for a value ``_epoch_operand`` already converted in SQL (idempotency)."""
    if isinstance(node, exp.Mul):
        node = node.this
    return isinstance(node, exp.Extract) and node.name.lower() == "epoch"


def _scope_tables(scope: exp.Expr) -> dict[str, tuple[str, str]]:
    if isinstance(scope, exp.Select):
        return _collect_table_aliases(scope)
    target = scope.this
    if isinstance(target, exp.Schema):
        target = target.this
    if isinstance(target, exp.Table) and target.db:
        return {target.alias_or_name.lower(): (target.db.lower(), target.name.lower())}
    return {}


def _resolve_epoch_column(
    col: exp.Column, tables: dict[str, tuple[str, str]], epoch_columns: EpochColumns
) -> tuple[str, str] | None:
    name = col.name.lower()
    if col.table:
        phys = tables.get(col.table.lower())
        return (epoch_columns.get(phys) or {}).get(name) if phys else None
    # Unqualified: SQL binds it to the one in-scope table that has it.
    owners = [
        epoch_columns[phys][name]
        for phys in set(tables.values())
        if name in epoch_columns.get(phys, {})
    ]
    return owners[0] if len(owners) == 1 else None


def _translate_writes(stmt: exp.Expr, epoch_columns: EpochColumns) -> None:
    """A written ISO 8601 value is stored as the column's epoch number."""
    tables = _scope_tables(stmt)
    if not tables:
        return
    cols = epoch_columns.get(next(iter(tables.values())), {})
    if isinstance(stmt, exp.Update):
        for assign in stmt.expressions:
            spec = cols.get(assign.this.name.lower()) if isinstance(assign, exp.EQ) else None
            if spec:
                converted = _epoch_operand(assign.expression, spec[0])
                if converted is not None:
                    assign.set("expression", converted)
        return
    schema = stmt.this
    values = stmt.expression
    if not isinstance(schema, exp.Schema) or not isinstance(values, exp.Values):
        return
    positions = {
        i: cols[ident.name.lower()]
        for i, ident in enumerate(schema.expressions)
        if ident.name.lower() in cols
    }
    for row in values.expressions:
        for i, spec in positions.items():
            converted = _epoch_operand(row.expressions[i], spec[0])
            if converted is not None:
                row.expressions[i].replace(converted)


def translate_epoch_temporal_columns(  # REQ-1908
    sql: str, epoch_columns: EpochColumns
) -> str:
    """Translate every reference to an epoch-stored temporal column (REQ-1908) so the statement
    sees the registered temporal type — on every surface, since both physical rewrites call this.

    * A read yields the temporal (``TO_TIMESTAMP`` & co., transpiled per engine later), aliased to
      the column's own name in a select list so the output column keeps its name.
    * A comparison (``=``/``<>``/``<``/``<=``/``>``/``>=``, ``IN``, ``BETWEEN``) against ISO 8601
      text or a parameter converts the OPERAND to the epoch number and leaves the column bare, so a
      source index on the stored number still applies.
    * ``ORDER BY``/``GROUP BY``/``IS NULL`` keep the bare column: the translation is monotonic and
      one-to-one, so the result is the same without converting every row.
    * An ``INSERT``/``UPDATE`` stores an ISO 8601 value as its epoch number, and its ``RETURNING``
      reads the temporal back under the column's own name.

    Idempotent — every call site may see already-translated SQL. Parses only when an affected table
    is referenced, so every other statement's text is returned unchanged."""
    if not epoch_columns:
        return sql
    lowered = sql.lower()
    if not any(table in lowered for (_schema, table) in epoch_columns):
        return sql
    tree = sqlglot.parse_one(sql, read="postgres")
    if not any(
        (t.db.lower(), t.name.lower()) in epoch_columns for t in tree.find_all(exp.Table) if t.db
    ):
        return sql
    for stmt in tree.find_all(exp.Update, exp.Insert):
        _translate_writes(stmt, epoch_columns)
    scopes: dict[int, dict[str, tuple[str, str]]] = {}
    for col in list(tree.find_all(exp.Column)):
        if isinstance(col.this, exp.Star):
            continue
        scope = col.find_ancestor(exp.Select, exp.Update, exp.Delete, exp.Insert)
        if scope is None:
            continue
        if isinstance(scope, exp.Insert) and col.find_ancestor(exp.Returning) is None:
            continue  # an INSERT's own target list / ON CONFLICT: written, not read
        if (
            isinstance(scope, exp.Update)
            and col.parent in scope.expressions
            and col.arg_key == "this"
        ):
            continue  # an assignment target, not a value
        tables = scopes.setdefault(id(scope), _scope_tables(scope))
        spec = _resolve_epoch_column(col, tables, epoch_columns)
        if spec is None:
            continue
        unit, data_type = spec
        parent = col.parent
        # Already read as the temporal — as built, or as its rendered SQL parses back.
        converted_read = col.find_ancestor(
            exp.UnixToTime, exp.Select, exp.Update, exp.Delete, exp.Insert
        )
        if isinstance(converted_read, exp.UnixToTime) or col.find_ancestor(exp.Order, exp.Group):
            continue
        if isinstance(parent, exp.Is):
            continue
        operands = _comparison_operands(col, parent)
        if operands is not None:
            if all(isinstance(o, exp.Literal) for o in operands):
                # ISO 8601 text becomes its exact epoch number at compile time; the column stays
                # bare, so a source index on the stored number applies. A number is left as is.
                for o in operands:
                    converted = _epoch_operand(o, unit)
                    if converted is not None:
                        o.replace(converted)
                continue
            # A bound value is typed as the registered temporal and compared with the column read
            # as that temporal. Converting the value to a number in SQL instead would not survive
            # the per-engine transpile (EXTRACT(EPOCH ...) drops the offset on Trino).
            temporal = _temporal_type(data_type)
            for o in operands:
                if isinstance(o, (exp.Parameter, exp.Placeholder)):
                    o.replace(exp.Cast(this=o.copy(), to=exp.DataType.build(temporal)))
        read = _epoch_read(col.copy(), unit, data_type)
        if isinstance(parent, (exp.Select, exp.Returning)):
            col.replace(exp.alias_(read, col.name))
        else:
            col.replace(read)
    return tree.sql(dialect="postgres")


# --- Main compilation ---
