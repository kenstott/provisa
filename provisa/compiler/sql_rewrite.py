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

import re as _re

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_types import CompilationContext, TableMeta


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
            alias=exp.TableAlias(this=exp.Identifier(this=alias_q, quoted=True)),
        )
        return new_tbl

    tree = tree.transform(_rewrite)

    for col in tree.find_all(exp.Column):
        tbl_id = col.args.get("table")
        if isinstance(tbl_id, exp.Identifier) and tbl_id.this in quoted_aliases:
            tbl_id.set("quoted", True)

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
    # normalize_table_refs FIRST — the same order the ENGINE lowering uses. It pins an unaliased
    # ref's own name as an explicit alias, which is what the query's column qualifiers bind to
    # ("query_audit_log"."id" against `FROM "ops"."query_audit_log"`). Substituting first renamed
    # the ref to its physical name before that alias existed, so normalize then aliased it to the
    # PHYSICAL name and the qualifiers were stranded — "missing FROM-clause entry for table
    # query_audit_log" on every ops/meta table whose physical name differs from its semantic one.
    sql = _apply_replacements(normalize_table_refs(sql, ctx), replacements)
    # DIRECT route: a native driver addresses schema.table, not catalog.schema.table — strip the
    # catalog normalize_table_refs re-attaches for already-qualified refs (REQ-641/REQ-863).
    return strip_catalog(sql)


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
        merged = f"{catalog.name}_{schema.name}" if schema is not None else catalog.name
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


def _is_literal(node: exp.Expr | None) -> bool:
    """A true constant: a literal, or a negated literal (``-1``) -- never a function/subquery/column."""
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


def _literal_predicate_target(node: exp.Expr) -> tuple[str, str] | None:
    """(alias, column) for a WHERE conjunct of the form ``alias.col <op> <literal(s)>`` for
    ``=``/``IN``/``BETWEEN`` -- ``None`` for anything else (a function call, a subquery, a second
    column, an unqualified column). Never guessed: a shape this doesn't recognize is simply not
    propagated, same posture as ``query_residency._join_key_column``."""
    if isinstance(node, exp.EQ):
        left, right = node.left, node.right
        if isinstance(left, exp.Column) and left.table and _is_literal(right):
            return left.table, left.name
        if isinstance(right, exp.Column) and right.table and _is_literal(left):
            return right.table, right.name
        return None
    if isinstance(node, exp.In):
        col = node.this
        if not (isinstance(col, exp.Column) and col.table):
            return None
        if node.args.get("query") is not None:  # IN (SELECT ...) is not a literal set
            return None
        exprs = node.args.get("expressions") or []
        if not exprs or not all(_is_literal(e) for e in exprs):
            return None
        return col.table, col.name
    if isinstance(node, exp.Between):
        col = node.this
        if not (isinstance(col, exp.Column) and col.table):
            return None
        if _is_literal(node.args.get("low")) and _is_literal(node.args.get("high")):
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
            target = _literal_predicate_target(conjunct)
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
    return tree.sql(dialect=dialect) if changed else sql


# --- Main compilation ---
