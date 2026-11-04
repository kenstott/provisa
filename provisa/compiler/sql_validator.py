# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1b2c4-d5e6-7890-abcd-ef1234567890
# (run scripts/canary_stamp.py on this file after creating it)

"""SQL validator: enforce GraphQL-equivalent access rules on raw SQL.

Rules:
  V001 – FROM-clause table's domain must be in role's domain_access (or domain_access empty).
  V002 – Every JOIN ON condition must match an approved relationship (source_col = target_col).
          Exception: JOINs to 'meta' or 'ops' domain tables are implicitly authorized (traversal only).
  V003 – Referenced columns must be visible to this role.
  V004 – (retired) Cyclic join graphs are pruned in-place; back-edges are dropped silently.
  V005 – Masked columns must not appear in WHERE or HAVING clauses (prevents plaintext inference).
  V006 – Every relation must be a registered table (or a CTE, or a pure row generator), for every
          role including domain_access ["*"]: a table function or unregistered name reaches the
          engine's internals or its host filesystem, not governed data.

Security model / layer responsibilities:
  Layer 0 – Introspection filtering (schema_gen): the GraphQL schema, SQL catalog, and
            column list exposed to a role contain only tables in its domain_access and
            columns passing visible_to. Inaccessible objects are invisible at discovery
            time — they cannot be queried, autocompleted, or inferred to exist.
  Layer 1 – Public access: all identities see data in domains with no access restriction.
  Layer 2 – Domain access (V001): gates which tables a role may query.
  Layer 3 – Row-level security (RLS): injected WHERE predicates (stage2) restrict which
            rows are visible within an allowed domain, regardless of SQL structure.
  Layer 4 – Column visibility (V003) and masking: restricts which columns are visible and
            replaces sensitive values with masked output in query results.
  Layer 5 – Predicate guard (V005): blocks masked columns from WHERE/HAVING to prevent
            plaintext inference against masked output.

  V002 is governance policy, not a hard security boundary. It marks approved traversal
  paths between tables a role already has access to. A role cannot use an unapproved join
  to reach data outside its Layer 2 domain grant or past Layers 3–5 content controls — so
  circumventing V002 does not expose data the role couldn't obtain through two separate
  queries. V002 exists to enforce allowed roads and provide an audit surface for
  deliberate circumvention, not to be the last line of defence.
"""

# Requirements: REQ-001, REQ-002, REQ-038, REQ-039, REQ-040, REQ-041, REQ-263, REQ-264, REQ-265, REQ-266

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from provisa.compiler.sql_where import SERVED_WITH_EVERY_TABLE
from provisa.compiler import temp_tables
from provisa.compiler.cte_utils import cte_names, names_a_cte_in_scope
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import GovernanceContext, approved_join_key, registered_name
from provisa.compiler.view_expand import marked_view
from provisa.security.rights import (
    TRAVERSABLE_DOMAINS,
    listed_only_message,
    missing_capability_message,
    reaches_domain,
)
from provisa.transpiler.xml_syntax import MAPPINGS


# The functions sqlglot's PostgreSQL reader takes for unnest under other names: it reads
# ``explode(a)`` and ``pg_catalog.explode(a)`` as it reads ``pg_catalog.unnest(a)``, an Explode.
# PostgreSQL has no such functions, so a user's schema may.
_EXPLODE_NAMES = frozenset({"EXPLODE", "EXPLODE_OUTER", "POSEXPLODE", "POSEXPLODE_OUTER"})


class PostgresAsWritten(Postgres):
    """PostgreSQL as the validator reads it (REQ-039, REQ-266): a function is read by the name it
    is written with -- ``explode`` is an unknown function, never PostgreSQL's unnest."""

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            name: build
            for name, build in Postgres.Parser.FUNCTIONS.items()
            if name not in _EXPLODE_NAMES
        }


#: The dialect a statement is parsed in for validation (:func:`validate_sql`).
READ = PostgresAsWritten


@dataclass
class ValidationViolation:
    code: str
    message: str
    #: V002: the two registered tables the statement combines outside a relationship.
    tables: tuple[int, int] | None = None


class GovernanceRefused(PermissionError):
    """A statement governance refuses, with the violations it is refused for: a PermissionError
    as before, whose violations a caller may read -- the NL retry gives the registered path between
    the tables a V002 names back to the model (REQ-603)."""

    def __init__(self, violations: list[ValidationViolation]) -> None:
        super().__init__("; ".join(f"[{v.code}] {v.message}" for v in violations))
        self.violations = violations


def validate_sql(  # REQ-001, REQ-002, REQ-038, REQ-266
    sql: str,
    ctx: CompilationContext,
    gov_ctx: GovernanceContext,
    role: dict,
    raw_tables: list[dict],
    *,
    bypass_relationship_guard: bool = False,
    bypass_uncovered_relationships: bool = False,
    table_functions: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Validate SQL against role-scoped GraphQL-equivalent access rules.

    REQ-039: the statement is judged twice, as written and with its identifiers folded as
    PostgreSQL folds them (unquoted in lower case, quoted exactly), and refused for what either
    reading refuses. The folded reading is what the engine executes; the written one is what stage
    2 matches its rules against, name for name. A spelling only one of them resolves -- ``C.ssn``
    over ``FROM bank.customer c`` -- is refused, so no column reaches the engine past either."""
    try:
        written = sqlglot.parse_one(sql, read=READ)
    except Exception as exc:
        return [ValidationViolation("V000", f"SQL parse error: {exc}")]
    folded = normalize_identifiers(written.copy(), dialect="postgres")

    seen: set[tuple[str, str, tuple[int, int] | None]] = set()
    violations: list[ValidationViolation] = []
    for tree in (written, folded):
        for v in _violations(
            tree,
            ctx,
            gov_ctx,
            role,
            raw_tables,
            bypass_relationship_guard=bypass_relationship_guard,
            bypass_uncovered_relationships=bypass_uncovered_relationships,
            table_functions=table_functions,
        ):
            key = (v.code, v.message, v.tables)
            if key not in seen:
                seen.add(key)
                violations.append(v)
    return violations


def _violations(
    tree: exp.Expr,
    ctx: CompilationContext,
    gov_ctx: GovernanceContext,
    role: dict,
    raw_tables: list[dict],
    *,
    bypass_relationship_guard: bool,
    bypass_uncovered_relationships: bool,
    table_functions: frozenset[str],
) -> list[ValidationViolation]:
    """Every rule's violations in one reading of the statement (:func:`validate_sql`)."""
    violations: list[ValidationViolation] = []
    cte_names_set = cte_names(tree)
    violations += _check_registered_relations(tree, gov_ctx, ctx, cte_names_set, table_functions)

    # Build reverse maps
    table_id_to_meta: dict[int, TableMeta] = {}
    for meta in ctx.tables.values():
        table_id_to_meta[meta.table_id] = meta

    domain_access: list[str] = role["domain_access"]

    violations += _check_domain_access(
        tree, gov_ctx, table_id_to_meta, domain_access, cte_names_set
    )
    if not bypass_relationship_guard:
        valid_joins = approved_joins(ctx)

        # REQ-603: however the statement relates its tables -- a join of any spelling, a CTE, a
        # derived table, a subquery -- every pairing of their columns is a registered
        # relationship.
        computed, constants = computed_joins(ctx)
        violations += tables_outside_relationships(
            tree,
            gov_ctx,
            valid_joins,
            table_id_to_meta,
            bypass_uncovered=bypass_uncovered_relationships,
            computed=computed,
            constants=constants,
            primary_keys=registered_primary_keys(raw_tables),
        )
    violations += _check_column_visibility(tree, gov_ctx, cte_names_set)
    violations += _check_dag(tree, gov_ctx, cte_names_set)
    violations += _check_masked_in_predicate(tree, gov_ctx, cte_names_set)
    violations += _check_masked_in_xmltable(tree, gov_ctx, cte_names_set)
    violations += _check_xml_mappings(tree)

    return violations


# --------------------------------------------------------------------------- #
# V006 – Registered relations only                                             #
# --------------------------------------------------------------------------- #

# Table functions that only generate rows from their arguments — no engine state, no file or
# network read. XMLTABLE reads the document it is passed, and a document cannot reach outside
# itself (provisa.federation.duckdb_pg_xml); what it is passed is the statement's own, governed as
# the statement is (_check_masked_in_xmltable).
_ROW_GENERATORS = (exp.GenerateSeries, exp.ExplodingGenerateSeries, exp.XMLTable)


def client_table_function(tbl: exp.Table, table_functions: Collection[str]) -> bool:
    """Whether FROM item ``tbl`` reads one of ``table_functions``: PostgreSQL's own, unqualified
    or in pg_catalog, never a function of that name in another schema or catalog (a user's
    ``evil.unnest``). A schema-qualified ``pg_catalog.unnest(...)`` is read by sqlglot as an
    Explode, not by its name: it is PostgreSQL's unnest all the same (REQ-2204). Only that Explode
    -- in :data:`READ`, ``explode`` and its kin are read by their own names, so an Explode was
    written ``unnest``; a subclass (``Posexplode``) is another function."""
    if tbl.catalog or tbl.db not in ("", "pg_catalog"):
        return False
    target = tbl.this
    if isinstance(target, exp.Anonymous):
        return target.name.lower() in table_functions
    return type(target) is exp.Explode and "unnest" in table_functions


def _check_registered_relations(  # REQ-001, REQ-266
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    ctx: CompilationContext,
    cte_names_set: frozenset[str] = frozenset(),
    table_functions: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Every relation the statement reads is a registered table, a CTE, or a row generator.

    Checked for EVERY role: ``domain_access: ["*"]`` widens which DOMAINS a role reads, never
    what counts as governed data. Anything else is engine internals or the engine host —
    ``duckdb_secrets()`` prints http-secret headers (a ClickHouse X-ClickHouse-Key) in clear text,
    ``duckdb_databases()`` attached DSNs, ``read_csv('/etc/passwd')`` and a quoted
    ``"/path/x.csv"`` (DuckDB's replacement scan) read host files."""
    violations: list[ValidationViolation] = []
    for tbl in tree.find_all(exp.Table):
        target = tbl.this
        if isinstance(target, _ROW_GENERATORS):
            continue
        if client_table_function(tbl, table_functions):
            continue  # REQ-2190: a statement of the catalog, on the role that runs client SQL
        if not isinstance(target, exp.Identifier):
            fn = target.sql(dialect="postgres").split("(", 1)[0] if target is not None else "?"
            violations.append(
                ValidationViolation("V006", f"Table function {fn!r} is not a governed relation")
            )
            continue
        if not tbl.db and (tbl.name in cte_names_set or names_a_cte_in_scope(tbl)):
            continue  # a CTE, the statement's own or an embedded statement's (EXISTS, COLLECT)
        if not tbl.db and tbl.name in temp_tables.names():
            continue  # REQ-615: the session's own temporary table, no model object
        said = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
        listed = ctx.listed_only.get(said)
        if listed is not None:
            # REQ-1132: list is metadata only, and said so.
            violations.append(
                ValidationViolation("V001", listed_only_message(ctx.role_id or "", listed))
            )
            continue
        # A registered table is named by its semantic/physical name (table_map) or, unqualified, by
        # its field name — the domain-prefixed ``sa__orders`` form a domain_prefix schema exposes.
        field_named = not tbl.db and tbl.name in ctx.tables
        if _resolve_table_id(tbl, gov_ctx) is None and not field_named:
            ref = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
            draft = ctx.draft_names.get(ref)
            if draft is not None:
                # REQ-1921: out of service, and said so — not taken for a relation that is not
                # registered.
                from provisa.compiler.definitions import DRAFT_REASON

                violations.append(ValidationViolation("V020", f"{draft!r} {DRAFT_REASON}"))
                continue
            system_domain = ctx.missing_capability.get(ref)
            if system_domain is not None:
                # REQ-1132: in no schema of this role for want of a capability, and said so.
                violations.append(
                    ValidationViolation(
                        "V001", missing_capability_message(ctx.role_id or "", system_domain)
                    )
                )
                continue
            violations.append(
                ValidationViolation("V006", f"Relation {ref!r} is not a registered table")
            )
    return violations


# --------------------------------------------------------------------------- #
# V001 – Domain access                                                         #
# --------------------------------------------------------------------------- #


def _from_tables(
    select: exp.Select, cte_names_set: frozenset[str] = frozenset()
) -> list[exp.Table]:
    """Return tables directly in FROM (not in JOINs, not in subqueries, not CTE aliases)."""
    from_clause = select.args.get("from_") or select.args.get("from")
    if not from_clause:
        return []
    results = []
    for tbl in from_clause.find_all(exp.Table):
        if not _inside_subquery(tbl, select) and tbl.name not in cte_names_set:
            results.append(tbl)
    return results


def _inside_subquery(node: exp.Expr, stop: exp.Expr) -> bool:
    cur = node.parent
    while cur is not None and cur is not stop:
        if isinstance(cur, exp.Subquery):
            return True
        cur = cur.parent
    return False


def _resolve_table_id(tbl: exp.Table, gov_ctx: GovernanceContext) -> int | None:
    from provisa.compiler.naming import table_id_of

    return table_id_of(tbl, gov_ctx.table_map)


def _check_domain_access(  # REQ-039, REQ-263
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    table_id_to_meta: dict[int, TableMeta],
    domain_access: list[str],
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    # A role reaches the domains it lists; "*" is every data domain, a system domain is reached by
    # its capability; an EMPTY list is none, so every table with a domain is a V001 for it
    # (rights.reaches_domain decides, single-domain mode included).
    violations = []
    for select in tree.find_all(exp.Select):
        for tbl in _from_tables(select, cte_names_set):
            tid = _resolve_table_id(tbl, gov_ctx)
            if tid is None:
                continue
            meta = table_id_to_meta.get(tid)
            if meta is None:
                continue
            # REQ-1959: a table outside the role's domains that publishes a column to it is one
            # the role is served; reading it directly is not a domain violation. Which of its
            # columns the role may read is the column check's (V003).
            if tid in gov_ctx.public_tables:
                continue
            if meta.domain_id and not reaches_domain(domain_access, meta.domain_id):
                ref = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
                violations.append(
                    ValidationViolation(
                        "V001",
                        f"Table {ref!r} belongs to domain {meta.domain_id!r} which is not in role's domain_access",
                    )
                )
    return violations


# --------------------------------------------------------------------------- #
# V002 – Join relationship validation                                          #
# --------------------------------------------------------------------------- #


def _extract_eq_pairs(on_expr: exp.Expr) -> list[tuple[str, str, str, str]]:
    """Extract (left_table, left_col, right_table, right_col) from EQ conditions in ON clause."""
    pairs = []
    for eq in on_expr.find_all(exp.EQ):
        left = eq.left
        right = eq.right
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            pairs.append(
                (
                    left.table or "",
                    left.name,
                    right.table or "",
                    right.name,
                )
            )
    return pairs


def _alias_map(
    select: exp.Select,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> dict[str, int]:
    """Map table alias (or name) → table_id for all physical tables in this SELECT."""
    result: dict[str, int] = {}
    from_clause = select.args.get("from_") or select.args.get("from")
    all_tables: list[exp.Table] = []
    if from_clause:
        all_tables += [
            t
            for t in from_clause.find_all(exp.Table)
            if not _inside_subquery(t, select) and t.name not in cte_names_set
        ]
    for join in select.args.get("joins") or []:
        all_tables += [
            t
            for t in join.find_all(exp.Table)
            if not _inside_subquery(t, select) and t.name not in cte_names_set
        ]
    for tbl in all_tables:
        tid = _resolve_table_id(tbl, gov_ctx)
        if tid is not None:
            alias = tbl.alias or tbl.name
            result[alias] = tid
    return result


_REMOTE_SOURCE_TYPES: frozenset[str] = frozenset({"graphql_remote", "grpc_remote"})


def approved_joins(ctx: CompilationContext) -> set[tuple[int, int, str, str]]:
    """The joins the registered relationships approve: (table, table, column, column), each in
    both directions, a junction-backed relationship as its two hops (REQ-1586)."""
    type_to_meta = {meta.type_name: meta for meta in ctx.tables.values()}
    approved: set[tuple[int, int, str, str]] = set()
    for (type_name, _), jm in ctx.joins.items():
        src = type_to_meta.get(type_name)
        if not src:
            continue
        if (
            jm.source_constant is not None
            or jm.source_expr is not None
            or jm.target_expr is not None
            or jm.source_json_key is not None
        ):
            # Registered with a computed edge: its own condition is what is approved
            # (computed_joins), not an equality of the two columns it is computed from.
            continue
        approved.add((src.table_id, jm.target.table_id, jm.source_column, jm.target_column))
        approved.add((jm.target.table_id, src.table_id, jm.target_column, jm.source_column))
        via = jm.via
        if via is not None and len(via.source_columns) == 1 and len(via.target_columns) == 1:
            via_id = via.table.table_id
            v_src_col = via.source_columns[0]
            v_tgt_col = via.target_columns[0]
            approved.add((src.table_id, via_id, jm.source_column, v_src_col))
            approved.add((via_id, src.table_id, v_src_col, jm.source_column))
            approved.add((via_id, jm.target.table_id, v_tgt_col, jm.target_column))
            approved.add((jm.target.table_id, via_id, jm.target_column, v_tgt_col))
    return approved


def canvas_joins(ctx: CompilationContext) -> list[tuple[int, str, int, str]]:
    """The joins a join canvas draws (the SQL page's): :func:`approved_joins`, each pair of
    columns once as (table, column, table, column) with the lower (table, column) first, less a
    column equal to itself -- a junction relationship's own end-to-end pair, which joins nothing;
    its two foreign-key joins are its lines."""
    out: set[tuple[int, str, int, str]] = set()
    for a_table, b_table, a_col, b_col in approved_joins(ctx):
        if (a_table, a_col) == (b_table, b_col):
            continue
        out.add(min((a_table, a_col, b_table, b_col), (b_table, b_col, a_table, a_col)))
    return sorted(out)


def computed_joins(
    ctx: CompilationContext,
) -> tuple[set[tuple[int, int, str, str]], set[tuple[int, int, str, str]]]:
    """The registered relationships whose edge is computed, as the guard compares a statement's
    condition with them (REQ-603): those with an expression or a JSON key on a side, as (table,
    table, side's form, side's form) in both directions; and those whose source side is a
    constant, as (source table, target table, the constant, the target side's form). A plain
    column side is written in the same form, so a condition is compared whole."""
    from sqlglot import exp as _exp

    from provisa.compiler.join_guard import registered_form

    type_to_meta = {meta.type_name: meta for meta in ctx.tables.values()}
    computed: set[tuple[int, int, str, str]] = set()
    constants: set[tuple[int, int, str, str]] = set()
    for (type_name, _), jm in ctx.joins.items():
        src = type_to_meta.get(type_name)
        if not src:
            continue
        target_side = registered_form(jm.target_expr or f'{{alias}}."{jm.target_column}"')
        if jm.source_constant is not None:
            literal = (
                _exp.Literal.string(jm.source_constant)
                if isinstance(jm.source_constant, str)
                else _exp.Literal.number(jm.source_constant)
            )
            constants.add(
                (src.table_id, jm.target.table_id, literal.sql(dialect="postgres"), target_side)
            )
            continue
        if jm.source_expr is not None:
            source_side = registered_form(jm.source_expr)
        elif jm.source_json_key is not None:
            source_side = registered_form(
                f"JSON_EXTRACT_SCALAR({{alias}}.\"{jm.source_column}\", '$.{jm.source_json_key}')"
            )
        elif jm.target_expr is not None:
            source_side = registered_form(f'{{alias}}."{jm.source_column}"')
        else:
            continue  # a plain column pair: approved_joins
        computed.add((src.table_id, jm.target.table_id, source_side, target_side))
        computed.add((jm.target.table_id, src.table_id, target_side, source_side))
    return computed, constants


def registered_primary_keys(raw_tables: list[dict]) -> dict[int, frozenset[str]]:
    """Each registered table's whole primary key, as registered -- not as a role sees it: a key
    the role sees part of is still the whole key (REQ-603). A table with none has no entry."""
    keys: dict[int, frozenset[str]] = {}
    for table in raw_tables:
        key = frozenset(c["column_name"] for c in table["columns"] if c.get("is_primary_key"))
        if key:
            keys[table["id"]] = key
    return keys


def tables_outside_relationships(  # REQ-603
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    valid_joins: set[tuple[int, int, str, str]],
    table_id_to_meta: dict[int, TableMeta],
    *,
    bypass_uncovered: bool = False,
    computed: set[tuple[int, int, str, str]] = frozenset(),  # type: ignore[assignment]
    constants: set[tuple[int, int, str, str]] = frozenset(),  # type: ignore[assignment]
    primary_keys: dict[int, frozenset[str]] | None = None,
) -> list[ValidationViolation]:
    """V002 for each pair of registered tables ``tree`` combines outside the registered
    relationships, however the statement is written (provisa.compiler.join_guard)."""
    from provisa.compiler.join_guard import (
        NEVER_TRUE,
        NOT_EQUALITY,
        UNREGISTERED,
        unrelated_tables,
    )

    covered_pairs = {(s, t) for s, t, _, _ in valid_joins}

    def exempt(table_id: int) -> bool:
        # the system domains are implicitly traversable — no registered relationship required
        meta = table_id_to_meta.get(table_id)
        return meta is not None and meta.domain_id in TRAVERSABLE_DOMAINS

    def same_remote_source(a: int, b: int) -> bool:
        # Remote schemas own their relationship model: two tables of one remote source with no
        # relationship registered between them here (see _check_join_relationships).
        if not bypass_uncovered or (a, b) in covered_pairs:
            return False
        left, right = table_id_to_meta.get(a), table_id_to_meta.get(b)
        return (
            left is not None
            and right is not None
            and left.source_type in _REMOTE_SOURCE_TYPES
            and right.source_type in _REMOTE_SOURCE_TYPES
            and left.source_id == right.source_id
        )

    def name(table_id: int) -> str:
        meta = table_id_to_meta.get(table_id)
        return meta.table_name if meta is not None else str(table_id)

    violations = []
    for pair in unrelated_tables(
        tree,
        resolve_table_id=lambda tbl: _resolve_table_id(tbl, gov_ctx),
        columns_of=lambda table_id: {c for c, _ in gov_ctx.all_columns.get(table_id, [])},
        registered=valid_joins,
        exempt_table=exempt,
        same_remote_source=same_remote_source,
        computed=computed,
        constants=constants,
        primary_keys=primary_keys,
    ):
        left, right = name(pair.left_table), name(pair.right_table)
        if pair.reason == UNREGISTERED and pair.written is not None:
            # A side read through a derived table or a CTE: the condition as the statement writes
            # it, and what that side reads -- the base columns alone name neither side as written.
            reads = [
                f"{said_as} reads {table}.{column}"
                for said_as, table, column in (
                    (pair.left_written, left, pair.left_column),
                    (pair.right_written, right, pair.right_column),
                )
                if said_as is not None
            ]
            said = (
                f"Invalid JOIN: {pair.written} ({'; '.join(reads)}) — no approved relationship "
                f"exists between {left}.{pair.left_column} and {right}.{pair.right_column}"
            )
        elif pair.reason == UNREGISTERED:
            said = (
                f"Invalid JOIN: {left}.{pair.left_column} = {right}.{pair.right_column} — no "
                f"approved relationship exists between these tables on these columns"
            )
        elif pair.reason == NEVER_TRUE:
            said = (
                f"Invalid JOIN: no approved relationship of the type named connects {left} and "
                f"{right}"
            )
        elif pair.reason == NOT_EQUALITY:
            said = (
                f"Invalid JOIN: {left} and {right} are matched by something other than an "
                f"equality of their columns — tables are related only along an approved "
                f"relationship, on its columns"
            )
        else:
            said = (
                f"Invalid JOIN: {left} and {right} are combined with no condition relating them "
                f"— cross joins are not permitted"
            )
        violations.append(
            ValidationViolation("V002", said, tables=(pair.left_table, pair.right_table))
        )
    return violations


# --------------------------------------------------------------------------- #
# V003 – Column visibility                                                     #
# --------------------------------------------------------------------------- #


def _check_column_visibility(  # REQ-039, REQ-040, REQ-263, REQ-265
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """V003: every column the role's SQL names, in every clause, judged where it resolves (REQ-039,
    amended 2026-11-04: A COLUMN IS JUDGED WHERE IT RESOLVES; A HIDDEN COLUMN IS REFUSED IN EVERY
    CLAUSE). The select list, WHERE, JOIN ON, USING and a NATURAL join's shared columns, GROUP BY,
    HAVING, ORDER BY, windows, FROM-function arguments and LATERAL items, in any subquery, CTE or
    set operation: no clause is an oracle on a value the role is not served. A view's SQL is its
    administrator's, as a row rule is, but for its select lists, which the role reads."""
    violations = []
    scopes = _relation_names_by_scope(tree)
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for expr in select.expressions:
            if not _is_star(expr):
                continue
            if isinstance(expr, exp.Column) and _names_no_relation(expr, scopes):
                violations.append(_no_relation_violation(expr))
                continue
            # A star's columns are the ones the role is served (stage2._expand_star); it is
            # refused only when that is none of them. The columns a USING or NATURAL join merges
            # are judged with the join (_merged_column_violations).
            star = _star_violation(expr, am, gov_ctx)
            if star is not None:
                violations.append(star)
        if _the_roles(select):
            violations += _merged_column_violations(select, gov_ctx, cte_names_set)
    for col in tree.find_all(exp.Column):
        if _is_star(col) and isinstance(col.parent, exp.Select):
            continue  # a select list's ``t.*``, judged above; a ``t.*`` in an expression is a row
        if not _the_roles(col) or _names_an_output(col, gov_ctx, cte_names_set):
            continue
        if _names_no_relation(col, scopes):
            violations.append(_no_relation_violation(col))
            continue
        if col.name in SERVED_WITH_EVERY_TABLE:
            # A virtual column (a table's _domain_/_name_, registered_tables' table_query_name) is
            # served with the table to every role that reads it, as GraphQL serves it; the
            # lowering answers it (sql_rewrite).
            continue
        if approved_join_key(col, gov_ctx):
            # A registered relationship's join key in a condition (a relationship field): read by
            # the engine to join, never returned (REQ-603).
            continue
        violation = _column_violation(col, gov_ctx, cte_names_set)
        if violation is not None:
            violations.append(violation)
    return violations


def _the_roles(node: exp.Expr) -> bool:
    """Whether ``node`` is the role's SQL, judged in every clause. A view's SQL, expanded where
    governance validates the statement (``view_expand.VIEW_MARK``), is its administrator's: only
    its select lists are judged, as they always were -- what the role reads of it."""
    in_list = False
    cur = node
    while cur.parent is not None:
        parent = cur.parent
        if isinstance(parent, exp.Select) and cur.arg_key == "expressions":
            in_list = True
        if isinstance(parent, exp.Subquery) and marked_view(parent.alias):
            return in_list
        cur = parent
    return True


def _names_an_output(
    col: exp.Column, gov_ctx: GovernanceContext, cte_names_set: frozenset[str]
) -> bool:
    """Whether ``col`` is a bare name PostgreSQL reads as an output column of its query, not as a
    column of a relation: an ORDER BY item of a set operation, an ORDER BY item its SELECT lists
    an output of that name (the output wins), or a GROUP BY item its SELECT lists an output of
    that name and no registered relation of the SELECT has (the input wins). The output's own
    columns are judged where the list reads them."""
    if col.table:
        return False
    parent = col.parent
    if isinstance(parent, exp.Ordered) and isinstance(parent.parent, exp.Order):
        query = parent.parent.parent
        if isinstance(query, exp.SetOperation):
            return True
        return isinstance(query, exp.Select) and col.name in _output_names(query)
    if isinstance(parent, exp.Group) and isinstance(parent.parent, exp.Select):
        select = parent.parent
        if col.name not in _output_names(select):
            return False
        am = _alias_map(select, gov_ctx, cte_names_set)
        return not any(_registers(gov_ctx, tid, col.name) for tid in am.values())
    return False


def _output_names(select: exp.Select) -> set[str]:
    return {_output_name(expr) for expr in select.expressions if not _is_star(expr)}


def _column_violation(
    col: exp.Column, gov_ctx: GovernanceContext, cte_names_set: frozenset[str]
) -> ValidationViolation | None:
    """V003 for ``col`` where it resolves: in its own SELECT, then each SELECT around it (an
    outer reference), as PostgreSQL resolves it. Unqualified, it resolves to every registered
    relation of the scope that has it (one whose columns are not known may), and is refused when
    one of them does not serve it; else to a relation of the scope no registry governs (a table
    function, VALUES, a derived table, a CTE) known to provide it. A source whose columns are not
    known never ends the walk: the name may be an outer reference to a governed column. A name no
    relation of any scope around it is known to have can only be a column of a registered
    relation the model does not serve, or none: it is judged against every registered relation it
    passed."""
    passed: list[int] = []
    scope = col.find_ancestor(exp.Select)
    while scope is not None:
        am = _alias_map(scope, gov_ctx, cte_names_set)
        others = _unregistered_sources(scope, gov_ctx, cte_names_set)
        if col.table:
            if col.table in am:
                return _hidden(col, [am[col.table]], gov_ctx)
            if col.table in others:
                return None  # read in its own SELECT, or not governed data (V006 says which)
        else:
            owners = [tid for tid in am.values() if _registers(gov_ctx, tid, col.name)]
            if owners:
                return _hidden(col, owners, gov_ctx)
            if any(names is not None and col.name in names for names in others.values()):
                return None
            passed += am.values()
        scope = scope.find_ancestor(exp.Select)
    return None if col.table else _hidden(col, passed, gov_ctx)


def _registers(gov_ctx: GovernanceContext, tid: int, name: str) -> bool:
    """Whether registered table ``tid`` has the column a statement reads as ``name``; a table
    whose columns are not known may have any."""
    columns = gov_ctx.all_columns.get(tid)
    if not columns:
        return True
    return registered_name(gov_ctx, tid, name) in {c for c, _ in columns}


def _hidden(
    col: exp.Column, tids: list[int], gov_ctx: GovernanceContext
) -> ValidationViolation | None:
    """V003 when one of the registered tables ``tids`` ``col`` resolves to does not serve it."""
    said = f"{col.table}.{col.name}" if col.table else repr(col.name)
    return _hidden_name(said, col.name, tids, gov_ctx)


def _hidden_name(
    said: str, name: str, tids: list[int], gov_ctx: GovernanceContext
) -> ValidationViolation | None:
    for tid in tids:
        vis = gov_ctx.visible_columns.get(tid)
        if vis is not None and registered_name(gov_ctx, tid, name) not in vis:
            return ValidationViolation("V003", f"Column {said} is not visible to this role")
    return None


def _merged_column_violations(
    select: exp.Select, gov_ctx: GovernanceContext, cte_names_set: frozenset[str]
) -> list[ValidationViolation]:
    """V003 for the columns a USING or NATURAL join merges: the join condition reads each, and a
    star returns it as one column of the two sides (H5), so each is judged against every
    registered relation joined so far that has it. A NATURAL join merges the names both sides
    have; a side whose columns are not known may have any, so every column of the other side is
    judged."""
    from_clause = select.args.get("from_") or select.args.get("from")
    if from_clause is None:
        return []
    joined: list[exp.Expr] = [from_clause.this]
    violations: list[ValidationViolation] = []
    for join in select.args.get("joins") or []:
        using = join.args.get("using") or []
        if using:
            names = {u.name for u in using}
        elif join.method.upper() == "NATURAL":
            names = _shared_names(joined, join.this, gov_ctx, cte_names_set)
        else:
            names = set()
        joined.append(join.this)
        tids = [tid for tid in (_registered_id(i, gov_ctx, cte_names_set) for i in joined) if tid]
        for name in sorted(names):
            owners = [tid for tid in tids if _registers(gov_ctx, tid, name)]
            violation = _hidden_name(repr(name), name, owners, gov_ctx)
            if violation is not None:
                violations.append(violation)
    return violations


def _registered_id(
    item: exp.Expr, gov_ctx: GovernanceContext, cte_names_set: frozenset[str]
) -> int | None:
    if not isinstance(item, exp.Table) or not isinstance(item.this, exp.Identifier):
        return None
    if not item.db and item.name in cte_names_set:
        return None
    return _resolve_table_id(item, gov_ctx)


def _shared_names(
    left: list[exp.Expr],
    right: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
) -> set[str]:
    """The names a NATURAL join merges: those both sides have, where a side's are not known
    every name of the other."""
    ours = _side_columns(left, gov_ctx, cte_names_set)
    theirs = _side_columns([right], gov_ctx, cte_names_set)
    if ours is None:
        return set(theirs or ())
    if theirs is None:
        return set(ours)
    return set(ours & theirs)


def _side_columns(
    items: list[exp.Expr], gov_ctx: GovernanceContext, cte_names_set: frozenset[str]
) -> frozenset[str] | None:
    names: set[str] = set()
    for item in items:
        tid = _registered_id(item, gov_ctx, cte_names_set)
        if tid is not None:
            columns = gov_ctx.all_columns.get(tid)
            if not columns:
                return None
            names.update(c for c, _ in columns)
            continue
        known = _source_columns(item, gov_ctx, cte_names_set, frozenset())
        if known is None:
            return None
        names.update(known)
    return frozenset(names)


# Table functions of one column, which PostgreSQL names by the alias, or else by the function.
_SCALAR_GENERATORS: dict[type, str] = {
    exp.Explode: "unnest",
    exp.Unnest: "unnest",
    exp.GenerateSeries: "generate_series",
    exp.ExplodingGenerateSeries: "generate_series",
}
# The client table functions (pgwire.catalog_route.CLIENT_TABLE_FUNCTIONS) that return one
# column of a base type, named as a scalar generator's is.
_SCALAR_FUNCTIONS = frozenset(
    {
        "generate_subscripts",
        "json_object_keys",
        "jsonb_object_keys",
        "jsonb_path_query",
        "jsonb_path_query_tz",
        "regexp_matches",
        "regexp_split_to_table",
        "string_to_table",
        "unnest",
    }
)
# The client table functions that return a row of known columns, as PostgreSQL names them. The
# json(b)_populate_record(set) and json(b)_to_record(set) functions return a row of the caller's
# type: their columns are the alias's, or not known.
_RECORD_FUNCTIONS: dict[str, tuple[str, ...]] = {
    "_pg_expandarray": ("x", "n"),
    "aclexplode": ("grantor", "grantee", "privilege_type", "is_grantable"),
    "json_array_elements": ("value",),
    "json_array_elements_text": ("value",),
    "json_each": ("key", "value"),
    "json_each_text": ("key", "value"),
    "jsonb_array_elements": ("value",),
    "jsonb_array_elements_text": ("value",),
    "jsonb_each": ("key", "value"),
    "jsonb_each_text": ("key", "value"),
    "pg_get_keywords": ("word", "catcode", "barelabel", "catdesc", "baredesc"),
    "pg_options_to_table": ("option_name", "option_value"),
    "pg_timezone_abbrevs": ("abbrev", "utc_offset", "is_dst"),
    "pg_timezone_names": ("name", "abbrev", "utc_offset", "is_dst"),
    "ts_token_type": ("tokid", "alias", "description"),
}


def _unregistered_sources(
    select: exp.Select,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
    seen: frozenset[str] = frozenset(),
) -> dict[str, frozenset[str] | None]:
    """The relations ``select`` reads that are no registered table, by the name the statement
    gives each: the columns it is known to have, or None where none are known."""
    from_clause = select.args.get("from_") or select.args.get("from")
    items = [from_clause.this] if from_clause is not None else []
    items += [join.this for join in select.args.get("joins") or []]
    sources: dict[str, frozenset[str] | None] = {}
    for item in items:
        if (
            isinstance(item, exp.Table)
            and isinstance(item.this, exp.Identifier)
            and not (not item.db and item.name in cte_names_set)
            and _resolve_table_id(item, gov_ctx) is not None
        ):
            continue  # a registered table (_alias_map)
        sources[item.alias_or_name] = _source_columns(item, gov_ctx, cte_names_set, seen)
    return sources


def _source_columns(
    item: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
    seen: frozenset[str],
) -> frozenset[str] | None:
    """The columns FROM item ``item``, no registered table, is known to give; None where none
    are known."""
    if isinstance(item, exp.Table) and isinstance(item.this, exp.Identifier):
        if not item.db and item.name in cte_names_set:
            return _alias_columns(item) or _cte_columns(item, gov_ctx, cte_names_set, seen)
        return _alias_columns(item)
    if isinstance(item, exp.Table):
        return _function_columns(item, item.this)
    if isinstance(item, exp.Unnest):
        return _function_columns(item, item)
    if isinstance(item, exp.Values):
        first = item.expressions[0] if item.expressions else None
        width = len(first.expressions) if isinstance(first, exp.Tuple) else 1
        return _renamed(item, [f"column{i}" for i in range(1, width + 1)])
    if isinstance(item, (exp.Subquery, exp.Lateral)) and isinstance(item.this, exp.Query):
        return _alias_columns(item) or _query_columns(item.this, gov_ctx, cte_names_set, seen)
    if isinstance(item, exp.Lateral):
        fn = item.this.this if isinstance(item.this, exp.Table) else item.this
        return _function_columns(item, fn)
    return _alias_columns(item)


def _alias_columns(item: exp.Expr) -> frozenset[str] | None:
    return frozenset(_alias_list(item)) or None


def _alias_list(item: exp.Expr) -> list[str]:
    alias = item.args.get("alias")
    columns = alias.columns if isinstance(alias, exp.TableAlias) else []
    return [c.name for c in columns]


def _renamed(item: exp.Expr, defaults: list[str]) -> frozenset[str]:
    """``defaults``, a FROM item's columns in order, as its alias's column list renames them:
    the first by the list, the rest as they are."""
    named = _alias_list(item)
    return frozenset(named + defaults[len(named) :])


def _function_columns(item: exp.Expr, fn: exp.Expr | None) -> frozenset[str] | None:
    """The columns a table function in FROM gives, where PostgreSQL's naming of them is known:
    a function of one column by the alias, or else by the function; a function of a known row
    by its columns; either renamed by the alias's column list."""
    named = fn.name.lower() if isinstance(fn, exp.Anonymous) else None
    default = _SCALAR_GENERATORS.get(type(fn)) or (named if named in _SCALAR_FUNCTIONS else None)
    alias = item.args.get("alias")
    if default is not None:
        if isinstance(alias, exp.TableAlias) and alias.name and not alias.columns:
            default = alias.name
        columns = [default]
    elif named in _RECORD_FUNCTIONS:
        columns = list(_RECORD_FUNCTIONS[named])
    else:
        return _alias_columns(item)  # a row of the caller's type: its columns are the alias's
    if item.args.get("ordinality") or item.args.get("offset"):
        columns.append("ordinality")
    return _renamed(item, columns)


def _cte_columns(
    item: exp.Table,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
    seen: frozenset[str],
) -> frozenset[str] | None:
    if item.name in seen:
        return None  # a recursive CTE read within itself
    for cte in item.root().find_all(exp.CTE):
        if cte.alias == item.name and isinstance(cte.this, exp.Query):
            return _alias_columns(cte) or _query_columns(
                cte.this, gov_ctx, cte_names_set, seen | {item.name}
            )
    return None


def _query_columns(
    query: exp.Query,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
    seen: frozenset[str],
) -> frozenset[str] | None:
    """The columns a derived table or a CTE gives: its (first) SELECT's output names, a star's
    as the relations it covers have them; None where one is not known."""
    while isinstance(query, (exp.SetOperation, exp.Subquery)):
        query = query.this if isinstance(query, exp.Subquery) else query.left
    if not isinstance(query, exp.Select):
        return None
    names: set[str] = set()
    for expr in query.expressions:
        if not _is_star(expr):
            name = _output_name(expr)
            if name:
                names.add(name)
            continue
        covered = expr.table if isinstance(expr, exp.Column) else None
        am = _alias_map(query, gov_ctx, cte_names_set)
        for alias, tid in am.items():
            if covered is None or alias == covered:
                columns = gov_ctx.all_columns.get(tid)
                if not columns:
                    return None
                names.update(c for c, _ in columns)
        for alias, cols in _unregistered_sources(query, gov_ctx, cte_names_set, seen).items():
            if covered is None or alias == covered:
                if cols is None:
                    return None
                names.update(cols)
    return frozenset(names)


def _output_name(expr: exp.Expr) -> str:
    """The name PostgreSQL gives a SELECT list item, where sqlglot knows it."""
    if expr.alias_or_name:
        return expr.alias_or_name
    while isinstance(expr, exp.Cast):
        expr = expr.this
    if isinstance(expr, exp.Anonymous):
        return expr.name.lower()
    if isinstance(expr, exp.Func):
        return expr.sql_name().lower()
    return expr.alias_or_name


def _is_star(expr: exp.Expr) -> bool:
    return isinstance(expr, exp.Star) or (
        isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star)
    )


def _star_violation(
    expr: exp.Expr, am: dict[str, int], gov_ctx: GovernanceContext
) -> ValidationViolation | None:
    """V003 for a star (``*`` or ``t.*``) over governed tables of which the role is served no
    column: it is refused as a named hidden column is, never read as every column. A star the
    role is served some columns of reads those (``stage2._expand_star``)."""
    if isinstance(expr, exp.Star):
        covered = list(am.items())
    elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
        tid = am.get(expr.table)
        covered = [(expr.table, tid)] if tid is not None else []
    else:
        return None
    if not covered:
        return None  # a star over derived relations only: their own SELECTs are checked
    if any(served_columns(tid, gov_ctx) for _alias, tid in covered):
        return None
    names = ", ".join(sorted(alias for alias, _tid in covered))
    return ValidationViolation("V003", f"No column of {names} is visible to this role")


def served_columns(tid: int, gov_ctx: GovernanceContext) -> list[str]:
    """The registered columns of table ``tid`` the role is served (REQ-040)."""
    vis = gov_ctx.visible_columns.get(tid)
    return [c for c, _ in gov_ctx.all_columns.get(tid, []) if vis is None or c in vis]


def _collect_columns(expr: exp.Expr) -> list[exp.Column]:
    if isinstance(expr, exp.Column):
        return [expr]
    if isinstance(expr, exp.Alias):
        return _collect_columns(expr.this)
    if isinstance(expr, exp.Star):
        return []
    return list(expr.find_all(exp.Column))


# The nodes a FROM item's name is in scope of: a query's own, a DML statement's for its subqueries.
_SCOPES = (exp.Select, exp.Update, exp.Delete, exp.Insert, exp.Merge)


def _relation_names_by_scope(tree: exp.Expr) -> dict[int, set[str]]:
    """The names a column may be qualified by in each scope of ``tree`` (by ``id`` of its
    statement node): each FROM item's alias, else a table's name, else a function's name -- as
    PostgreSQL names an unaliased ``generate_series(1, 3)``. A CTE's name is listed where the CTE
    is declared; it is a relation where it is read."""
    names: dict[int, set[str]] = {}
    for node in tree.find_all(exp.TableAlias, exp.Table, exp.Unnest):
        if isinstance(node, exp.TableAlias):
            name = node.name
        elif node.alias:
            continue  # its TableAlias names it
        elif isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier):
            name = node.name
        else:
            fn = node.this if isinstance(node, exp.Table) else node
            if fn is None:
                continue
            name = fn.sql(dialect="postgres").split("(", 1)[0].rsplit(".", 1)[-1].lower()
        scope = node.find_ancestor(*_SCOPES)
        if name and scope is not None:
            names.setdefault(id(scope), set()).add(name)
    return names


def _names_no_relation(col: exp.Column, scopes: dict[int, set[str]]) -> bool:  # REQ-039
    """Whether ``col`` is qualified by a name that matches no relation in its own scope or any
    enclosing one -- compared exactly, in the reading of the statement ``scopes`` was taken from.
    PostgreSQL refuses it (a missing FROM-clause entry); governance refuses it before that, never
    reading it as a column it need not check."""
    if not col.table:
        return False
    scope = col.find_ancestor(*_SCOPES)
    while scope is not None:
        if col.table in scopes.get(id(scope), ()):
            return False
        scope = scope.find_ancestor(*_SCOPES)
    return True


def _no_relation_violation(col: exp.Column) -> ValidationViolation:
    written = col.sql(dialect="postgres")
    return ValidationViolation(
        "V003",
        f"Column {written}: {col.table!r} names no relation in scope (a column is qualified by "
        f"its relation's name or alias as the FROM clause writes it)",
    )


# --------------------------------------------------------------------------- #
# V004 – Cycle pruning (back-edges removed in-place, no violation raised)     #
# --------------------------------------------------------------------------- #


def _check_dag(
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Prune back-edges from cyclic join graphs rather than rejecting them.

    When a cycle is detected the join(s) that close it are removed in-place and
    any SELECT-list columns that referenced the pruned table are replaced with
    NULL so the rest of the query remains valid.
    """
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        joins = select.args.get("joins") or []

        # Build (src_id, tgt_id) → join-node mapping alongside the flat edge list.
        edge_join: list[tuple[int, int, exp.Join]] = []
        for join in joins:
            on_expr = join.args.get("on")
            if on_expr is None:
                continue
            for tbl in join.find_all(exp.Table):
                if _inside_subquery(tbl, select) or tbl.name in cte_names_set:
                    continue
                tgt_tid = _resolve_table_id(tbl, gov_ctx)
                if tgt_tid is None:
                    continue
                for lt, rt in ((p[0], p[2]) for p in _extract_eq_pairs(on_expr)):
                    lt_id = am.get(lt)
                    rt_id = am.get(rt)
                    if lt_id is None or rt_id is None:
                        continue
                    src_id = lt_id if rt_id == tgt_tid else rt_id
                    edge_join.append((src_id, tgt_tid, join))

        edges = [(s, t) for s, t, _ in edge_join]
        back = _back_edges(edges)
        if not back:
            continue

        # Collect join nodes that correspond to back-edges and remove them.
        pruned_joins: set[int] = set()
        for src_id, tgt_id, join_node in edge_join:
            if (src_id, tgt_id) in back:
                pruned_joins.add(id(join_node))

        pruned_tids: set[int] = set()
        for src_id, tgt_id, join_node in edge_join:
            if id(join_node) in pruned_joins:
                pruned_tids.add(tgt_id)

        select.set("joins", [j for j in joins if id(j) not in pruned_joins])

        # Replace SELECT-list columns that reference pruned tables with NULL.
        tid_to_alias: dict[int, str] = {v: k for k, v in am.items()}
        pruned_aliases = {tid_to_alias[t] for t in pruned_tids if t in tid_to_alias}
        if pruned_aliases:
            new_exprs = []
            for expr in select.expressions:
                col = expr.this if isinstance(expr, exp.Alias) else expr
                if isinstance(col, exp.Column) and (col.table or "") in pruned_aliases:
                    alias = expr.alias if isinstance(expr, exp.Alias) else col.name
                    new_exprs.append(exp.alias_(exp.null(), alias))
                else:
                    new_exprs.append(expr)
            select.set("expressions", new_exprs)

    return []


def _back_edges(edges: list[tuple[int, int]]) -> set[tuple[int, int]]:
    """Return the set of back-edges that form cycles (iterative DFS)."""
    from collections import defaultdict

    if not edges:
        return set()

    children: dict[int, list[int]] = defaultdict(list)
    nodes: set[int] = set()
    for src, tgt in edges:
        children[src].append(tgt)
        nodes.add(src)
        nodes.add(tgt)

    visited: set[int] = set()
    in_stack: set[int] = set()
    back: set[tuple[int, int]] = set()

    for start in nodes:
        if start in visited:
            continue
        # Iterative DFS: stack holds (node, iterator-over-children, entered-stack)
        stack: list[tuple[int, object, bool]] = [(start, iter(children[start]), True)]
        while stack:
            node, it, entering = stack[-1]
            if entering:
                visited.add(node)
                in_stack.add(node)
                stack[-1] = (node, it, False)
            try:
                child = next(it)  # type: ignore[call-overload]
                if child not in visited:
                    stack.append((child, iter(children[child]), True))
                elif child in in_stack:
                    back.add((node, child))
            except StopIteration:
                in_stack.discard(node)
                stack.pop()

    return back


# --------------------------------------------------------------------------- #
# V005 – Masked columns in predicates                                         #
# --------------------------------------------------------------------------- #


def _filters_plaintext(gov_ctx: GovernanceContext, tid: int, col_name: str) -> bool:
    """Whether a predicate on the column would read the real value under its mask. A faked
    column's predicates read its fake -- the faked projection of its table -- never the real value
    (REQ-1494), so it may be filtered on."""
    entry = gov_ctx.masking_rules.get((tid, registered_name(gov_ctx, tid, col_name)))
    if entry is None:
        return False
    from provisa.security.masking import MaskType

    return entry[0].mask_type != MaskType.fake


def _check_xml_mappings(tree: exp.Expr) -> list[ValidationViolation]:  # REQ-2167
    """Reject query_to_xml and its kin: what they read is in a string, which this statement's
    governance cannot see, and a PostgreSQL source would run it as Provisa's own connection. A
    surface that serves them answers each first, through governed reads (compiler.xml_mapping)."""
    return [
        ValidationViolation("V006", f"{call.name.lower()} is not a governed relation")
        for call in tree.find_all(exp.Anonymous)
        if call.name.lower() in MAPPINGS
    ]


def _check_masked_in_xmltable(  # REQ-040, REQ-2167
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Reject a masked or hidden column anywhere in XMLTABLE: its document, a path or a default.
    Masks are written on the statement's projections and visibility checked on its select lists,
    and XMLTABLE is in FROM, so its rows would come from the plaintext."""
    violations = []
    scopes = _relation_names_by_scope(tree)
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for table in select.find_all(exp.XMLTable):
            for col in table.find_all(exp.Column):
                if _names_no_relation(col, scopes):
                    violations.append(_no_relation_violation(col))
                    continue
                tids = [am[col.table]] if col.table in am else []
                tids = tids if col.table else list(am.values())
                written = col.sql(dialect="postgres")
                if any(_filters_plaintext(gov_ctx, tid, col.name) for tid in tids):
                    violations.append(
                        ValidationViolation(
                            "V005", f"Column {written} is masked and may not be read by XMLTABLE"
                        )
                    )
                for tid in tids:
                    vis = gov_ctx.visible_columns.get(tid)
                    if vis is not None and registered_name(gov_ctx, tid, col.name) not in vis:
                        violations.append(
                            ValidationViolation(
                                "V003", f"Column {written} is not visible to this role"
                            )
                        )
    return violations


def _check_masked_in_predicate(  # REQ-040, REQ-263
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Reject masked columns in WHERE/HAVING — they would filter on plaintext,
    allowing inference of the unmasked value despite output masking."""
    violations = []
    scopes = _relation_names_by_scope(tree)
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for clause in (select.args.get("where"), select.args.get("having")):
            if clause is None:
                continue
            for col in clause.find_all(exp.Column):
                col_name = col.name
                tbl_ref = col.table
                if _names_no_relation(col, scopes):
                    violations.append(_no_relation_violation(col))
                    continue
                if approved_join_key(col, gov_ctx):
                    # A registered relationship's join condition: it relates the rows on the real
                    # key and filters nothing of the role's own (REQ-603).
                    continue
                if tbl_ref:
                    tid = am.get(tbl_ref)
                    if tid is None:
                        continue
                    if _filters_plaintext(gov_ctx, tid, col_name):
                        violations.append(
                            ValidationViolation(
                                "V005",
                                f"Column {tbl_ref}.{col_name} is masked and may not appear in a WHERE or HAVING clause",
                            )
                        )
                else:
                    for tid in am.values():
                        if _filters_plaintext(gov_ctx, tid, col_name):
                            violations.append(
                                ValidationViolation(
                                    "V005",
                                    f"Column {col_name!r} is masked and may not appear in a WHERE or HAVING clause",
                                )
                            )
                            break
    return violations
