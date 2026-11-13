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
from provisa.compiler.cte_utils import cte_in_scope, cte_names, names_a_cte_in_scope
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import (
    GovernanceContext,
    approved_join_key,
    flatten_leftmost_join_groups,
    parenthesised_join_message,
    parenthesised_join_tables,
    registered_name,
)
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
        # REQ-2222: answered by the app's PermissionError handler under this stable code.
        self.code = "data.governance_refused"
        self.params: dict[str, object] = {
            "violations": [{"code": v.code, "message": v.message} for v in violations]
        }


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
    flatten_leftmost_join_groups(written)  # as stage 2 does: a leftmost group is the flat join
    _qualified_calls_as_calls(written)
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


def _qualified_calls_as_calls(tree: exp.Expr) -> None:  # REQ-039, REQ-2167
    """Read a schema-qualified call sqlglot takes for a column as the call it is, in place.

    sqlglot parses ``pg_catalog."cume_dist"(x) WITHIN GROUP (ORDER BY y)`` -- and a qualified
    call with FILTER -- as a column whose name is the call and whose relation is the schema,
    where ``pg_catalog."lower"(x)`` is a Dot of the schema and the call. As a column it is
    refused, its schema naming no relation in scope, and the columns inside it are never
    judged. It becomes the Dot every other qualified call is: a function call, whose arguments,
    ORDER BY and FILTER are judged as the expressions they are. The call is recognized by its
    node -- a WithinGroup, or a Filter over a function or a WithinGroup -- and any other shape
    stays a column, judged as one (REQ-039)."""
    for col in list(tree.find_all(exp.Column)):
        call = col.this
        if isinstance(call, exp.Filter):
            call = call.this
        if not isinstance(call, (exp.WithinGroup, exp.Func)):
            continue
        qualifiers = [col.args[k] for k in ("catalog", "db", "table") if col.args.get(k)]
        col.replace(exp.Dot.build([*qualifiers, col.this]))


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
    violations += _check_unreadable_shapes(tree, gov_ctx)

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
    # A table named by its field name (the domain-prefixed ``sa__orders``) is admitted by
    # _check_registered_relations through ctx.tables; its columns are judged by the same rule.
    field_named = {name: meta.table_id for name, meta in ctx.tables.items()}
    violations += _check_column_visibility(tree, gov_ctx, cte_names_set, field_named)
    # On its own copy: it prunes joins in place, and the checks after it judge the statement as
    # written -- a pruned join's alias is still in scope, its columns still governed (REQ-039).
    violations += _check_dag(tree.copy(), gov_ctx, cte_names_set)
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


def _check_unreadable_shapes(  # REQ-039
    tree: exp.Expr, gov_ctx: GovernanceContext
) -> list[ValidationViolation]:
    """V006 for a shape that hides a registered relation from governance, refused until every
    reference to one is governed by construction (RV597b):

    - a parenthesised FROM item that is no query -- ``(bank.customer CROSS JOIN v)``, or
      ``(bank.customer) x`` -- holding a registered relation: neither the column checks nor stage
      2 see the relation inside it, so its hidden columns, masks and row rules go unapplied;
    - a data write placed anywhere but where REQ-039 admits one -- the statement's root, or
      ``WITH w AS (<write> RETURNING ...) SELECT ... FROM w`` -- or a second write
      (:func:`~provisa.compiler.write_admission.the_write`);
    - a CTE named as a registered relation the statement reads: the column checks take the
      relation for the CTE."""
    from provisa.compiler.write_admission import WriteNotAdmitted, the_write

    violations = [
        ValidationViolation("V006", parenthesised_join_message(tbl))
        for tbl in parenthesised_join_tables(tree, gov_ctx)
    ]
    try:
        the_write(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub types parse_one as Expr
    except WriteNotAdmitted as refused:
        violations.append(ValidationViolation("V006", str(refused).removeprefix("[V006] ")))
    read = {
        tbl.name
        for tbl in tree.find_all(exp.Table)
        if tbl.db and _resolve_table_id(tbl, gov_ctx) is not None
    }
    for cte in tree.find_all(exp.CTE):
        if cte.alias in read:
            violations.append(
                ValidationViolation(
                    "V006",
                    f"CTE {cte.alias!r} is named as a registered relation the statement reads: "
                    "give it another name",
                )
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


def _check_column_visibility(  # REQ-040, REQ-263, REQ-265
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
    field_named: dict[str, int] | None = None,
) -> list[ValidationViolation]:
    violations = []
    scopes = _relation_names_by_scope(tree)
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for expr in select.expressions:
            if _is_star(expr):
                if isinstance(expr, exp.Column) and _names_no_relation(expr, scopes):
                    violations.append(_no_relation_violation(expr))
                    continue
                # A star's columns are the ones the role is served (stage2._expand_star); it is
                # refused only when that is none of them.
                star = _star_violation(expr, am, gov_ctx)
                if star is not None:
                    violations.append(star)
                continue
            cols = _collect_columns(expr)
            for col in cols:
                tbl_ref = col.table
                col_name = col.name
                if _names_no_relation(col, scopes):
                    violations.append(_no_relation_violation(col))
                    continue
                if col_name in SERVED_WITH_EVERY_TABLE:
                    # A virtual column (a table's _domain_/_name_, registered_tables'
                    # table_query_name) is served with the table to every role that reads it,
                    # as GraphQL serves it; the lowering answers it (sql_rewrite).
                    continue
                if approved_join_key(col, gov_ctx):
                    # A registered relationship's join key in a nested SELECT's condition (a
                    # relationship field): read by the engine to join, never returned (REQ-603).
                    continue
                if not tbl_ref:
                    if _unqualified_column_refused(col_name, am, gov_ctx):
                        violations.append(
                            ValidationViolation(
                                "V003",
                                f"Column {col_name!r} is not visible to this role",
                            )
                        )
                else:
                    named = _qualified_relation(col, gov_ctx, cte_names_set, field_named or {})
                    if named is None:
                        violations.append(
                            ValidationViolation(
                                "V003",
                                f"Column {tbl_ref}.{col_name}: {tbl_ref!r} names no governed "
                                "relation, so the column is not read",
                            )
                        )
                        continue
                    if not isinstance(named, int):
                        continue  # _DERIVED: a derived relation's own SELECT is checked where it is
                    tid = named
                    vis = gov_ctx.visible_columns.get(tid)
                    # Refuse when no governance data (key absent = fail closed) OR when the role's
                    # visible set exists and excludes this column (REQ-2226, REQ-039).
                    if (tid not in gov_ctx.visible_columns) or (
                        vis is not None and registered_name(gov_ctx, tid, col_name) not in vis
                    ):
                        violations.append(
                            ValidationViolation(
                                "V003",
                                f"Column {tbl_ref}.{col_name} is not visible to this role",
                            )
                        )
        violations += _hidden_sort_keys(select, am, gov_ctx)
        violations += _hidden_join_keys(select, am, gov_ctx)
    return violations


#: What :func:`_qualified_relation` answers for a qualifier naming a derived relation: a CTE, a
#: derived table, a row generator or table function, or the session's own temporary table.
_DERIVED = object()


#: The nodes that name a relation a column may be qualified by.
_RELATIONS = (exp.Table, exp.Subquery, exp.Lateral, exp.Unnest, exp.Values)


def _scope_relations(scope: exp.Expr) -> list[exp.Expr]:
    """The relations ``scope`` itself names: its FROM items and joins at any depth of
    parenthesised join groups (a BI tool's left-deep ``((a JOIN b) JOIN c)``), and a DML
    statement's target -- not those of a SELECT nested in it, which is its own scope."""
    return [
        node
        for node in scope.find_all(*_RELATIONS)
        if node is not scope and _nearest_scope(node) is scope
    ]


def _nearest_scope(node: exp.Expr) -> exp.Expr | None:
    return node.find_ancestor(*_SCOPES)


def _qualified_relation(  # REQ-039, REQ-2226
    col: exp.Column,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str],
    field_named: dict[str, int],
) -> int | object | None:
    """The registered table a qualified column ``col`` reads, found by its qualifier in the
    column's own SELECT and then each enclosing one (a correlated reference reads an outer
    table); :data:`_DERIVED` for a qualifier naming a derived relation; None for one naming a
    table no registered table resolves, or no relation at all. None is refused, never read as a
    column there is nothing to check: unresolved, ``o.secret`` was served to a role whose grant
    does not name it (REQ-2226)."""
    select = _nearest_scope(col)
    while select is not None:
        items = _scope_relations(select)
        for item in items:
            if item.alias_or_name != col.table:
                continue
            if not isinstance(item, exp.Table) or not isinstance(item.this, exp.Identifier):
                return _DERIVED
            if not item.db and (
                item.name in cte_names_set
                or names_a_cte_in_scope(item)
                or item.name in temp_tables.names()
            ):
                return _DERIVED
            tid = _resolve_table_id(item, gov_ctx)
            if tid is None and not item.db:
                tid = field_named.get(item.name)
            return tid
        if any(col.table in _value_columns(item) for item in items):
            # ``"__b"."n"`` over ``UNNEST($1) AS _uw0(__b)``, ``"m"."k"`` over ``FROM _w0`` whose
            # SELECT gives ``m``: the qualifier is a value column of a derived relation, a field
            # of which the column reads -- checked where that relation's own SELECT is.
            return _DERIVED
        select = _nearest_scope(select)
    return None


def _value_columns(item: exp.Expr) -> set[str]:  # REQ-039, REQ-2176
    """The value columns FROM item ``item`` gives that a column may be qualified by, reading a
    field of one (a Cypher map, a row a write gave): those its column list binds
    (``UNNEST($1) AS _uw0(__b)`` binds ``__b``), and those a derived relation's SELECT gives --
    a derived table's, or a CTE's it names. A table's own columns are not: a field of one is read
    through the table, whose columns governance judges by name."""
    alias = item.args.get("alias")
    if isinstance(alias, exp.TableAlias) and alias.args.get("columns"):
        return {column.name for column in alias.args["columns"]}
    if isinstance(item, exp.Subquery) and isinstance(item.this, exp.Query):
        return set(item.this.named_selects)
    if isinstance(item, exp.Table) and isinstance(item.this, exp.Identifier):
        cte = cte_in_scope(item)
        if cte is not None:
            listed = cte.args["alias"].args.get("columns") if cte.args.get("alias") else None
            if listed:
                return {column.name for column in listed}
            if isinstance(cte.this, exp.Query):
                return set(cte.this.named_selects)
    return set()


def _unqualified_column_refused(  # REQ-039, REQ-2190
    col_name: str, am: dict[str, int], gov_ctx: GovernanceContext
) -> bool:
    """Whether an unqualified ``col_name`` read beside the relations of ``am`` is refused (V003).

    It is judged at the relations in scope that have it -- register it, or serve it: refused when
    any of them hides it. A relation without the name has no say: psycopg2's hstore lookup
    (``SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ...``, sent by every SQLAlchemy
    psycopg2 connection) names pg_type's column beside pg_namespace. When no relation in scope
    has the name it is refused if any of them is governed, as before: a physical column the model
    does not serve is never passed through."""
    holders: list[int] = []
    for tid in am.values():
        name = registered_name(gov_ctx, tid, col_name)
        vis = gov_ctx.visible_columns.get(tid)
        registered = {c for c, _ in gov_ctx.all_columns.get(tid, [])}
        if name in registered or (vis is not None and name in vis):
            holders.append(tid)
            if vis is not None and name not in vis:
                return True
    if holders:
        return False
    # Refuse when any table in scope is governed (key present in visible_columns) OR has no
    # governance data at all (key absent = fail closed, REQ-039, REQ-2226).  The only safe case
    # is every table explicitly marked "all visible" (key present, value None).
    return any(
        gov_ctx.visible_columns.get(tid) is not None or tid not in gov_ctx.visible_columns
        for tid in am.values()
    )


def _hidden_columns(tid: int, gov_ctx: GovernanceContext) -> set[str]:
    """The registered columns of table ``tid`` the role is not served."""
    vis = gov_ctx.visible_columns.get(tid)
    if vis is None:
        return set()
    return {c for c, _ in gov_ctx.all_columns.get(tid, [])} - vis


def _hidden_join_keys(  # REQ-039
    select: exp.Select, am: dict[str, int], gov_ctx: GovernanceContext
) -> list[ValidationViolation]:
    """V003 for a join of ``select`` that matches rows on a hidden column it never names in a
    select list: ``USING (ssn)``, or a NATURAL join beside a table with a hidden column (it joins
    on every common name, which the other side may give). Either counts rows by the hidden value."""
    violations = []
    for join in select.args.get("joins") or []:
        for key in join.args.get("using") or []:
            if any(
                registered_name(gov_ctx, tid, key.name) in _hidden_columns(tid, gov_ctx)
                for tid in am.values()
            ):
                violations.append(
                    ValidationViolation(
                        "V003", f"Column {key.name!r} is not visible to this role (JOIN USING)"
                    )
                )
        if (join.args.get("method") or "").upper() == "NATURAL" and any(
            _hidden_columns(tid, gov_ctx) for tid in am.values()
        ):
            violations.append(
                ValidationViolation(
                    "V003",
                    "A NATURAL JOIN over a table with columns not visible to this role may match "
                    "on them: name the join's columns",
                )
            )
    return violations


def _hidden_sort_keys(  # REQ-039
    select: exp.Select, am: dict[str, int], gov_ctx: GovernanceContext
) -> list[ValidationViolation]:
    """V003 for a bare ORDER BY or GROUP BY key of ``select`` that is a hidden registered column,
    as PostgreSQL resolves it: an ORDER BY name is an output's only where an output is named so --
    by a real alias or as a bare column, never by a string literal's text -- and a GROUP BY name is
    the input column first. Names compare exactly, in the reading of the statement judged."""
    outputs = {e.alias for e in select.expressions if isinstance(e, exp.Alias)} | {
        e.name
        for e in select.expressions
        if isinstance(e, exp.Column) and not isinstance(e.this, exp.Star)
    }
    keys: list[exp.Column] = []
    order = select.args.get("order")
    if order is not None:
        keys += [
            o.this
            for o in order.expressions
            if isinstance(o.this, exp.Column) and o.this.name not in outputs
        ]
    group = select.args.get("group")
    if group is not None:
        keys += [g for g in group.expressions if isinstance(g, exp.Column)]
    violations = []
    for col in keys:
        if col.table or isinstance(col.this, exp.Star):
            continue
        for tid in am.values():
            name = registered_name(gov_ctx, tid, col.name)
            vis = gov_ctx.visible_columns.get(tid)
            registered = {c for c, _ in gov_ctx.all_columns.get(tid, [])}
            if vis is not None and name in registered and name not in vis:
                violations.append(
                    ValidationViolation("V003", f"Column {col.name!r} is not visible to this role")
                )
                break
    return violations


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
    is declared; it is a relation where it is read. A derived FROM item's value columns
    (:func:`_value_columns`) are names too, each a value whose fields a qualified column reads --
    ``"__b"."n"`` over ``UNNEST($1) AS _uw0(__b)``, as REQ-2176 unwinds the rows a write gave;
    ``"m"."k"`` over a WITH's rows carrying the map ``m``."""
    names: dict[int, set[str]] = {}
    for scope in tree.find_all(*_SCOPES):
        for item in _scope_relations(scope):
            names.setdefault(id(scope), set()).update(_value_columns(item))
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
                    # Refuse when no governance data (key absent = fail closed) OR when the role's
                    # visible set exists and excludes this column (REQ-039).
                    if (tid not in gov_ctx.visible_columns) or (
                        vis is not None and registered_name(gov_ctx, tid, col.name) not in vis
                    ):
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
