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

from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.cte_utils import cte_names
from provisa.compiler.schema_gen import _IMPLICIT_TRAVERSAL_DOMAINS
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import GovernanceContext
from provisa.security.rights import reaches_all_domains


@dataclass
class ValidationViolation:
    code: str
    message: str


def validate_sql(  # REQ-001, REQ-002, REQ-038, REQ-266
    sql: str,
    ctx: CompilationContext,
    gov_ctx: GovernanceContext,
    role: dict,
    _raw_tables: list[dict],  # pyright: ignore[reportUnusedParameter]
    *,
    bypass_relationship_guard: bool = False,
    bypass_uncovered_relationships: bool = False,
) -> list[ValidationViolation]:
    """Validate SQL against role-scoped GraphQL-equivalent access rules."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception as exc:
        return [ValidationViolation("V000", f"SQL parse error: {exc}")]

    violations: list[ValidationViolation] = []
    cte_names_set = cte_names(tree)
    violations += _check_registered_relations(tree, gov_ctx, ctx, cte_names_set)

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
        violations += tables_outside_relationships(
            tree,
            gov_ctx,
            valid_joins,
            table_id_to_meta,
            bypass_uncovered=bypass_uncovered_relationships,
        )
    violations += _check_column_visibility(tree, gov_ctx, cte_names_set)
    violations += _check_dag(tree, gov_ctx, cte_names_set)
    violations += _check_masked_in_predicate(tree, gov_ctx, cte_names_set)

    return violations


# --------------------------------------------------------------------------- #
# V006 – Registered relations only                                             #
# --------------------------------------------------------------------------- #

# Table functions that only generate rows from their literal arguments — no engine state, no
# file or network read.
_ROW_GENERATORS = (exp.GenerateSeries, exp.ExplodingGenerateSeries)


def _check_registered_relations(  # REQ-001, REQ-266
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    ctx: CompilationContext,
    cte_names_set: frozenset[str] = frozenset(),
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
        if not isinstance(target, exp.Identifier):
            fn = target.sql(dialect="postgres").split("(", 1)[0] if target is not None else "?"
            violations.append(
                ValidationViolation("V006", f"Table function {fn!r} is not a governed relation")
            )
            continue
        if not tbl.db and tbl.name in cte_names_set:
            continue
        # A registered table is named by its semantic/physical name (table_map) or, unqualified, by
        # its field name — the domain-prefixed ``sa__orders`` form a domain_prefix schema exposes.
        field_named = not tbl.db and tbl.name in ctx.tables
        if _resolve_table_id(tbl, gov_ctx) is None and not field_named:
            ref = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
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
    db = tbl.db
    name = tbl.name
    if db:
        full = f"{db}.{name}"
        if full in gov_ctx.table_map:
            return gov_ctx.table_map[full]
    return gov_ctx.table_map.get(name)


def _check_domain_access(  # REQ-039, REQ-263
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    table_id_to_meta: dict[int, TableMeta],
    domain_access: list[str],
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    # A role reaches the domains it lists; "*" is all; an EMPTY list is none, so every table
    # with a domain is a V001 for it (rights.reaches_all_domains decides, single-domain mode
    # included).
    if reaches_all_domains(domain_access):
        return []

    violations = []
    for select in tree.find_all(exp.Select):
        for tbl in _from_tables(select, cte_names_set):
            tid = _resolve_table_id(tbl, gov_ctx)
            if tid is None:
                continue
            meta = table_id_to_meta.get(tid)
            if meta is None:
                continue
            if meta.domain_id and meta.domain_id not in domain_access:
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


def tables_outside_relationships(  # REQ-264
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    valid_joins: set[tuple[int, int, str, str]],
    table_id_to_meta: dict[int, TableMeta],
    *,
    bypass_uncovered: bool = False,
) -> list[ValidationViolation]:
    """V002 for each pair of registered tables ``tree`` combines outside the registered
    relationships, however the statement is written (provisa.compiler.join_guard)."""
    from provisa.compiler.join_guard import NOT_EQUALITY, UNREGISTERED, unrelated_tables

    covered_pairs = {(s, t) for s, t, _, _ in valid_joins}

    def exempt(table_id: int) -> bool:
        # meta/ops tables are implicitly traversable — no registered relationship required
        meta = table_id_to_meta.get(table_id)
        return meta is not None and meta.domain_id in _IMPLICIT_TRAVERSAL_DOMAINS

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
    ):
        left, right = name(pair.left_table), name(pair.right_table)
        if pair.reason == UNREGISTERED:
            said = (
                f"Invalid JOIN: {left}.{pair.left_column} = {right}.{pair.right_column} — no "
                f"approved relationship exists between these tables on these columns"
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
        violations.append(ValidationViolation("V002", said))
    return violations


# --------------------------------------------------------------------------- #
# V003 – Column visibility                                                     #
# --------------------------------------------------------------------------- #


def _check_column_visibility(  # REQ-040, REQ-263, REQ-265
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    violations = []
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for expr in select.expressions:
            cols = _collect_columns(expr)
            for col in cols:
                tbl_ref = col.table
                col_name = col.name
                if not tbl_ref:
                    for tid in am.values():
                        vis = gov_ctx.visible_columns.get(tid)
                        if vis is not None and col_name not in vis:
                            violations.append(
                                ValidationViolation(
                                    "V003",
                                    f"Column {col_name!r} is not visible to this role",
                                )
                            )
                            break
                else:
                    tid = am.get(tbl_ref)
                    if tid is None:
                        continue
                    vis = gov_ctx.visible_columns.get(tid)
                    if vis is not None and col_name not in vis:
                        violations.append(
                            ValidationViolation(
                                "V003",
                                f"Column {tbl_ref}.{col_name} is not visible to this role",
                            )
                        )
    return violations


def _collect_columns(expr: exp.Expr) -> list[exp.Column]:
    if isinstance(expr, exp.Column):
        return [expr]
    if isinstance(expr, exp.Alias):
        return _collect_columns(expr.this)
    if isinstance(expr, exp.Star):
        return []
    return list(expr.find_all(exp.Column))


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
    entry = gov_ctx.masking_rules.get((tid, col_name))
    if entry is None:
        return False
    from provisa.security.masking import MaskType

    return entry[0].mask_type != MaskType.fake


def _check_masked_in_predicate(  # REQ-040, REQ-263
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    """Reject masked columns in WHERE/HAVING — they would filter on plaintext,
    allowing inference of the unmasked value despite output masking."""
    violations = []
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for clause in (select.args.get("where"), select.args.get("having")):
            if clause is None:
                continue
            for col in clause.find_all(exp.Column):
                col_name = col.name
                tbl_ref = col.table
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
