# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1b2c4-d5e6-7890-abcd-ef1234567890
# (run scripts/canary_stamp.py on this file after creating it)

"""SQL validator: enforce GraphQL-equivalent access rules on raw SQL.

Rules:
  V001 – FROM-clause table's domain must be in role's domain_access (or domain_access empty).
  V002 – Every JOIN ON condition must match an approved relationship (source_col = target_col).
          Exception: JOINs to 'meta' or 'ops' domain tables are implicitly authorized (traversal only).
  V003 – Referenced columns must be visible to this role.
  V004 – Join graph must be a DAG (no cycles).
  V005 – Masked columns must not appear in WHERE or HAVING clauses (prevents plaintext inference).

Security model / layer responsibilities:
  V001 and V003+RLS are the hard security primitives. V001 gates domain access; V003 and
  stage2 RLS control what data is visible in query output regardless of SQL structure.

  V002 is governance policy, not a hard security boundary. It marks approved traversal
  paths between tables a role already has access to. A role cannot use an unapproved join
  to reach data outside its V001 domain grant or past V003/RLS content controls — so
  circumventing V002 does not expose data the role couldn't obtain through two separate
  approved queries. V002 exists to enforce approved roads and provide an audit surface for
  deliberate circumvention, not to be the last line of defence.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.cte_utils import cte_names
from provisa.compiler.schema_gen import _IMPLICIT_TRAVERSAL_DOMAINS
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import GovernanceContext


@dataclass
class ValidationViolation:
    code: str
    message: str


def validate_sql(
    sql: str,
    ctx: CompilationContext,
    gov_ctx: GovernanceContext,
    role: dict,
    raw_tables: list[dict],
    discovery_mode: bool = False,
) -> list[ValidationViolation]:
    """Validate SQL against role-scoped GraphQL-equivalent access rules."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception as exc:
        return [ValidationViolation("V000", f"SQL parse error: {exc}")]

    violations: list[ValidationViolation] = []
    cte_names_set = cte_names(tree)

    # Build reverse maps
    table_id_to_meta: dict[int, TableMeta] = {}
    for meta in ctx.tables.values():
        table_id_to_meta[meta.table_id] = meta

    # Build (src_table_id, tgt_table_id, src_col, tgt_col) approved join set
    type_to_meta: dict[str, TableMeta] = {}
    for meta in ctx.tables.values():
        type_to_meta[meta.type_name] = meta

    valid_joins: set[tuple[int, int, str, str]] = set()
    for (type_name, _), jm in ctx.joins.items():
        src = type_to_meta.get(type_name)
        if not src:
            continue
        valid_joins.add((src.table_id, jm.target.table_id, jm.source_column, jm.target_column))
        valid_joins.add((jm.target.table_id, src.table_id, jm.target_column, jm.source_column))

    domain_access: list[str] = role.get("domain_access") or []

    if not discovery_mode:
        violations += _check_domain_access(
            tree, gov_ctx, table_id_to_meta, domain_access, cte_names_set
        )
        violations += _check_join_relationships(
            tree, gov_ctx, valid_joins, table_id_to_meta, cte_names_set
        )
    violations += _check_column_visibility(tree, gov_ctx, cte_names_set)
    violations += _check_dag(tree, gov_ctx, valid_joins, cte_names_set)
    violations += _check_masked_in_predicate(tree, gov_ctx, cte_names_set)

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


def _join_tables(
    select: exp.Select, cte_names_set: frozenset[str] = frozenset()
) -> list[tuple[exp.Table, exp.Expression | None]]:
    """Return (table, ON-condition) for each JOIN in a SELECT, skipping CTE aliases."""
    results = []
    for join in select.args.get("joins") or []:
        on = join.args.get("on")
        for tbl in join.find_all(exp.Table):
            if not _inside_subquery(tbl, select) and tbl.name not in cte_names_set:
                results.append((tbl, on))
    return results


def _inside_subquery(node: exp.Expression, stop: exp.Expression) -> bool:
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


def _check_domain_access(
    tree: exp.Expression,
    gov_ctx: GovernanceContext,
    table_id_to_meta: dict[int, TableMeta],
    domain_access: list[str],
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    if not domain_access or "*" in domain_access:
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


def _extract_eq_pairs(on_expr: exp.Expression) -> list[tuple[str, str, str, str]]:
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


def _alias_to_table_name(
    alias: str, am: dict[str, int], table_id_to_meta: dict[int, TableMeta]
) -> str:
    """Return 'alias(table_name)' or just 'alias' if no meta found."""
    tid = am.get(alias)
    if tid is None:
        return alias
    meta = table_id_to_meta.get(tid)
    if meta is None or meta.table_name == alias:
        return alias
    return f"{alias}({meta.table_name})"


def _check_join_relationships(
    tree: exp.Expression,
    gov_ctx: GovernanceContext,
    valid_joins: set[tuple[int, int, str, str]],
    table_id_to_meta: dict[int, TableMeta],
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    violations = []
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        for tbl, on_expr in _join_tables(select, cte_names_set):
            if on_expr is None:
                tbl_ref = f"{tbl.db}.{tbl.name}" if tbl.db else tbl.name
                violations.append(
                    ValidationViolation(
                        "V002",
                        f"JOIN on {tbl_ref!r} has no ON condition — cross joins are not permitted",
                    )
                )
                continue
            pairs = _extract_eq_pairs(on_expr)
            if not pairs:
                continue
            tgt_tid = _resolve_table_id(tbl, gov_ctx)
            if tgt_tid is None:
                continue
            # meta/ops tables are implicitly traversable — no registered relationship required
            tgt_meta = table_id_to_meta.get(tgt_tid)
            if tgt_meta and tgt_meta.domain_id in _IMPLICIT_TRAVERSAL_DOMAINS:
                continue
            on_sql = on_expr.sql(dialect="postgres")
            for lt, lc, rt, rc in pairs:
                lt_id = am.get(lt)
                rt_id = am.get(rt)
                if lt_id is None or rt_id is None:
                    continue
                src_id = lt_id if rt_id == tgt_tid else rt_id
                src_col = lc if rt_id == tgt_tid else rc
                tgt_col = rc if rt_id == tgt_tid else lc
                src_alias = lt if rt_id == tgt_tid else rt
                tgt_alias = tbl.alias or tbl.name
                if (src_id, tgt_tid, src_col, tgt_col) not in valid_joins:
                    src_label = _alias_to_table_name(src_alias, am, table_id_to_meta)
                    tgt_label = _alias_to_table_name(tgt_alias, am, table_id_to_meta)
                    violations.append(
                        ValidationViolation(
                            "V002",
                            f"Invalid JOIN: {src_label}.{src_col} = {tgt_label}.{tgt_col} "
                            f"(full ON: {on_sql}) — no approved relationship exists between these tables on these columns",
                        )
                    )
    return violations


# --------------------------------------------------------------------------- #
# V003 – Column visibility                                                     #
# --------------------------------------------------------------------------- #


def _check_column_visibility(
    tree: exp.Expression,
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


def _collect_columns(expr: exp.Expression) -> list[exp.Column]:
    if isinstance(expr, exp.Column):
        return [expr]
    if isinstance(expr, exp.Alias):
        return _collect_columns(expr.this)
    if isinstance(expr, exp.Star):
        return []
    return list(expr.find_all(exp.Column))


# --------------------------------------------------------------------------- #
# V004 – DAG (no cycles in join graph)                                        #
# --------------------------------------------------------------------------- #


def _check_dag(
    tree: exp.Expression,
    gov_ctx: GovernanceContext,
    valid_joins: set[tuple[int, int, str, str]],
    cte_names_set: frozenset[str] = frozenset(),
) -> list[ValidationViolation]:
    violations = []
    for select in tree.find_all(exp.Select):
        am = _alias_map(select, gov_ctx, cte_names_set)
        edges: list[tuple[int, int]] = []
        for tbl, on_expr in _join_tables(select, cte_names_set):
            if on_expr is None:
                continue
            tgt_tid = _resolve_table_id(tbl, gov_ctx)
            if tgt_tid is None:
                continue
            pairs = _extract_eq_pairs(on_expr)
            for lt, lc, rt, rc in pairs:
                lt_id = am.get(lt)
                rt_id = am.get(rt)
                if lt_id is None or rt_id is None:
                    continue
                src_id = lt_id if rt_id == tgt_tid else rt_id
                edges.append((src_id, tgt_tid))

        if _has_cycle(edges):
            violations.append(
                ValidationViolation(
                    "V004",
                    "JOIN graph contains a cycle — queries must form a directed acyclic graph (parent → child)",
                )
            )
    return violations


def _has_cycle(edges: list[tuple[int, int]]) -> bool:
    from collections import defaultdict, deque

    if not edges:
        return False
    children: dict[int, list[int]] = defaultdict(list)
    in_degree: dict[int, int] = defaultdict(int)
    nodes: set[int] = set()
    for src, tgt in edges:
        children[src].append(tgt)
        in_degree[tgt] += 1
        nodes.add(src)
        nodes.add(tgt)
    queue = deque(n for n in nodes if in_degree[n] == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for child in children[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
    return visited != len(nodes)


# --------------------------------------------------------------------------- #
# V005 – Masked columns in predicates                                         #
# --------------------------------------------------------------------------- #


def _check_masked_in_predicate(
    tree: exp.Expression,
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
                    if (tid, col_name) in gov_ctx.masking_rules:
                        violations.append(
                            ValidationViolation(
                                "V005",
                                f"Column {tbl_ref}.{col_name} is masked and may not appear in a WHERE or HAVING clause",
                            )
                        )
                else:
                    for tid in am.values():
                        if (tid, col_name) in gov_ctx.masking_rules:
                            violations.append(
                                ValidationViolation(
                                    "V005",
                                    f"Column {col_name!r} is masked and may not appear in a WHERE or HAVING clause",
                                )
                            )
                            break
    return violations
