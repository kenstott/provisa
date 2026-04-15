# Copyright (c) 2026 Kenneth Stott
# Canary: 1cf652fa-af09-49ac-a860-b3222b8a38ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate semantic SQL → Cypher (reverse of cypher_to_sql pipeline).

Entry point: semantic_sql_to_cypher(sql, label_map, ctx) -> str | None

Only handles SELECT statements with MATCH-translatable FROM/JOIN structures.
Returns None when the SQL cannot be represented as a Cypher pattern query.
"""

from __future__ import annotations

import re
import string

import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, RelationshipMapping



def semantic_sql_to_cypher(
    semantic_sql: str,
    label_map: CypherLabelMap,
    ctx: object,
) -> str | None:
    """Convert semantic SQL to an equivalent Cypher query.

    Each node gets two labels: the type label (e.g. User) and the domain label
    (e.g. SalesData), so callers can MATCH all nodes in a domain with
    MATCH (n:SalesData).

    Args:
        semantic_sql: Semantic SQL using domain.field_name table references.
        label_map:    CypherLabelMap built from the same CompilationContext.
        ctx:          CompilationContext (used to map domain.field_name → label).

    Returns:
        Cypher string, or None if the SQL cannot be translated.
    """
    from provisa.compiler.naming import domain_to_sql_name

    try:
        tree = sqlglot.parse_one(semantic_sql, read="postgres")
    except Exception:
        return None

    if not isinstance(tree, exp.Select):
        return None

    # Build reverse lookups: (sql_domain, field_name) → node label
    domain_to_label: dict[tuple[str, str], str] = {}
    for _fn, table_meta in ctx.tables.items():  # type: ignore[attr-defined]
        type_name = table_meta.type_name
        if type_name not in label_map.nodes:
            continue
        nm = label_map.nodes[type_name]
        sql_domain = domain_to_sql_name(table_meta.domain_id)
        # Strip domain prefix from field_name — same logic as _semantic_table_ref:
        # "sa__orders" → "orders" so lookups match the parsed semantic SQL table name.
        field_key = (
            table_meta.field_name.split("__", 1)[1]
            if "__" in table_meta.field_name
            else table_meta.field_name
        )
        domain_to_label[(sql_domain, field_key)] = nm.label
        domain_to_label[("", field_key)] = nm.label

    # Build reverse lookup for relationships: (src_col, tgt_col) → RelationshipMapping
    join_to_rel: dict[tuple[str, str], RelationshipMapping] = {}
    for rel in label_map.relationships.values():
        join_to_rel[(rel.join_source_column, rel.join_target_column)] = rel
        join_to_rel[(rel.join_target_column, rel.join_source_column)] = rel

    # --- Resolve FROM clause ---
    from_clause = tree.args.get("from_")
    if from_clause is None:
        return None

    # sqlglot stores the Table directly (with alias embedded) in from_.this
    from_tbl = from_clause.this
    if not isinstance(from_tbl, exp.Table):
        return None  # subquery in FROM

    base_label = _resolve_label(from_tbl, domain_to_label)
    if base_label is None:
        return None

    sql_base_alias = from_tbl.alias or from_tbl.name

    # --- Resolve JOINs → relationship segments ---
    joins = tree.args.get("joins") or []
    # Each entry: (is_optional, rel_type | None, sql_alias, label, domain_label)
    join_segments: list[tuple[bool, str | None, str, str, str | None]] = []

    for join in joins:
        join_tbl = join.this
        if not isinstance(join_tbl, exp.Table):
            return None  # subquery join — can't translate

        tgt_label = _resolve_label(join_tbl, domain_to_label)
        if tgt_label is None:
            return None

        tgt_sql_alias = join_tbl.alias or join_tbl.name

        on_expr = join.args.get("on")
        rel_type = _rel_type_from_on(on_expr, join_to_rel)
        is_optional = (join.side or "").upper() == "LEFT"
        join_segments.append((is_optional, rel_type, tgt_sql_alias, tgt_label))

    # Build short alias map: verbose SQL alias → a, b, c, …
    _letters = list(string.ascii_lowercase)
    all_sql_aliases = [sql_base_alias] + [seg[2] for seg in join_segments]
    alias_map: dict[str, str] = {
        sql_a: _letters[i] if i < len(_letters) else f"n{i}"
        for i, sql_a in enumerate(all_sql_aliases)
    }
    base_alias = alias_map[sql_base_alias]

    def _node(short: str, label: str) -> str:
        return f"({short}:{label})"

    def _remap(text: str) -> str:
        """Replace verbose SQL aliases with short Cypher aliases."""
        # Sort longest first to avoid partial replacements
        for sql_a in sorted(alias_map, key=len, reverse=True):
            text = re.sub(rf'\b{re.escape(sql_a)}\b', alias_map[sql_a], text)
        return text

    # --- Build MATCH pattern ---
    required_path = _node(base_alias, base_label)
    for is_optional, rel_type, sql_a, label in join_segments:
        if not is_optional:
            rel_str = f"[:{rel_type}]" if rel_type else "[]"
            required_path += f"-{rel_str}->{_node(alias_map[sql_a], label)}"

    cypher_lines = [f"MATCH {required_path}"]

    for is_optional, rel_type, sql_a, label in join_segments:
        if is_optional:
            rel_str = f"[:{rel_type}]" if rel_type else "[]"
            cypher_lines.append(
                f"OPTIONAL MATCH {_node(base_alias, base_label)}"
                f"-{rel_str}->{_node(alias_map[sql_a], label)}"
            )

    # --- WHERE ---
    where_expr = tree.args.get("where")
    if where_expr:
        where_sql = _remap(_sql_to_cypher_expr(where_expr.this.sql(dialect="postgres")))
        cypher_lines.append(f"WHERE {where_sql}")

    # --- RETURN ---
    select_exprs = tree.args.get("expressions") or []
    default_sql_alias = sql_base_alias if not join_segments else None
    return_items = _build_return(select_exprs, default_sql_alias, alias_map)
    cypher_lines.append(f"RETURN {', '.join(return_items)}" if return_items else "RETURN *")

    # --- ORDER BY ---
    order = tree.args.get("order")
    if order:
        order_items = []
        for o in order.expressions:
            col_expr = o.this
            if isinstance(col_expr, exp.Column) and not col_expr.table and default_sql_alias:
                col_sql = f"{alias_map[default_sql_alias]}.{col_expr.name}"
            else:
                col_sql = _remap(_sql_to_cypher_expr(col_expr.sql(dialect="postgres")))
            direction = " DESC" if o.args.get("desc") else ""
            order_items.append(f"{col_sql}{direction}")
        cypher_lines.append(f"ORDER BY {', '.join(order_items)}")

    # --- SKIP / LIMIT ---
    offset = tree.args.get("offset")
    limit = tree.args.get("limit")
    if offset:
        cypher_lines.append(f"SKIP {offset.this.sql()}")
    if limit:
        cypher_lines.append(f"LIMIT {limit.this.sql()}")

    return "\n".join(cypher_lines)


# --- Helpers ---

def _resolve_label(
    tbl: exp.Table,
    domain_to_label: dict[tuple[str, str], str],
) -> str | None:
    """Map a sqlglot Table node to a Cypher node label using the domain lookup."""
    db = tbl.db or ""
    name = tbl.name or ""
    return domain_to_label.get((db, name)) or domain_to_label.get(("", name))


def _rel_type_from_on(
    on_expr: exp.Expression | None,
    join_to_rel: dict[tuple[str, str], RelationshipMapping],
) -> str | None:
    """Extract Cypher relationship type from a JOIN ON condition."""
    if on_expr is None:
        return None
    for eq in on_expr.find_all(exp.EQ):
        left, right = eq.this, eq.expression
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lc = left.name
            rc = right.name
            rel = join_to_rel.get((lc, rc)) or join_to_rel.get((rc, lc))
            if rel:
                return rel.rel_type
    return None


def _build_return(
    select_exprs: list[exp.Expression],
    default_sql_alias: str | None = None,
    alias_map: dict[str, str] | None = None,
) -> list[str]:
    """Convert SELECT expressions to RETURN items."""
    am = alias_map or {}

    def _short(sql_tbl: str) -> str:
        return am.get(sql_tbl, sql_tbl)

    items = []
    for expr in select_exprs:
        if isinstance(expr, exp.Star):
            return ["*"]
        if isinstance(expr, exp.Column):
            raw_tbl = expr.table or default_sql_alias or ""
            tbl = _short(raw_tbl)
            col = expr.name or "*"
            if col == "*":
                items.append(tbl if tbl else "*")
            else:
                items.append(f"{tbl}.{col}" if tbl else col)
        elif isinstance(expr, exp.Alias):
            raw = _sql_to_cypher_expr(expr.this.sql(dialect="postgres"))
            for sql_a in sorted(am, key=len, reverse=True):
                raw = re.sub(rf'\b{re.escape(sql_a)}\b', am[sql_a], raw)
            items.append(f"{raw} AS {expr.alias}")
        else:
            raw = _sql_to_cypher_expr(expr.sql(dialect="postgres"))
            for sql_a in sorted(am, key=len, reverse=True):
                raw = re.sub(rf'\b{re.escape(sql_a)}\b', am[sql_a], raw)
            items.append(raw)
    return items or ["*"]


def _offset_aliases(cypher: str, offset: int) -> tuple[str, int]:
    """Rename single-letter node aliases to start at the given letter offset.

    Aliases are discovered from MATCH pattern definitions (e.g. ``(a:Label)``),
    then every word-boundary occurrence is substituted.  Returns the rewritten
    Cypher and the next available offset.
    """
    letters = list(string.ascii_lowercase)
    defined = sorted(set(re.findall(r'\(([a-z])(?:[:\s)])', cypher)))
    rename = {
        old: letters[offset + i] if (offset + i) < len(letters) else f"n{offset + i}"
        for i, old in enumerate(defined)
    }
    result = cypher
    for old in sorted(rename, key=len, reverse=True):
        result = re.sub(rf'\b{re.escape(old)}\b', rename[old], result)
    return result, offset + len(defined)


def combine_cypher_queries(cyphers: list[str]) -> str:
    """Combine independent per-root Cypher queries into a single CALL {} query.

    Each query is wrapped in a CALL {} subquery; the outer RETURN collects all
    projected items so callers get a single unified query.  Aliases are
    offset per subquery so no two roots share the same alias letter.
    """
    if len(cyphers) == 1:
        return cyphers[0]

    wrapped: list[str] = []
    all_return_items: list[str] = []
    alias_offset = 0

    for cypher in cyphers:
        renamed, alias_offset = _offset_aliases(cypher, alias_offset)
        lines = renamed.strip().splitlines()
        # Extract RETURN items to re-expose them in the outer RETURN
        for line in lines:
            stripped = line.strip()
            if re.match(r"RETURN\s+", stripped, re.IGNORECASE):
                items_str = re.sub(r"^RETURN\s+", "", stripped, flags=re.IGNORECASE)
                if items_str.strip() != "*":
                    all_return_items.extend(
                        item.strip() for item in items_str.split(",")
                    )
                break
        indented = "\n".join(f"  {l}" for l in lines)
        wrapped.append(f"CALL {{\n{indented}\n}}")

    combined = "\n".join(wrapped)
    if all_return_items:
        combined += f"\nRETURN {', '.join(all_return_items)}"
    else:
        combined += "\nRETURN *"

    return combined


def _sql_to_cypher_expr(sql_expr: str) -> str:
    """Minimally rewrite a SQL expression fragment to Cypher syntax."""
    # Remove double-quote wrapping from identifiers (sqlglot emits them)
    result = re.sub(r'"(\w+)"', r'\1', sql_expr)
    result = result.replace(" ILIKE ", " =~ ")
    result = result.replace("TRUE", "true").replace("FALSE", "false")
    return result
