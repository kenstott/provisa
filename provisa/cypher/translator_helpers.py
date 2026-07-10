# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Free-function AST/expression helpers for the Cypher translator (REQ-345, REQ-409).

Join/column expression builders, OPTIONAL-MATCH where-folding, and Cypher
expression/string rewrites. Extracted from translator.py; leaf module (no _Translator).
"""

from __future__ import annotations

import re

import sqlglot.expressions as exp

from provisa.cypher.parser import (
    MatchClause,
    PathPattern,
)
from provisa.cypher.label_map import NodeMapping, RelationshipMapping


def _safe_alias(expr: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", expr)


def _const_literal(v: int | str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v)


def _node_table_expr(nm: "NodeMapping", alias: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build an aliased table expression for a NodeMapping.

    When physical column names differ from SQL aliases (e.g. breedName vs breed_name),
    wraps the physical table in a subquery: SELECT *, "phys" AS "sql_alias" FROM table.
    This keeps physical column names accessible for JOIN conditions while the outer SQL
    can reference SQL aliases throughout — preserving governance on the outer query.
    """
    phys_table = exp.Table(
        this=exp.Identifier(this=nm.sql_table_name, quoted=True),
        db=exp.Identifier(this=nm.schema_name, quoted=True),
        catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
    )
    alias_exprs = [
        exp.alias_(
            exp.Column(this=exp.Identifier(this=phys, quoted=True)),
            sql_al,
        )
        for cql, sql_al in nm.properties.items()
        if (phys := nm.physical_properties.get(cql)) and phys != sql_al
    ]
    if not alias_exprs:
        return exp.alias_(phys_table, alias=alias)  # pyright: ignore[reportReturnType]
    subq = exp.Select(expressions=[exp.Star(), *alias_exprs]).from_(phys_table)
    return exp.alias_(exp.Subquery(this=subq), alias=alias)  # pyright: ignore[reportReturnType]


def _tgt_col_expr_for_rm(rm: "RelationshipMapping", alias: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build the target column expression for a RelationshipMapping."""
    if rm.target_expr is not None:
        return exp.maybe_parse(
            rm.target_expr.replace("{alias}", alias),
            dialect="postgres",
        )
    return exp.Column(
        this=exp.Identifier(this=rm.join_target_column, quoted=True),
        table=exp.Identifier(this=alias),
    )


def _src_col_expr_for_rm(
    rm: "RelationshipMapping",
    src_table_ref: str,
    src_nm: "NodeMapping | None",
) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build the source column expression for a RelationshipMapping (forward direction)."""
    if rm.source_constant is not None:
        return _const_literal(rm.source_constant)
    if rm.source_expr is not None:
        return exp.maybe_parse(
            rm.source_expr.replace("{alias}", src_table_ref),
            dialect="postgres",
        )
    if rm.join_source_column == "_name_" and src_nm is not None:
        from provisa.compiler.naming import domain_to_sql_name as _d2s

        _name_val = f"{_d2s(src_nm.domain_id or src_nm.schema_name or '')}.{src_nm.table_name}"
        return exp.Literal.string(_name_val)
    return exp.Column(
        this=exp.Identifier(this=rm.join_source_column, quoted=True),
        table=exp.Identifier(this=src_table_ref),
    )


def _make_rel_join(
    rm: "RelationshipMapping",
    is_bwd: bool,
    tgt_nm: "NodeMapping",
    tgt_alias: str,
    src_table_ref: str,
    src_nm: "NodeMapping | None",
    join_type: str,
) -> dict:
    """Build a join dict for a single relationship mapping candidate."""
    jt = _node_table_expr(tgt_nm, tgt_alias)
    # The join condition between two fixed tables is identical regardless of which way
    # the pattern is traversed; orientation is determined by which label the source
    # table holds, not the traversal flag. For an undirected pattern the candidate
    # resolver emits a backward variant without swapping src_nm/tgt_nm, so is_bwd alone
    # would place the source column on the target table (REQ-575 regression). Recompute
    # from labels for non-self-referential rels; keep is_bwd for self-refs (same label
    # on both sides, where direction genuinely selects the column pair).
    if rm.source_label != rm.target_label and src_nm is not None:
        is_bwd = src_nm.type_name == rm.target_label
    if is_bwd:
        if rm.source_constant is not None:
            cond = exp.EQ(
                this=_const_literal(rm.source_constant),
                expression=_tgt_col_expr_for_rm(rm, src_table_ref),
            )
        else:
            cond = exp.EQ(
                this=exp.Column(
                    this=exp.Identifier(this=rm.join_source_column, quoted=True),
                    table=exp.Identifier(this=tgt_alias),
                ),
                expression=_tgt_col_expr_for_rm(rm, src_table_ref),
            )
    else:
        src_col = _src_col_expr_for_rm(rm, src_table_ref, src_nm)
        tgt_col = _tgt_col_expr_for_rm(rm, tgt_alias)
        cond = exp.EQ(this=src_col, expression=tgt_col)
    return {"table": jt, "on": cond, "join_type": join_type}


def _is_bwd_for_candidate(
    rm: "RelationshipMapping",
    bidir: bool,
    backward: bool,
    src_nm: "NodeMapping | None",
    tgt_nm: "NodeMapping | None",
    tgt_nm_explicit: bool,
) -> "bool | None":
    """Determine backward-ness for a relationship mapping candidate.

    Returns None if the candidate should be filtered out (direction mismatch).
    """
    if bidir:
        if src_nm is not None:
            canonical_fwd = rm.source_label == src_nm.type_name
            canonical_bwd = rm.target_label == src_nm.type_name
            chains_from_tgt = tgt_nm is not None and rm.source_label == tgt_nm.type_name
            if not canonical_fwd and not canonical_bwd and not chains_from_tgt:
                return None
            return rm.source_label != src_nm.type_name
        return False
    if src_nm is not None:
        canonical_fwd = rm.source_label == src_nm.type_name
        canonical_bwd = rm.target_label == src_nm.type_name
        chains_from_tgt = tgt_nm is not None and rm.source_label == tgt_nm.type_name
        if not canonical_fwd and not canonical_bwd and not chains_from_tgt:
            return None
        if tgt_nm is not None and tgt_nm_explicit:
            # For non-self-referential rels, backward on a fwd-canonical rel is invalid
            # and forward on a bwd-only rel is invalid.  For self-referential rels
            # (canonical_fwd AND canonical_bwd both true), both directions are valid.
            if backward and canonical_fwd and not canonical_bwd:
                return None
            if not backward and not canonical_fwd and canonical_bwd:
                return None
        return not canonical_fwd
    return backward


def _optional_vars(clauses: "list[MatchClause]") -> "set[str]":
    """Return variables first introduced by OPTIONAL MATCH (not already bound by MATCH)."""
    seen: set[str] = set()
    optional_only: set[str] = set()
    for clause in clauses:
        if not isinstance(clause.pattern, PathPattern):
            continue
        new_in_clause = set()
        for node in clause.pattern.nodes:
            if node.variable:
                new_in_clause.add(node.variable)
        for rel in clause.pattern.rels:
            if rel.variable:
                new_in_clause.add(rel.variable)
        if clause.optional:
            optional_only.update(new_in_clause - seen)
        seen.update(new_in_clause)
    return optional_only


def _join_alias(table_expr: "exp.Expression") -> "str | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Extract the SQL alias from a join table expression."""
    alias = getattr(table_expr, "alias", None)
    return str(alias) if alias else None


def _split_and(expr: "exp.Expression") -> "list[exp.Expression]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Flatten top-level AND conjuncts into a list."""
    if isinstance(expr, exp.And):
        return _split_and(expr.this) + _split_and(expr.expression)
    return [expr]


def _fold_where_into_optional_joins(
    where_expr: "exp.Expression",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    optional_vars: "set[str]",
    where_text: str,
    joins: "list[dict]",
) -> "tuple[list[dict], exp.Expression | None]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Fold WHERE conditions referencing optional variables into the relevant LEFT JOIN ON clauses.

    Cypher semantics: WHERE after OPTIONAL MATCH constrains the optional pattern.
    In SQL this must be an ON condition, not a global WHERE — a global WHERE turns
    a LEFT JOIN into an implicit INNER JOIN, filtering out rows where the optional
    variable is NULL and eliminating the base MATCH rows.

    Each AND conjunct is assigned only to the LEFT JOIN that introduces its
    last-referenced optional variable, so a condition on variable `c` is never
    placed in an earlier join (e.g. `b`) where `c` is not yet in scope.

    Returns (modified_joins, remaining_where_or_None).
    """
    referenced = {v for v in optional_vars if re.search(rf"\b{re.escape(v)}\b", where_text)}
    if not referenced:
        return joins, where_expr

    # Build position map for LEFT JOIN aliases so we can find the "last" one.
    join_order: dict[str, int] = {}
    for i, join in enumerate(joins):
        alias = _join_alias(join["table"])
        if alias:
            join_order[alias] = i

    # Split into individual AND conjuncts, route each to the appropriate join.
    conjuncts = _split_and(where_expr)
    alias_to_conjuncts: dict[str, list] = {}
    remaining_conjuncts: list = []

    for cond in conjuncts:
        cond_text = cond.sql(dialect="postgres")
        # _nf_ conditions must stay in WHERE so nf_extractor can strip them before SQL execution.
        if "_nf_" in cond_text:
            remaining_conjuncts.append(cond)
            continue
        refs = {v for v in optional_vars if re.search(rf"\b{re.escape(v)}\b", cond_text)}
        refs_in_joins = refs & set(join_order.keys())
        if refs_in_joins:
            target = max(refs_in_joins, key=lambda v: join_order[v])
            alias_to_conjuncts.setdefault(target, []).append(cond)
        else:
            remaining_conjuncts.append(cond)

    modified: list[dict] = []
    for join in joins:
        alias = _join_alias(join["table"])
        if alias in alias_to_conjuncts and join["join_type"] == "LEFT":
            existing_on = join["on"]
            new_on = existing_on
            for cond in alias_to_conjuncts[alias]:
                if new_on is None or (
                    hasattr(new_on, "sql") and new_on.sql() in ("TRUE", "true", "1 = 1")
                ):
                    new_on = cond
                else:
                    new_on = exp.And(this=new_on, expression=cond)
            modified.append({**join, "on": new_on})
        else:
            modified.append(join)

    remaining: "exp.Expression | None" = None  # pyright: ignore[reportPrivateImportUsage]
    for cond in remaining_conjuncts:
        remaining = cond if remaining is None else exp.And(this=remaining, expression=cond)

    return modified, remaining


def _is_bare_variable(expr: str) -> bool:
    return bool(re.match(r"^[A-Za-z_]\w*$", expr.strip()))


_CYPHER_DQUOTE_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _rewrite_cypher_dquote_strings(expr: str) -> str:  # REQ-410
    """Cypher ``"str"`` → SQL ``'str'``, leaving quoted identifiers (after ``.``) untouched. Retained
    for the UNWIND text path + REQ-410; the AST grammar handles this on the predicate paths (REQ-913)."""
    result: list[str] = []
    pos = 0
    for m in _CYPHER_DQUOTE_RE.finditer(expr):
        start = m.start()
        result.append(expr[pos:start])
        keep = start > 0 and expr[start - 1] == "."  # property name after `.` — leave as-is
        result.append(m.group(0) if keep else "'" + m.group(1).replace("'", "\\'") + "'")
        pos = m.end()
    result.append(expr[pos:])
    return "".join(result)


_ISO_TS_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)'")


def _coerce_ts_literals(text: str) -> str:  # REQ-409
    """Wrap ISO-datetime string literals as ``TIMESTAMP '...'`` (retained for REQ-409)."""
    return _ISO_TS_LITERAL_RE.sub(lambda m: f"TIMESTAMP {m.group(0)}", text)


def _rewrite_property_access(expr: str) -> str:
    """Rewrite n.prop → n."prop" for SQL."""
    return re.sub(
        r"\b([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)\b",
        lambda m: f'{m.group(1)}."{m.group(2)}"',
        expr,
    )


# ---------------------------------------------------------------------------
# Cypher → SQL function mapping
# ---------------------------------------------------------------------------

# Simple name renames: Cypher fn (uppercase) → SQL fn name
_CYPHER_FN_RENAMES: dict[str, str] = {
    "TOLOWER": "lower",
    "TOUPPER": "upper",
    "LTRIM": "ltrim",
    "RTRIM": "rtrim",
    "TRIM": "trim",
    "REVERSE": "reverse",
    "REPLACE": "replace",
    "SPLIT": "split",
    "RANGE": "sequence",  # Cypher range(start, end[, step]) → sequence(start, end[, step])
    "LOG": "ln",  # Neo4j log() = natural log = the engine ln()
    "LOG2": "log2",
    "COLLECT": "array_agg",
    "STDEV": "stddev_samp",
    "STDEVP": "stddev_pop",
    "PERCENTILECONT": "approx_percentile",
    "PERCENTILEDISC": "approx_percentile",
}

# Cast functions: Cypher fn (uppercase) → (sql_type, use_try_cast)
_CYPHER_CAST_FNS: dict[str, tuple[str, bool]] = {
    "TOSTRING": ("VARCHAR", False),
    "TOSTRINGORNULL": ("VARCHAR", True),
    "TOINTEGER": ("BIGINT", True),
    "TOINTEGERORNULL": ("BIGINT", True),
    "TOFLOAT": ("DOUBLE", True),
    "TOFLOATORNULL": ("DOUBLE", True),
    "TOBOOLEAN": ("BOOLEAN", True),
    "TOBOOLEANORNULL": ("BOOLEAN", True),
}

# SUBSTRING(str, start[, len]) needs at least the string + start-index args.
_SUBSTRING_MIN_ARGS = 2


def _rewrite_cypher_fn_node(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """SQLGlot transform: rewrite Cypher function names to SQL equivalents."""
    # Cypher last(list) → element_at(list, -1) — SQLGlot parses last() as exp.Last
    if isinstance(node, exp.Last):
        inner = node.args.get("this")
        if inner is not None:
            return exp.Anonymous(this="element_at", expressions=[inner, exp.Literal.number(-1)])
        return node

    # log(x) in Cypher = natural log → the engine ln(x)
    # exp.Log with one arg (no base) is natural log in Cypher
    if isinstance(node, exp.Log):
        base = node.args.get("this")
        value = node.args.get("expression")
        # sqlglot Log: Log(this=base, expression=value) for log(base, value)
        # single-arg log(x) → Log(this=x, expression=None) or similar
        if value is None:
            # single argument — natural log
            return exp.Anonymous(this="ln", expressions=[base])
        return node

    # exp.Left / exp.Right — SQLGlot parses left()/right() as these; emit as Anonymous
    # so the engine receives LEFT(str, n) rather than a SUBSTRING expansion.
    if isinstance(node, exp.Left):
        return exp.Anonymous(this="left", expressions=[node.this, node.expression])

    if isinstance(node, exp.Right):
        return exp.Anonymous(this="right", expressions=[node.this, node.expression])

    # Handle built-in exp.Substring — adjust 0-indexed Cypher start to 1-indexed SQL
    if isinstance(node, exp.Substring):
        start = node.args.get("start")
        if start is not None:
            return exp.Substring(
                this=node.this,
                start=exp.Add(this=start, expression=exp.Literal.number(1)),
                length=node.args.get("length"),
            )
        return node

    if not isinstance(node, exp.Anonymous):
        return node
    name = node.name.upper()
    args: list[exp.Expression] = node.args.get("expressions") or []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    if name == "HEAD" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(1)])

    if name == "LAST" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(-1)])

    if name == "TAIL" and args:
        return exp.Anonymous(
            this="slice",
            expressions=[
                args[0],
                exp.Literal.number(2),
                exp.Anonymous(this="cardinality", expressions=[args[0]]),
            ],
        )

    if name == "ISEMPTY" and args:
        return exp.EQ(
            this=exp.Anonymous(this="cardinality", expressions=args),
            expression=exp.Literal.number(0),
        )

    if name == "SIZE" and args:
        arg = args[0]
        if isinstance(arg, exp.Literal) and arg.is_string:
            return exp.Anonymous(this="char_length", expressions=args)
        return exp.Anonymous(this="cardinality", expressions=args)

    if name in _CYPHER_FN_RENAMES:
        return exp.Anonymous(this=_CYPHER_FN_RENAMES[name], expressions=args)

    if name in _CYPHER_CAST_FNS and args:
        sql_type, use_try = _CYPHER_CAST_FNS[name]
        cls = exp.TryCast if use_try else exp.Cast
        return cls(this=args[0], to=exp.DataType.build(sql_type))

    if name == "SUBSTRING" and len(args) >= _SUBSTRING_MIN_ARGS:
        # Fallback if sqlglot parsed as Anonymous instead of Substring
        start_plus_1 = exp.Add(this=args[1], expression=exp.Literal.number(1))
        return exp.Anonymous(this="substr", expressions=[args[0], start_plus_1, *args[2:]])
    return node
