# Copyright (c) 2026 Kenneth Stott
# Canary: 44032c4f-2697-47ab-96cf-e56459dff2d0
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

from provisa.compiler.sql_types import key_list
from provisa.gql.expr_ast import CypherExpr
from provisa.gql.ir import MatchClause, PathPattern
from provisa.gql.label_map import JunctionMapping, NodeMapping, RelationshipMapping


def _properties_of(var: str, exprs: "list[CypherExpr]") -> list[str]:
    """The properties ``exprs`` read of ``var`` (``var.prop``), each once, in the order written."""
    from provisa.gql.expr_ast import Property, Variable, walk

    seen: dict[str, None] = {}
    for expr in exprs:
        for node in walk(expr):
            if (
                isinstance(node, Property)
                and isinstance(node.obj, Variable)
                and node.obj.name == var
            ):
                seen.setdefault(node.name, None)
    return list(seen)


def _collects_maps(expr: "CypherExpr") -> bool:
    """Whether ``expr`` collects map literals (``collect({...})``): its value is a list of maps."""
    from provisa.gql.expr_ast import FunctionCall, MapLiteral, walk

    return any(
        isinstance(node, FunctionCall)
        and node.name.lower() == "collect"
        and bool(node.args)
        and isinstance(node.args[0], MapLiteral)
        for node in walk(expr)
    )


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
        # The table's _name_: its name as queried (compiler.context), as SQL and GraphQL read it.
        if not src_nm.query_name:
            raise ValueError(f"node {src_nm.label!r} carries no name to join its _name_ on")
        return exp.Literal.string(src_nm.query_name)
    return exp.Column(
        this=exp.Identifier(this=rm.join_source_column, quoted=True),
        table=exp.Identifier(this=src_table_ref),
    )


def edge_properties_column(rel_col: str) -> str:  # REQ-603
    """The all-relationships union's column holding one edge's own properties, beside its JSON."""
    return f"{rel_col}__properties"


def via_alias_for(rm: "RelationshipMapping", seed: "str | None") -> str:  # REQ-1586
    """Deterministic SQL alias for a junction-backed relationship's junction table.

    The seed is the relationship variable when the pattern names one, and the hop's target alias
    when it does not; both are unique within a query, so two junction hops never collide.
    """
    assert rm.via is not None, "via_alias_for called on a relationship with no junction"
    return f"{seed}__via" if seed else f"{rm.via.table_name}__via"


def junction_edge_fields(  # REQ-1586
    via: "tuple[str, JunctionMapping] | None",
) -> "list[exp.Expression]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The ``properties`` and ``junctionTable`` fields of an emitted edge object.

    A junction-backed edge exposes its junction's own columns as the edge's properties and names
    the table they were read from; the name sits beside properties rather than inside it, so it can
    never collide with a junction column. An FK/PK edge has no junction, so it carries an empty
    properties object and no table name.
    """
    if via is None:
        return [
            exp.Literal.string("properties"),
            exp.Anonymous(this="JSON_OBJECT", expressions=[]),
        ]
    via_alias, via_map = via
    prop_exprs: list[exp.Expression] = []
    for prop_name, col_name in via_map.attributes.items():
        prop_exprs.append(exp.Literal.string(prop_name))
        prop_exprs.append(via_column(via_alias, col_name))
    return [
        exp.Literal.string("properties"),
        exp.Anonymous(this="JSON_OBJECT", expressions=prop_exprs),
        exp.Literal.string("junctionTable"),
        exp.Literal.string(via_map.table_name),
    ]


def _via_table_expr(via: "JunctionMapping", alias: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Aliased table expression for a junction table (REQ-1586)."""
    return exp.alias_(  # pyright: ignore[reportReturnType]
        exp.Table(
            this=exp.Identifier(this=via.table_name, quoted=True),
            db=exp.Identifier(this=via.schema_name, quoted=True),
            catalog=exp.Identifier(this=via.catalog_name, quoted=True),
        ),
        alias=alias,
    )


def via_column(alias: str, column: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A column on the junction alias — the anchor for edge properties and edge filters."""
    return exp.Column(
        this=exp.Identifier(this=column, quoted=True),
        table=exp.Identifier(this=alias),
    )


def _and_all(conds: list[exp.Expression]) -> exp.Expression:  # REQ-1586
    """AND a non-empty list of conditions together, left to right."""
    head, *rest = conds
    for cond in rest:
        head = exp.And(this=head, expression=cond)
    return head


def _make_junction_joins(  # REQ-1586
    rm: "RelationshipMapping",
    is_bwd: bool,
    tgt_nm: "NodeMapping",
    tgt_alias: str,
    src_table_ref: str,
    join_type: str,
    via_alias: str,
) -> list[dict]:
    """Build the two join dicts a junction-backed relationship emits.

    Source hop first (source table → junction), then junction → target. The discriminator, when
    the junction carries several relationship types, is ANDed onto the source hop so it restricts
    the junction scan rather than the target scan.
    """
    via = rm.via
    assert via is not None
    # A junction edge is a plain two-key-pair join; the expression escapes exist for FK edges
    # whose join value is computed, and there is no declaration path that sets both.
    assert rm.source_expr is None and rm.target_expr is None and rm.source_constant is None, (
        f"junction relationship {rm.rel_type!r} may not also carry a computed join expression"
    )
    if is_bwd:
        src_keys, tgt_keys = via.target_columns, via.source_columns
        src_node_cols = key_list(rm.join_target_column)
        tgt_node_cols = key_list(rm.join_source_column)
    else:
        src_keys, tgt_keys = via.source_columns, via.target_columns
        src_node_cols = key_list(rm.join_source_column)
        tgt_node_cols = key_list(rm.join_target_column)
    # A composite end pairs its columns positionally, so the two lists must be the same length —
    # a mismatch is a malformed declaration, not a case to truncate around.
    if len(src_keys) != len(src_node_cols) or len(tgt_keys) != len(tgt_node_cols):
        raise ValueError(
            f"junction relationship {rm.rel_type!r} pairs {len(src_keys)} source and "
            f"{len(tgt_keys)} target junction columns against {len(src_node_cols)} and "
            f"{len(tgt_node_cols)} node columns"
        )

    src_cond: exp.Expression = _and_all(
        [
            exp.EQ(
                this=exp.Column(
                    this=exp.Identifier(this=node_col, quoted=True),
                    table=exp.Identifier(this=src_table_ref),
                ),
                expression=via_column(via_alias, via_col),
            )
            for node_col, via_col in zip(src_node_cols, src_keys, strict=True)
        ]
    )
    # An untyped hop's junction (type_value and type_values both unset) reads every row of it.
    if via.type_column is not None and (via.type_values or via.type_value is not None):
        discriminator = via_column(via_alias, via.type_column)
        src_cond = exp.And(
            this=src_cond,
            expression=(
                # An alternation's rows are of any of its types.
                exp.In(
                    this=discriminator,
                    expressions=[exp.Literal.string(v) for v in via.type_values],
                )
                if via.type_values
                else exp.EQ(this=discriminator, expression=exp.Literal.string(via.type_value))
            ),
        )
    tgt_cond = _and_all(
        [
            exp.EQ(
                this=via_column(via_alias, via_col),
                expression=exp.Column(
                    this=exp.Identifier(this=node_col, quoted=True),
                    table=exp.Identifier(this=tgt_alias),
                ),
            )
            for via_col, node_col in zip(tgt_keys, tgt_node_cols, strict=True)
        ]
    )
    return [
        {"table": _via_table_expr(via, via_alias), "on": src_cond, "join_type": join_type},
        {
            "table": _node_table_expr(tgt_nm, tgt_alias),
            "on": tgt_cond,
            "join_type": join_type,
            # The junction row it is the far end of: one relationship, two joins.
            "junction_alias": via_alias,
        },
    ]


def _make_rel_joins(
    rm: "RelationshipMapping",
    is_bwd: bool,
    tgt_nm: "NodeMapping",
    tgt_alias: str,
    src_table_ref: str,
    src_nm: "NodeMapping | None",
    join_type: str,
    rel_var: "str | None" = None,
) -> list[dict]:
    """Build the join dicts for a relationship mapping candidate.

    One dict for an FK/PK edge; two for a junction-backed edge (REQ-1586), junction first.
    """
    if rm.source_label != rm.target_label and src_nm is not None:
        is_bwd = src_nm.type_name == rm.target_label
    if rm.via is not None:
        return _make_junction_joins(
            rm,
            is_bwd,
            tgt_nm,
            tgt_alias,
            src_table_ref,
            join_type,
            via_alias_for(rm, rel_var or tgt_alias),
        )
    return [_make_rel_join(rm, is_bwd, tgt_nm, tgt_alias, src_table_ref, src_nm, join_type)]


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
        # The joined table holds the relationship's SOURCE side and the table already in the
        # statement its TARGET side. The condition is the relationship's own in either
        # direction: its source side as registered -- a constant, an expression, the column --
        # never the bare column an expression is computed from (#148).
        cond = exp.EQ(
            this=_src_col_expr_for_rm(rm, tgt_alias, tgt_nm),
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
        if canonical_fwd and canonical_bwd:
            # Self-referential: both ends hold the label, so the arrow alone picks the side.
            return backward
        return not canonical_fwd
    return backward


def _clause_vars(clause: "MatchClause") -> "set[str]":
    """The node and relationship variables a MATCH or OPTIONAL MATCH clause's pattern names."""
    if not isinstance(clause.pattern, PathPattern):
        return set()
    return {n.variable for n in clause.pattern.nodes if n.variable} | {
        r.variable for r in clause.pattern.rels if r.variable
    }


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
    step_vars: "set[str]",
    joins: "list[dict]",
) -> "tuple[list[dict], exp.Expression | None]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """An OPTIONAL MATCH's own WHERE in the ON of the last LEFT JOIN the match introduces
    (``step_vars``: its variables not bound before it). openCypher scopes the WHERE to the match,
    so every conjunct conditions it, whatever it names, and none removes a row (REQ-345): as a
    global WHERE it would turn the LEFT JOIN into an inner one. A ``_nf_`` condition stays in the
    WHERE, where nf_extractor strips it before the SQL runs.

    Returns (modified_joins, what stays in the WHERE).
    """
    order = {
        alias: i
        for i, join in enumerate(joins)
        if join["join_type"] == "LEFT" and (alias := _join_alias(join["table"])) in step_vars
    }
    if not order:
        # The OPTIONAL MATCH is the statement's FROM, not a join: there is no join to condition,
        # so its WHERE filters its own rows, as it always has here.
        return joins, where_expr
    last = max(order, key=lambda a: order[a])
    by_target: dict[str, list] = {}
    remaining: "exp.Expression | None" = None  # pyright: ignore[reportPrivateImportUsage]
    for cond in _split_and(where_expr):
        text = cond.sql(dialect="postgres")
        if "_nf_" in text:
            remaining = cond if remaining is None else exp.And(this=remaining, expression=cond)
            continue
        # The join of the last of the match's variables it names, where they are all in scope;
        # one naming none of them conditions the match's last join.
        named = [a for a in order if re.search(rf"\b{re.escape(a)}\b", text)]
        target = max(named, key=lambda a: order[a]) if named else last
        by_target.setdefault(target, []).append(cond)
    if not by_target:
        return joins, remaining
    modified: list[dict] = []
    for join in joins:
        if (target := _join_alias(join["table"])) in by_target:
            new_on = join["on"]
            for cond in by_target[target]:
                if new_on is None or (
                    hasattr(new_on, "sql") and new_on.sql() in ("TRUE", "true", "1 = 1")
                ):
                    new_on = cond
                else:
                    new_on = exp.And(this=new_on, expression=cond)
            modified.append({**join, "on": new_on})
        else:
            modified.append(join)
    return _one_join_per_junction_row(modified, set(by_target)), remaining


def _optional_step_matches_whole(joins: "list[dict]", step_vars: "set[str]") -> "list[dict]":
    """An OPTIONAL MATCH's pattern -- each of its hops and comma-separated parts, and its own WHERE
    -- matches whole or not at all: where only part of it matches, every variable it introduces is
    null (REQ-345). As plain LEFT JOINs a row would keep the hops that matched beside nulls for the
    rest. Each of the match's joins but the last holds the next as an EXISTS, built from the last
    back, so a join matches only where all of the pattern after it does."""
    span = [
        i
        for i, join in enumerate(joins)
        if join["join_type"] == "LEFT" and _join_alias(join["table"]) in step_vars
    ]
    if len(span) < 2:
        return joins
    out = list(joins)
    for k in range(span[-1] - 1, span[0] - 1, -1):
        after = out[k + 1]
        rest = exp.select(exp.Literal.number(1)).from_(after["table"].copy())
        if after["on"] is not None:
            rest = rest.where(after["on"].copy())
        on = out[k]["on"]
        whole = exp.Exists(this=rest)
        bare = on is None or (isinstance(on, exp.Boolean) and on.this is True)
        out[k] = {**out[k], "on": whole if bare else exp.And(this=on, expression=whole)}
    return out


def _one_join_per_junction_row(joins: "list[dict]", constrained: "set[str]") -> "list[dict]":
    """An optional junction-backed relationship constrained at its far end matches a junction
    row only where its far node meets the condition: the junction's join holds the far join as an
    EXISTS. As two plain LEFT JOINs, a junction row whose far node the condition refuses would
    stay, beside a null node, where the pattern has no match."""
    out: list[dict] = []
    for join in joins:
        via = join.get("junction_alias")
        alias = _join_alias(join["table"])
        if (
            via is not None
            and alias in constrained
            and join["join_type"] == "LEFT"
            and out
            and _join_alias(out[-1]["table"]) == via
            and out[-1]["join_type"] == "LEFT"
        ):
            far = exp.Exists(
                this=exp.select(exp.Literal.number(1))
                .from_(join["table"].copy())
                .where(join["on"].copy())
            )
            junction = out[-1]
            out[-1] = {**junction, "on": exp.And(this=junction["on"], expression=far)}
        out.append(join)
    return out


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
    # SPARQL's xsd:decimal, xsd:date and xsd:dateTime casts (REQ-2135)
    "TODECIMAL": ("DECIMAL", True),
    "TODATE": ("DATE", True),
    "TODATETIME": ("TIMESTAMP", True),
}

# Cypher's temporal values: from a string, a cast to the type (date('2015-07-21')); with no
# argument, the engine's current value of the type (datetime()).
_CYPHER_TEMPORAL_FNS: dict[str, tuple[str, exp.Expression]] = {
    "DATE": ("DATE", exp.CurrentDate()),
    "DATETIME": ("TIMESTAMPTZ", exp.CurrentTimestamp()),
    "LOCALDATETIME": ("TIMESTAMP", exp.Localtimestamp()),
    "TIME": ("TIMETZ", exp.CurrentTime()),
    "LOCALTIME": ("TIME", exp.Localtime()),
}

_MINUTE_ONLY = re.compile(r"((?:^|T)\d{2}:\d{2})(?=$|[-+Z])")

# SPARQL's date parts (REQ-2135): its function → the field extracted.
_DATE_PARTS: dict[str, str] = {
    "YEAR": "YEAR",
    "MONTH": "MONTH",
    "DAY": "DAY",
    "HOURS": "HOUR",
    "MINUTES": "MINUTE",
    "SECONDS": "SECOND",
}
_SPARQL_SCALARS = frozenset({*_DATE_PARTS, "STRCONCAT", "STRBEFORE", "STRAFTER"})

# SUBSTRING(str, start[, len]) needs at least the string + start-index args.
_SUBSTRING_MIN_ARGS = 2


def _str_before_after(
    name: str,
    text: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    sought: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """SPARQL's STRBEFORE / STRAFTER (REQ-2135): the text before (after) the first ``sought`` in
    ``text``, the empty string where it is not found."""
    # substr, 1-based: an exp.Substring is a Cypher substring, whose start this transform shifts.
    at = exp.StrPosition(this=text.copy(), substr=sought.copy())
    if name == "STRBEFORE":
        part = exp.Anonymous(
            this="substr",
            expressions=[
                text.copy(),
                exp.Literal.number(1),
                exp.Sub(this=at.copy(), expression=exp.Literal.number(1)),
            ],
        )
    else:
        part = exp.Anonymous(
            this="substr",
            expressions=[
                text.copy(),
                exp.Add(this=at.copy(), expression=exp.Length(this=sought.copy())),
            ],
        )
    missing = exp.EQ(this=at, expression=exp.Literal.number(0))
    return exp.Case(ifs=[exp.If(this=missing, true=exp.Literal.string(""))], default=part)


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
    # The transform does not descend into the node it is handed back, so its arguments are
    # rewritten here (toLower(x) ENDS WITH y reads right(toLower(x), ...)).
    if isinstance(node, (exp.Left, exp.Right)):
        return exp.Anonymous(
            this="left" if isinstance(node, exp.Left) else "right",
            expressions=[
                node.this.transform(_rewrite_cypher_fn_node),
                node.expression.transform(_rewrite_cypher_fn_node),
            ],
        )

    # Handle built-in exp.Substring — adjust 0-indexed Cypher start to 1-indexed SQL
    if isinstance(node, exp.Substring):
        start = node.args.get("start")
        if start is not None:
            length = node.args.get("length")
            return exp.Substring(
                this=node.this.transform(_rewrite_cypher_fn_node),
                start=exp.Add(
                    this=start.transform(_rewrite_cypher_fn_node),
                    expression=exp.Literal.number(1),
                ),
                length=length.transform(_rewrite_cypher_fn_node) if length is not None else None,
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
        # Its arguments first, as the transform does not descend into the node it is handed back
        # (collect(size(list)) counts the list).
        args = [a.transform(_rewrite_cypher_fn_node) for a in args]
        renamed = exp.Anonymous(this=_CYPHER_FN_RENAMES[name], expressions=args)
        if name == "COLLECT" and len(args) == 1:
            # collect() skips nulls, and collects nothing as [] (openCypher).
            value = args[0].expressions[0] if isinstance(args[0], exp.Distinct) else args[0]
            kept = exp.Filter(
                this=renamed,
                expression=exp.Where(
                    this=exp.Not(this=exp.Is(this=value.copy(), expression=exp.Null()))
                ),
            )
            return exp.Coalesce(this=kept, expressions=[exp.Array(expressions=[])])
        return renamed

    if name in _SPARQL_SCALARS or name in _CYPHER_CAST_FNS:
        # The transform does not descend into the node it is handed back: its arguments first
        # (toInteger(substring(x, 1)) shifts the substring's start).
        args = [a.transform(_rewrite_cypher_fn_node) for a in args]

    if name in _CYPHER_CAST_FNS and args:
        sql_type, use_try = _CYPHER_CAST_FNS[name]
        cls = exp.TryCast if use_try else exp.Cast
        return cls(this=args[0], to=exp.DataType.build(sql_type))

    if name in _CYPHER_TEMPORAL_FNS and len(args) <= 1:
        sql_type, current = _CYPHER_TEMPORAL_FNS[name]
        if not args:
            return current.copy()
        value = args[0].transform(_rewrite_cypher_fn_node)
        if isinstance(value, exp.Literal) and value.is_string:
            # A time written to the minute (16:30, 16:30+0100) is the minute's first second, which
            # an engine reads only written out (16:30:00).
            value = exp.Literal.string(_MINUTE_ONLY.sub(r"\1:00", value.this))
        return exp.Cast(this=value, to=exp.DataType.build(sql_type))

    if name in _DATE_PARTS and len(args) == 1:
        return exp.Extract(this=exp.var(_DATE_PARTS[name]), expression=args[0])

    if name == "STRCONCAT" and args:
        # SPARQL's CONCAT: unbound where any part is, as || is (an engine's concat() skips nulls).
        out = args[0]
        for arg in args[1:]:
            out = exp.DPipe(this=out, expression=arg)
        return out

    if name in ("STRBEFORE", "STRAFTER") and len(args) == 2:
        return _str_before_after(name, args[0], args[1])

    if name == "SUBSTRING" and len(args) >= _SUBSTRING_MIN_ARGS:
        # Fallback if sqlglot parsed as Anonymous instead of Substring
        start_plus_1 = exp.Add(this=args[1], expression=exp.Literal.number(1))
        return exp.Anonymous(this="substr", expressions=[args[0], start_plus_1, *args[2:]])
    return node
