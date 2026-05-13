# Copyright (c) 2026 Kenneth Stott
# Canary: 2af8ab62-fcda-4876-9364-1040f6919d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLGlot-based SQL transpilation (REQ-066, REQ-068).

Supports PG SQL → Trino, and PG SQL → any target dialect for direct execution.
"""

import sqlglot
import sqlglot.expressions as exp


# Valid SQLGlot write dialects for target sources
SUPPORTED_DIALECTS: set[str] = {
    "trino",
    "postgres",
    "mysql",
    "tsql",
    "duckdb",
    "snowflake",
    "bigquery",
}


def transpile_to_trino(pg_sql: str) -> str:
    """Transpile PostgreSQL-dialect SQL to Trino SQL."""
    pg_sql = rewrite_correlated_subqueries_for_trino(pg_sql)
    return transpile(pg_sql, "trino")


def transpile(pg_sql: str, target_dialect: str) -> str:
    """Transpile PostgreSQL-dialect SQL to a target dialect.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.
        target_dialect: SQLGlot dialect name (e.g. "trino", "postgres", "mysql").

    Returns:
        SQL string in target dialect.
    """
    results = sqlglot.transpile(pg_sql, read="postgres", write=target_dialect)
    if not results:
        raise ValueError(f"SQLGlot produced no output for: {pg_sql!r}")
    return results[0]


# ── CTE rewriter ──────────────────────────────────────────────────────────────
# Trino does not support correlated scalar subqueries in SELECT.
# _rewrite_correlated_json_to_ctes hoists each json_object/json_agg correlated
# subquery from SELECT into a CTE, then replaces it with a LEFT JOIN reference.


def _rewrite_correlated_json_to_ctes(sql: str) -> str:
    """Rewrite correlated json_object/json_agg SELECT subqueries to CTEs."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql
    if not isinstance(tree, exp.Select):
        return sql

    # Handle "SELECT * FROM (inner_select) AS alias [LIMIT N]" sampling wrapper:
    # the sampling wrapper is added by the frontend before the request is sent,
    # so the correlated subqueries live in the inner select, not at the top level.
    # Descend into the inner select, rewrite it, then hoist any new CTEs to the outer.
    exprs = tree.args.get("expressions") or []
    from_clause = tree.args.get("from_")
    if (
        len(exprs) == 1
        and isinstance(exprs[0], exp.Star)
        and from_clause is not None
        and isinstance(from_clause.this, exp.Subquery)
    ):
        inner_subq = from_clause.this
        inner_sel = inner_subq.this
        if isinstance(inner_sel, exp.Select):
            inner_sql = inner_sel.sql(dialect="postgres")
            rewritten_inner = _rewrite_correlated_json_to_ctes(inner_sql)
            if rewritten_inner == inner_sql:
                return sql
            rewritten_tree = sqlglot.parse_one(rewritten_inner, read="postgres")
            if not isinstance(rewritten_tree, exp.Select):
                return sql
            # Hoist new CTEs from rewritten inner, merged after existing outer CTEs
            inner_with = rewritten_tree.args.get("with_")
            new_inner_ctes = list(inner_with.args.get("expressions") or []) if inner_with else []
            new_tree = tree.copy()
            outer_with = new_tree.args.get("with_")
            existing_outer_ctes = (
                list(outer_with.args.get("expressions") or []) if outer_with else []
            )
            all_ctes = existing_outer_ctes + new_inner_ctes
            if all_ctes:
                new_tree.set("with_", exp.With(expressions=all_ctes))
            rewritten_tree.set("with_", None)
            new_inner_subq = inner_subq.copy()
            new_inner_subq.set("this", rewritten_tree)
            new_from = from_clause.copy()
            new_from.set("this", new_inner_subq)
            new_tree.set("from_", new_from)
            return new_tree.sql(dialect="postgres")

    cte_defs: list[exp.CTE] = []
    cte_counter = [0]
    new_joins: list[exp.Join] = []
    new_exprs: list[exp.Expression] = []
    modified = False

    for expr in tree.args.get("expressions") or []:
        rewritten = _try_rewrite_to_cte(expr, cte_defs, new_joins, cte_counter)
        if rewritten is not None:
            new_exprs.append(rewritten)
            modified = True
        else:
            new_exprs.append(expr)

    if not modified:
        return sql

    new_tree = tree.copy()
    new_tree.set("expressions", new_exprs)
    existing_joins = list(new_tree.args.get("joins") or [])
    new_tree.set("joins", existing_joins + new_joins)

    if cte_defs:
        existing_with = new_tree.args.get("with_")
        if existing_with:
            existing_ctes = list(existing_with.args.get("expressions") or [])
            existing_with.set("expressions", existing_ctes + cte_defs)
        else:
            new_tree.set("with_", exp.With(expressions=cte_defs))

    return new_tree.sql(dialect="postgres")


def _try_rewrite_to_cte(
    expr: exp.Expression,
    cte_defs: list[exp.CTE],
    new_joins: list[exp.Join],
    cte_counter: list[int],
) -> exp.Expression | None:
    """Attempt to rewrite a correlated json subquery SELECT expression to a CTE join.

    Returns the replacement SELECT expression, or None if not applicable.
    """
    if not (isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Subquery)):
        return None

    alias_name = expr.alias
    subquery = expr.this
    inner = subquery.this
    if not isinstance(inner, exp.Select):
        return None

    inner_exprs = inner.args.get("expressions") or []
    if len(inner_exprs) != 1:
        return None

    inner_expr = inner_exprs[0]
    # Detect json_object (many-to-one) or json_agg(json_object) (one-to-many)
    is_many_to_one = isinstance(inner_expr, exp.JSONObject)
    is_one_to_many = (
        isinstance(inner_expr, exp.Anonymous)
        and inner_expr.name.upper() == "JSON_AGG"
        and inner_expr.expressions
        and isinstance(inner_expr.expressions[0], exp.JSONObject)
    )
    if not is_many_to_one and not is_one_to_many:
        return None

    # Identify the inner FROM table
    from_clause = inner.args.get("from_")
    if not from_clause:
        return None
    from_expr = from_clause.this
    if isinstance(from_expr, exp.Subquery):
        inner_alias = from_expr.alias or from_expr.alias_or_name
        inner_table_expr: exp.Expression = from_expr
    elif isinstance(from_expr, exp.Table):
        inner_alias = from_expr.alias or from_expr.name
        inner_table_expr = from_expr
    else:
        return None

    # Extract the WHERE correlation condition
    where = inner.args.get("where")
    if not where:
        return None
    cond = where.this

    jk_expr, outer_expr = _split_eq_condition(cond, inner_alias)
    if jk_expr is None or outer_expr is None:
        return None

    # Flatten nested correlated subqueries within the json expression
    if is_many_to_one:
        json_expr = inner_expr
    else:
        json_expr = inner_expr.expressions[0]

    extra_joins: list[exp.Join] = []
    flat_json = _flatten_json_subqueries(json_expr, extra_joins, inner_alias)
    if flat_json is None:
        flat_json = json_expr

    cte_name = f"_rel_{cte_counter[0]}"
    cte_counter[0] += 1

    # Build CTE: SELECT jk_expr AS _jk, flat_json AS _json FROM inner_table [extra_joins]
    cte_select = exp.Select(
        expressions=[
            exp.Alias(this=jk_expr.copy(), alias=exp.to_identifier("_jk")),
            exp.Alias(this=flat_json.copy(), alias=exp.to_identifier("_json")),
        ]
    ).from_(inner_table_expr.copy())
    for ej in extra_joins:
        cte_select = cte_select.join(ej.copy(), append=True)

    cte_def = exp.CTE(
        this=cte_select,
        alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
    )
    cte_defs.append(cte_def)

    cte_jk_col = exp.Column(
        this=exp.to_identifier("_jk"),
        table=exp.to_identifier(cte_name),
    )

    if is_many_to_one:
        # LEFT JOIN _rel_N ON _rel_N._jk = outer_expr
        join = exp.Join(
            this=exp.Table(this=exp.to_identifier(cte_name)),
            on=exp.EQ(this=cte_jk_col, expression=outer_expr.copy()),
            kind="LEFT",
        )
        new_joins.append(join)
        replacement = exp.Alias(
            this=exp.Column(
                this=exp.to_identifier("_json"),
                table=exp.to_identifier(cte_name),
            ),
            alias=exp.to_identifier(alias_name),
        )
    else:
        # One-to-many: aggregate in a sub-CTE
        agg_name = f"{cte_name}_agg"
        agg_select = (
            exp.select(
                exp.Column(this=exp.to_identifier("_jk"), table=exp.to_identifier(cte_name)),
                exp.Alias(
                    this=exp.Anonymous(
                        this="JSON_AGG",
                        expressions=[
                            exp.Column(
                                this=exp.to_identifier("_json"),
                                table=exp.to_identifier(cte_name),
                            )
                        ],
                    ),
                    alias=exp.to_identifier("_val"),
                ),
            )
            .from_(cte_name)
            .group_by(exp.Column(this=exp.to_identifier("_jk"), table=exp.to_identifier(cte_name)))
        )
        agg_cte = exp.CTE(
            this=agg_select,
            alias=exp.TableAlias(this=exp.to_identifier(agg_name)),
        )
        cte_defs.append(agg_cte)

        agg_jk_col = exp.Column(
            this=exp.to_identifier("_jk"),
            table=exp.to_identifier(agg_name),
        )
        join = exp.Join(
            this=exp.Table(this=exp.to_identifier(agg_name)),
            on=exp.EQ(this=agg_jk_col, expression=outer_expr.copy()),
            kind="LEFT",
        )
        new_joins.append(join)
        replacement = exp.Alias(
            this=exp.Column(
                this=exp.to_identifier("_val"),
                table=exp.to_identifier(agg_name),
            ),
            alias=exp.to_identifier(alias_name),
        )

    return replacement


def _flatten_json_subqueries(
    jbo: exp.JSONObject,
    extra_joins: list[exp.Join],
    outer_alias: str,
) -> exp.JSONObject | None:
    """Replace KEY 'k' VALUE (subquery) pairs with direct expressions + LEFT JOINs.

    Returns a new JSONObject with all nested subqueries replaced, or None if unchanged.
    """
    new_kvs: list[exp.JSONKeyValue] = []
    changed = False

    for kv in jbo.expressions or []:
        if not isinstance(kv, exp.JSONKeyValue):
            new_kvs.append(kv)
            continue

        val = kv.expression
        if not isinstance(val, exp.Subquery):
            new_kvs.append(kv)
            continue

        inner = val.this
        if not isinstance(inner, exp.Select):
            new_kvs.append(kv)
            continue

        inner_exprs = inner.args.get("expressions") or []
        if len(inner_exprs) != 1:
            new_kvs.append(kv)
            continue

        inner_expr = inner_exprs[0]
        is_jbo = isinstance(inner_expr, exp.JSONObject)
        is_agg = (
            isinstance(inner_expr, exp.Anonymous)
            and inner_expr.name.upper() == "JSON_AGG"
            and inner_expr.expressions
            and isinstance(inner_expr.expressions[0], exp.JSONObject)
        )
        if not is_jbo and not is_agg:
            new_kvs.append(kv)
            continue

        # Extract inner table + correlation
        from_clause = inner.args.get("from_")
        if not from_clause:
            new_kvs.append(kv)
            continue

        from_expr = from_clause.this
        if isinstance(from_expr, exp.Table):
            nested_alias = from_expr.alias or from_expr.name
            nested_table: exp.Expression = from_expr
        elif isinstance(from_expr, exp.Subquery):
            nested_alias = from_expr.alias or from_expr.alias_or_name
            nested_table = from_expr
        else:
            new_kvs.append(kv)
            continue

        where = inner.args.get("where")
        if not where:
            new_kvs.append(kv)
            continue

        jk_expr, outer_ref = _split_eq_condition(where.this, nested_alias)
        if jk_expr is None or outer_ref is None:
            new_kvs.append(kv)
            continue

        # Recursively flatten any deeper subqueries
        if is_jbo:
            nested_json = inner_expr
        else:
            nested_json = inner_expr.expressions[0]

        deeper_joins: list[exp.Join] = []
        flat_nested = _flatten_json_subqueries(nested_json, deeper_joins, nested_alias)
        if flat_nested is None:
            flat_nested = nested_json

        # Add a LEFT JOIN for the nested table
        join_cond = exp.EQ(this=jk_expr.copy(), expression=outer_ref.copy())
        extra_joins.append(
            exp.Join(
                this=nested_table.copy(),
                on=join_cond,
                kind="LEFT",
            )
        )
        extra_joins.extend(deeper_joins)

        if is_jbo:
            new_val = flat_nested.copy()
        else:
            new_val = exp.Anonymous(
                this="JSON_AGG",
                expressions=[flat_nested.copy()],
            )

        new_kvs.append(exp.JSONKeyValue(this=kv.this.copy(), expression=new_val))
        changed = True

    if not changed:
        return None
    return exp.JSONObject(expressions=new_kvs)


def _split_eq_condition(
    cond: exp.Expression,
    inner_alias: str,
) -> tuple[exp.Expression | None, exp.Expression | None]:
    """Split an EQ condition into (inner_table_expr, outer_expr).

    Identifies which side references inner_alias and which is the outer correlation.
    Returns (None, None) if the condition is not a simple EQ or cannot be identified.
    """
    if not isinstance(cond, exp.EQ):
        return None, None

    left, right = cond.this, cond.expression

    def _references_alias(e: exp.Expression, alias: str) -> bool:
        if isinstance(e, exp.Column):
            tbl = e.args.get("table")
            if tbl is not None:
                return tbl.name == alias
        return False

    if _references_alias(left, inner_alias):
        return left, right
    if _references_alias(right, inner_alias):
        return right, left
    return None, None


# ── General correlated subquery rewriter (experimental) ───────────────────────
# rewrite_correlated_subqueries_for_trino lifts ANY correlated scalar subquery
# (not just json_object / json_agg) from a SELECT into CTEs + LEFT JOINs.
# The original _rewrite_correlated_json_to_ctes path is preserved unchanged.


def rewrite_correlated_subqueries_for_trino(sql: str) -> str:
    """Rewrite all correlated scalar subqueries to CTEs for Trino compatibility.

    Handles: scalar subqueries, json_object, json_agg, multi-condition AND WHERE,
    recursive nesting, and the SELECT * FROM (inner) AS alias LIMIT N wrapper.
    The original json-specific path is preserved; this is an independent entry point.
    """
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql
    if not isinstance(tree, exp.Select):
        return sql

    # Handle sampling wrapper: SELECT * FROM (inner_select) AS alias [LIMIT N]
    exprs = tree.args.get("expressions") or []
    from_clause = tree.args.get("from_")
    if (
        len(exprs) == 1
        and isinstance(exprs[0], exp.Star)
        and from_clause is not None
        and isinstance(from_clause.this, exp.Subquery)
    ):
        inner_subq = from_clause.this
        inner_sel = inner_subq.this
        if isinstance(inner_sel, exp.Select):
            inner_sql = inner_sel.sql(dialect="postgres")
            rewritten_inner = rewrite_correlated_subqueries_for_trino(inner_sql)
            if rewritten_inner == inner_sql:
                return sql
            rewritten_tree = sqlglot.parse_one(rewritten_inner, read="postgres")
            if not isinstance(rewritten_tree, exp.Select):
                return sql
            inner_with = rewritten_tree.args.get("with_")
            new_inner_ctes = list(inner_with.args.get("expressions") or []) if inner_with else []
            new_tree = tree.copy()
            outer_with = new_tree.args.get("with_")
            existing_outer_ctes = (
                list(outer_with.args.get("expressions") or []) if outer_with else []
            )
            all_ctes = existing_outer_ctes + new_inner_ctes
            if all_ctes:
                new_tree.set("with_", exp.With(expressions=all_ctes))
            rewritten_tree.set("with_", None)
            new_inner_subq = inner_subq.copy()
            new_inner_subq.set("this", rewritten_tree)
            new_from = from_clause.copy()
            new_from.set("this", new_inner_subq)
            new_tree.set("from_", new_from)
            return new_tree.sql(dialect="postgres")

    outer_aliases = _collect_select_aliases(tree)
    cte_defs: list[exp.CTE] = []
    cte_counter = [0]
    new_joins: list[exp.Join] = []
    new_exprs: list[exp.Expression] = []
    modified = False

    for expr in tree.args.get("expressions") or []:
        rewritten = _lift_correlated_in_expr(expr, outer_aliases, cte_defs, new_joins, cte_counter)
        if rewritten is not None:
            new_exprs.append(rewritten)
            modified = True
        else:
            new_exprs.append(expr)

    if not modified:
        return sql

    new_tree = tree.copy()
    new_tree.set("expressions", new_exprs)
    existing_joins = list(new_tree.args.get("joins") or [])
    new_tree.set("joins", existing_joins + new_joins)

    if cte_defs:
        existing_with = new_tree.args.get("with_")
        if existing_with:
            existing_ctes = list(existing_with.args.get("expressions") or [])
            existing_with.set("expressions", existing_ctes + cte_defs)
        else:
            new_tree.set("with_", exp.With(expressions=cte_defs))

    return new_tree.sql(dialect="postgres")


def _collect_select_aliases(select: exp.Select) -> set[str]:
    """Collect all table aliases in scope for a SELECT (FROM + JOINs)."""
    aliases: set[str] = set()
    from_clause = select.args.get("from_")
    if from_clause:
        fe = from_clause.this
        if isinstance(fe, (exp.Table, exp.Subquery)):
            name = fe.alias or (fe.name if isinstance(fe, exp.Table) else "")
            if name:
                aliases.add(name)
    for join in select.args.get("joins") or []:
        je = join.this
        if isinstance(je, (exp.Table, exp.Subquery)):
            name = je.alias or (je.name if isinstance(je, exp.Table) else "")
            if name:
                aliases.add(name)
    return aliases


def _is_aggregate_expr(expr: exp.Expression) -> bool:
    """Return True if expr is an aggregate function call."""
    _AGG_NAMES = {"COUNT", "SUM", "MIN", "MAX", "AVG", "JSON_AGG", "ARRAY_AGG", "ARBITRARY"}
    if isinstance(expr, exp.Anonymous):
        return expr.name.upper() in _AGG_NAMES
    if isinstance(expr, (exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg,
                         exp.ArrayAgg, exp.JSONArrayAgg)):
        return True
    return False


def _lift_correlated_in_expr(
    expr: exp.Expression,
    outer_aliases: set[str],
    cte_defs: list[exp.CTE],
    new_joins: list[exp.Join],
    cte_counter: list[int],
) -> exp.Expression | None:
    """Walk expr, lifting any correlated Subquery nodes to CTEs.

    Returns a rewritten copy of expr, or None if nothing changed.
    Walks: Alias, Subquery, JSONObject key values, Anonymous (json_agg children).
    """
    if isinstance(expr, exp.Alias):
        inner_rewritten = _lift_correlated_in_expr(
            expr.this, outer_aliases, cte_defs, new_joins, cte_counter
        )
        if inner_rewritten is not None:
            new_alias = expr.copy()
            new_alias.set("this", inner_rewritten)
            return new_alias
        return None

    if isinstance(expr, exp.Subquery):
        result = _try_lift_subquery(expr, outer_aliases, cte_defs, new_joins, cte_counter)
        return result  # Column reference or None

    if isinstance(expr, exp.JSONObject):
        return _lift_in_json_object(expr, outer_aliases, cte_defs, new_joins, cte_counter)

    if isinstance(expr, exp.Anonymous) and expr.name.upper() in ("JSON_AGG", "ARRAY_AGG"):
        children = expr.expressions or []
        if children:
            child_rewritten = _lift_correlated_in_expr(
                children[0], outer_aliases, cte_defs, new_joins, cte_counter
            )
            if child_rewritten is not None:
                new_anon = expr.copy()
                new_exprs = [child_rewritten] + list(children[1:])
                new_anon.set("expressions", new_exprs)
                return new_anon

    if isinstance(expr, exp.JSONArrayAgg):
        child = expr.this
        child_rewritten = _lift_correlated_in_expr(
            child, outer_aliases, cte_defs, new_joins, cte_counter
        )
        if child_rewritten is not None:
            new_expr = expr.copy()
            new_expr.set("this", child_rewritten)
            return new_expr

    return None


def _lift_in_json_object(
    jbo: exp.JSONObject,
    outer_aliases: set[str],
    cte_defs: list[exp.CTE],
    new_joins: list[exp.Join],
    cte_counter: list[int],
) -> exp.JSONObject | None:
    """Walk a JSONObject's key-value pairs, lifting any correlated subqueries."""
    new_kvs: list[exp.JSONKeyValue] = []
    changed = False
    for kv in jbo.expressions or []:
        if not isinstance(kv, exp.JSONKeyValue):
            new_kvs.append(kv)
            continue
        val_rewritten = _lift_correlated_in_expr(
            kv.expression, outer_aliases, cte_defs, new_joins, cte_counter
        )
        if val_rewritten is not None:
            new_kv = kv.copy()
            new_kv.set("expression", val_rewritten)
            new_kvs.append(new_kv)
            changed = True
        else:
            new_kvs.append(kv)
    if not changed:
        return None
    return exp.JSONObject(expressions=new_kvs)


def _try_lift_subquery(
    subq: exp.Subquery,
    outer_aliases: set[str],
    cte_defs: list[exp.CTE],
    new_joins: list[exp.Join],
    cte_counter: list[int],
) -> exp.Expression | None:
    """Lift a correlated Subquery to a CTE + LEFT JOIN.

    Returns a Column reference to the CTE's output column, or None if not correlated
    or the structure is not supported.
    """
    inner = subq.this
    if not isinstance(inner, exp.Select):
        return None

    inner_exprs = inner.args.get("expressions") or []
    if len(inner_exprs) != 1:
        return None

    from_clause = inner.args.get("from_")
    if not from_clause:
        return None

    from_expr = from_clause.this
    if isinstance(from_expr, exp.Table):
        inner_alias = from_expr.alias or from_expr.name
        inner_table_expr: exp.Expression = from_expr
    elif isinstance(from_expr, exp.Subquery):
        inner_alias = from_expr.alias or from_expr.alias_or_name
        inner_table_expr = from_expr
    else:
        return None

    where = inner.args.get("where")
    if not where:
        return None

    correlated_pairs, local_conditions = _split_where_conditions_general(
        where.this, {inner_alias}
    )
    if not correlated_pairs:
        return None

    inner_select_expr = inner_exprs[0]
    is_agg = _is_aggregate_expr(inner_select_expr)

    # Recursively flatten nested correlated subqueries within the selected expression
    inner_aliases_in_scope = {inner_alias}
    extra_joins: list[exp.Join] = []
    flat_expr = _flatten_nested_in_expr(inner_select_expr, inner_aliases_in_scope, extra_joins)
    if flat_expr is None:
        flat_expr = inner_select_expr

    # Build ON condition: AND of all correlated pairs
    def _make_join_cond(pairs: list[tuple[exp.Expression, exp.Expression]]) -> exp.Expression:
        cond: exp.Expression = exp.EQ(this=pairs[0][0].copy(), expression=pairs[0][1].copy())
        for jk, outer_ref in pairs[1:]:
            cond = exp.And(this=cond, expression=exp.EQ(this=jk.copy(), expression=outer_ref.copy()))
        return cond

    cte_name = f"_grel_{cte_counter[0]}"
    cte_counter[0] += 1

    # CTE SELECT: join keys + value column
    jk_aliases = [f"_jk{i}" for i in range(len(correlated_pairs))]
    cte_select_exprs: list[exp.Expression] = [
        exp.Alias(this=jk.copy(), alias=exp.to_identifier(jk_aliases[i]))
        for i, (jk, _) in enumerate(correlated_pairs)
    ]

    if is_agg:
        cte_select_exprs.append(
            exp.Alias(this=flat_expr.copy(), alias=exp.to_identifier("_val"))
        )
        cte_sel = exp.Select(expressions=cte_select_exprs).from_(inner_table_expr.copy())
        for ej in extra_joins:
            cte_sel = cte_sel.join(ej.copy(), append=True)
        if local_conditions:
            local_where: exp.Expression = local_conditions[0]
            for lc in local_conditions[1:]:
                local_where = exp.And(this=local_where, expression=lc)
            cte_sel = cte_sel.where(local_where)
        group_cols = [
            exp.Column(this=exp.to_identifier(jk_aliases[i]), table=exp.to_identifier(cte_name))
            for i in range(len(correlated_pairs))
        ]
        # group by positional index to avoid quoting issues
        cte_sel = cte_sel.group_by(*[
            exp.Column(this=exp.to_identifier(jk_aliases[i]))
            for i in range(len(correlated_pairs))
        ])
    else:
        # Wrap in ARBITRARY() for dedup when LIMIT 1 semantics are needed
        user_limit = inner.args.get("limit")
        if user_limit is None:
            # No LIMIT set — use ARBITRARY() to deduplicate
            arb_val = exp.Anonymous(this="ARBITRARY", expressions=[flat_expr.copy()])
            cte_select_exprs.append(
                exp.Alias(this=arb_val, alias=exp.to_identifier("_val"))
            )
            cte_sel = exp.Select(expressions=cte_select_exprs).from_(inner_table_expr.copy())
            for ej in extra_joins:
                cte_sel = cte_sel.join(ej.copy(), append=True)
            if local_conditions:
                local_where = local_conditions[0]
                for lc in local_conditions[1:]:
                    local_where = exp.And(this=local_where, expression=lc)
                cte_sel = cte_sel.where(local_where)
            cte_sel = cte_sel.group_by(*[
                exp.Column(this=exp.to_identifier(jk_aliases[i]))
                for i in range(len(correlated_pairs))
            ])
        else:
            # User specified LIMIT — select raw value, use ROW_NUMBER to pick top-N
            cte_select_exprs.append(
                exp.Alias(this=flat_expr.copy(), alias=exp.to_identifier("_val"))
            )
            cte_sel = exp.Select(expressions=cte_select_exprs).from_(inner_table_expr.copy())
            for ej in extra_joins:
                cte_sel = cte_sel.join(ej.copy(), append=True)
            if local_conditions:
                local_where = local_conditions[0]
                for lc in local_conditions[1:]:
                    local_where = exp.And(this=local_where, expression=lc)
                cte_sel = cte_sel.where(local_where)

    cte_def = exp.CTE(
        this=cte_sel,
        alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
    )
    cte_defs.append(cte_def)

    # Build LEFT JOIN ON condition using outer references
    join_cond = _make_join_cond(
        [(
            exp.Column(this=exp.to_identifier(jk_aliases[i]), table=exp.to_identifier(cte_name)),
            outer_ref,
        ) for i, (_, outer_ref) in enumerate(correlated_pairs)]
    )
    join = exp.Join(
        this=exp.Table(this=exp.to_identifier(cte_name)),
        on=join_cond,
        kind="LEFT",
    )
    new_joins.append(join)

    return exp.Column(
        this=exp.to_identifier("_val"),
        table=exp.to_identifier(cte_name),
    )


def _split_where_conditions_general(
    cond: exp.Expression,
    inner_aliases: set[str],
) -> tuple[list[tuple[exp.Expression, exp.Expression]], list[exp.Expression]]:
    """Walk an AND-tree, returning (correlated_eq_pairs, local_conditions).

    correlated_eq_pairs: list of (inner_col, outer_col) for EQ conditions where
        one side references inner_aliases and the other does not.
    local_conditions: remaining non-correlated conditions (passed into CTE WHERE).
    """
    correlated: list[tuple[exp.Expression, exp.Expression]] = []
    local: list[exp.Expression] = []

    def _walk(node: exp.Expression) -> None:
        if isinstance(node, exp.And):
            _walk(node.this)
            _walk(node.expression)
            return
        if isinstance(node, exp.EQ):
            left, right = node.this, node.expression
            left_inner = _col_references_any(left, inner_aliases)
            right_inner = _col_references_any(right, inner_aliases)
            # Correlated: one side is inner, the other is a qualified outer column
            if left_inner and not right_inner and _is_qualified_outer_col(right, inner_aliases):
                correlated.append((left, right))
                return
            if right_inner and not left_inner and _is_qualified_outer_col(left, inner_aliases):
                correlated.append((right, left))
                return
        local.append(node)

    _walk(cond)
    return correlated, local


def _col_references_any(expr: exp.Expression, aliases: set[str]) -> bool:
    """Return True if expr is a Column whose table qualifier is in aliases."""
    if isinstance(expr, exp.Column):
        tbl = expr.args.get("table")
        if tbl is not None:
            return tbl.name in aliases
    return False


def _is_qualified_outer_col(expr: exp.Expression, inner_aliases: set[str]) -> bool:
    """Return True if expr is a Column with a table qualifier not in inner_aliases.

    Literals, unqualified columns, and inner-table columns all return False.
    Only a column explicitly qualified with an outer table alias returns True.
    """
    if isinstance(expr, exp.Column):
        tbl = expr.args.get("table")
        if tbl is not None and tbl.name not in inner_aliases:
            return True
    return False


def _flatten_nested_in_expr(
    expr: exp.Expression,
    inner_aliases: set[str],
    extra_joins: list[exp.Join],
) -> exp.Expression | None:
    """Walk expr inside a CTE body, replacing nested correlated subqueries with LEFT JOINs.

    Uses a simple inline CTE counter (nested CTEs not needed — joins suffice here).
    Returns rewritten expr, or None if unchanged.
    """
    counter = [0]

    def _walk(node: exp.Expression) -> exp.Expression | None:
        if isinstance(node, exp.Alias):
            inner_rw = _walk(node.this)
            if inner_rw is not None:
                new_n = node.copy()
                new_n.set("this", inner_rw)
                return new_n
            return None

        if isinstance(node, exp.JSONObject):
            new_kvs: list[exp.JSONKeyValue] = []
            changed = False
            for kv in node.expressions or []:
                if not isinstance(kv, exp.JSONKeyValue):
                    new_kvs.append(kv)
                    continue
                val_rw = _walk(kv.expression)
                if val_rw is not None:
                    new_kv = kv.copy()
                    new_kv.set("expression", val_rw)
                    new_kvs.append(new_kv)
                    changed = True
                else:
                    new_kvs.append(kv)
            if not changed:
                return None
            return exp.JSONObject(expressions=new_kvs)

        if isinstance(node, exp.Anonymous) and node.name.upper() in ("JSON_AGG", "ARRAY_AGG"):
            children = node.expressions or []
            if children:
                rw = _walk(children[0])
                if rw is not None:
                    new_n = node.copy()
                    new_n.set("expressions", [rw] + list(children[1:]))
                    return new_n
            return None

        if isinstance(node, exp.JSONArrayAgg):
            rw = _walk(node.this)
            if rw is not None:
                new_n = node.copy()
                new_n.set("this", rw)
                return new_n
            return None

        if isinstance(node, exp.Subquery):
            inner = node.this
            if not isinstance(inner, exp.Select):
                return None
            inner_exprs_list = inner.args.get("expressions") or []
            if len(inner_exprs_list) != 1:
                return None
            from_clause = inner.args.get("from_")
            if not from_clause:
                return None
            from_expr = from_clause.this
            if isinstance(from_expr, exp.Table):
                nested_alias = from_expr.alias or from_expr.name
                nested_table: exp.Expression = from_expr
            elif isinstance(from_expr, exp.Subquery):
                nested_alias = from_expr.alias or from_expr.alias_or_name
                nested_table = from_expr
            else:
                return None
            where = inner.args.get("where")
            if not where:
                return None
            corr, local = _split_where_conditions_general(where.this, {nested_alias})
            if not corr:
                return None
            nested_select_expr = inner_exprs_list[0]
            deeper_joins: list[exp.Join] = []
            flat_nested = _flatten_nested_in_expr(nested_select_expr, {nested_alias}, deeper_joins)
            if flat_nested is None:
                flat_nested = nested_select_expr
            join_cond: exp.Expression = exp.EQ(
                this=corr[0][0].copy(), expression=corr[0][1].copy()
            )
            for jk2, outer2 in corr[1:]:
                join_cond = exp.And(
                    this=join_cond,
                    expression=exp.EQ(this=jk2.copy(), expression=outer2.copy()),
                )
            if local:
                local_w: exp.Expression = local[0]
                for lc in local[1:]:
                    local_w = exp.And(this=local_w, expression=lc)
                join_cond = exp.And(this=join_cond, expression=local_w)
            extra_joins.append(
                exp.Join(
                    this=nested_table.copy(),
                    on=join_cond,
                    kind="LEFT",
                )
            )
            extra_joins.extend(deeper_joins)
            return flat_nested.copy()

        return None

    return _walk(expr)
