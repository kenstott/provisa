# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4c1a2e-7d3b-4e9f-a1c6-2b5d8e0f4a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Metric query expansion (REQ-1317) and metric-composed view generation (REQ-1318).

A metric is a named aggregate with no grain of its own (``provisa.core.models.Metric``).
This module closes the grain:

- ``expand_metric_query`` rewrites a raw-SQL query against the reserved ``metrics``
  schema (``SELECT region, value FROM metrics.net_revenue``) into the real grouped
  aggregate over the underlying semantic tables, BEFORE governance, so RLS/masking
  apply to the real columns (REQ-1317).
- ``generate_view_metrics_sql`` compiles a declarative ``ViewMetricsSpec`` into the
  view's SELECT (REQ-1318).
- ``expand_metric_calls_in_sql`` inlines ``metric('name')`` scalar calls inside
  free-hand view SQL (REQ-1318).

Pure: callers pass the metric registry, the table registry (name → {"id", "columns"}),
and the relationship registry; no state access here. Every unresolvable reference is a
hard, named error — never a silent passthrough.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

import sqlglot
from sqlglot import Expr
from sqlglot import expressions as exp

if TYPE_CHECKING:
    from provisa.core.models import Metric, ViewMetricsSpec

# The reserved schema name a metric query addresses (REQ-1317).
METRICS_SCHEMA = "metrics"

_DIALECT = "postgres"

# Bare SQL identifier — metric names are snake_case (Metric._valid_name) and dimension
# names are semantic column names; anything else is rejected before SQL assembly.
_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


def metric_semantic_sql(
    name: str, dimensions: Sequence[str], filters: str | None = None
) -> str:  # REQ-1319
    """The semantic-SQL addressing form for a metric at the requested grain.

    ``SELECT <dims>, value FROM metrics.<name> [WHERE <filters>] GROUP BY <dims>``
    (no dims → ``SELECT value FROM metrics.<name>``). Every identifier is validated
    as a bare SQL identifier so callers (Bolt CALL args, NL resolution, Flight
    tickets) cannot inject SQL through names; ``filters`` is a raw boolean SQL
    string governed like any user predicate by the pipeline downstream.
    """
    for ident in (name, *dimensions):
        if not _IDENT_RE.fullmatch(ident):
            raise ValueError(f"invalid metric identifier {ident!r}")
    where = f" WHERE {filters}" if filters else ""
    if not dimensions:
        return f"SELECT value FROM {METRICS_SCHEMA}.{name}{where}"
    dims = ", ".join(dimensions)
    return f"SELECT {dims}, value FROM {METRICS_SCHEMA}.{name}{where} GROUP BY {dims}"


def _parse_expression(metric_name: str, expression: str) -> Expr:
    """Parse a metric's aggregate expression; a malformed definition is a hard error."""
    try:
        return sqlglot.parse_one(expression, read=_DIALECT)
    except Exception as exc:
        raise ValueError(
            f"metric {metric_name!r}: unparseable expression {expression!r}: {exc}"
        ) from exc


def _expression_tables(metric_name: str, parsed: Expr) -> list[str]:
    """The semantic tables a metric expression references, in first-reference order."""
    seen: list[str] = []
    for col in parsed.find_all(exp.Column):
        if not col.table:
            raise ValueError(
                f"metric {metric_name!r}: column {col.name!r} in expression must be "
                "table-qualified (semantic reference like 'orders.amount')"
            )
        if col.table not in seen:
            seen.append(col.table)
    if not seen:
        raise ValueError(f"metric {metric_name!r}: expression references no semantic columns")
    return seen


def metric_reference_tables(metric_name: str, expression: str) -> list[str]:  # REQ-1319
    """The semantic tables a metric expression references, in first-reference order.

    Public wrapper over the expansion path's reference extraction so schema projection
    (the GraphQL ``metrics`` block) shares one parser with query expansion (REQ-1317).
    """
    return _expression_tables(metric_name, _parse_expression(metric_name, expression))


def inline_metric_sql(  # REQ-1319
    metric_name: str,
    expression: str,
    table_name: str,
    resolve_column: Callable[[str], str],
) -> str:
    """Rewrite a single-table metric expression into physical, unqualified SQL.

    Every column reference must be qualified with ``table_name``; each is replaced by
    the quoted physical column ``resolve_column`` returns, so the result drops straight
    into the single-table aggregate SELECT the GraphQL aggregate compiler builds. A
    reference to any other table is a hard error — the aggregate compiler has no join
    builder, so a multi-table metric cannot be inlined here.
    """
    parsed = _parse_expression(metric_name, expression)
    for col in list(parsed.find_all(exp.Column)):
        if col.table != table_name:
            raise ValueError(
                f"metric {metric_name!r}: expression references table {col.table!r}; "
                f"only references to {table_name!r} can be inlined into its aggregate"
            )
        physical = resolve_column(col.name)
        col.replace(exp.column(exp.to_identifier(physical, quoted=True)))
    return parsed.sql(dialect=_DIALECT)


def _table_entry(tables: Mapping[str, Any], name: str, *, context: str) -> Mapping[str, Any]:
    entry = tables.get(name)
    if entry is None:
        raise ValueError(f"{context}: unknown semantic table {name!r}")
    return entry


def _table_columns(entry: Mapping[str, Any]) -> set[str]:
    return set(entry["columns"])


def _name_by_id(tables: Mapping[str, Any]) -> dict[Any, str]:
    return {entry["id"]: name for name, entry in tables.items()}


class _JoinPlan:
    """Accumulates the FROM root plus derived one-hop joins (fact → dimension)."""

    def __init__(
        self,
        root: str,
        tables: Mapping[str, Any],
        relationships: Sequence[Mapping[str, Any]],
    ) -> None:
        self.root = root
        self._tables = tables
        self._rels = relationships
        self._by_id = _name_by_id(tables)
        # table name → ON condition SQL; the root has no join.
        self.joins: dict[str, str] = {}

    def _rel_between(self, a: str, b: str) -> tuple[str, str] | None:
        """A registered relationship between tables ``a`` and ``b`` (either direction).

        Returns (a_column, b_column) or None."""
        a_id = self._tables[a]["id"]
        b_id = self._tables[b]["id"]
        for rel in self._rels:
            if rel["source_table_id"] == a_id and rel["target_table_id"] == b_id:
                return rel["source_column"], rel["target_column"]
            if rel["source_table_id"] == b_id and rel["target_table_id"] == a_id:
                return rel["target_column"], rel["source_column"]
        return None

    def require_table(self, name: str, *, context: str) -> None:
        """Ensure ``name`` participates in the FROM clause, joining it to the root."""
        if name == self.root or name in self.joins:
            return
        pair = self._rel_between(self.root, name)
        if pair is None:
            raise ValueError(
                f"{context}: no registered relationship joins {name!r} to {self.root!r}"
            )
        root_col, other_col = pair
        self.joins[name] = f"{self.root}.{root_col} = {name}.{other_col}"

    def resolve_dimension(self, dim: str, base_tables: list[str], *, context: str) -> str:
        """Resolve a dimension name to a qualified column, joining one hop if needed.

        A dimension is valid when it is a column of a metric base table, or a column of
        a table one registered relationship hop away from a base table (fact → dimension).
        Ambiguous or unresolvable dimensions are hard errors (REQ-1317)."""
        direct = [t for t in base_tables if dim in _table_columns(self._tables[t])]
        if len(direct) > 1:
            raise ValueError(
                f"{context}: dimension {dim!r} is ambiguous across tables {sorted(direct)}"
            )
        if direct:
            return f"{direct[0]}.{dim}"

        hops: list[tuple[str, str, str, str]] = []  # (base, target, base_col, target_col)
        for base in base_tables:
            base_id = self._tables[base]["id"]
            for rel in self._rels:
                if rel["source_table_id"] != base_id:
                    continue
                target = self._by_id.get(rel["target_table_id"])
                if target is None:
                    continue
                if dim in _table_columns(self._tables[target]):
                    hops.append((base, target, rel["source_column"], rel["target_column"]))
        if not hops:
            raise ValueError(
                f"{context}: dimension {dim!r} is not a column of the metric's tables "
                f"{sorted(base_tables)} nor reachable via one relationship hop"
            )
        if len({(h[1], h[3]) for h in hops}) > 1:
            raise ValueError(
                f"{context}: dimension {dim!r} is ambiguous — reachable via multiple "
                f"relationships ({sorted({h[1] for h in hops})})"
            )
        base, target, base_col, target_col = hops[0]
        if target not in self.joins and target != self.root:
            self.joins[target] = f"{base}.{base_col} = {target}.{target_col}"
        return f"{target}.{dim}"

    def build_select(
        self,
        select_items: list[str],
        group_items: list[str],
        where_sql: str | None,
    ) -> exp.Select:
        out = exp.select(*select_items).from_(self.root)
        for name, on in self.joins.items():
            out = out.join(name, on=on, join_type="inner")
        if where_sql:
            out = out.where(where_sql)
        if group_items:
            out = out.group_by(*group_items)
        return out


def _metric_base_tables(
    named: list[tuple[str, Expr]],
    tables: Mapping[str, Any],
    *,
    context: str,
) -> list[str]:
    """Union of semantic tables referenced across metric expressions, validated."""
    base: list[str] = []
    for name, parsed in named:
        for t in _expression_tables(name, parsed):
            _table_entry(tables, t, context=f"{context}: metric {name!r}")
            if t not in base:
                base.append(t)
    return base


def expand_metric_query(  # REQ-1317
    tree: Expr,
    metrics: dict[str, "Metric"],
    tables: Mapping[str, Any],
    relationships: Sequence[Mapping[str, Any]],
) -> Expr | None:
    """Expand a query against the reserved ``metrics`` schema into the real aggregate.

    Returns None when ``tree`` does not reference the ``metrics`` schema (caller keeps
    the original statement). Otherwise returns a new SELECT over the underlying semantic
    tables: the caller's non-``value`` selected columns become the dimension set (and
    GROUP BY), ``value`` (or the metric's own name) becomes the metric expression, and
    the caller's WHERE passes through. Unknown metric or unresolvable dimension is a
    hard ValueError naming the offender.
    """
    if not isinstance(tree, exp.Select):
        return None
    from_clause = tree.args.get("from_") or tree.find(exp.From)
    if from_clause is None:
        return None
    if isinstance(from_clause.this, exp.Subquery):
        # UI sampling wraps the caller's query in `SELECT * FROM (<inner>) _sample LIMIT n`
        # (see SqlPage.tsx) — the metrics.<name> reference lives one level down.
        inner = from_clause.this.this
        expanded_inner = expand_metric_query(inner, metrics, tables, relationships)
        if expanded_inner is None:
            return None
        from_clause.this.set("this", expanded_inner)
        return tree
    if not isinstance(from_clause.this, exp.Table):
        return None
    src = from_clause.this
    if src.db != METRICS_SCHEMA:
        return None

    metric_name = src.name
    metric = metrics.get(metric_name)
    if metric is None:
        raise ValueError(f"unknown metric {metric_name!r} (metrics.{metric_name})")

    parsed_expr = _parse_expression(metric_name, metric.expression)
    base_tables = _metric_base_tables([(metric_name, parsed_expr)], tables, context="metric query")
    plan = _JoinPlan(base_tables[0], tables, relationships)
    for extra in base_tables[1:]:
        plan.require_table(extra, context=f"metric {metric_name!r}")

    relation_names = {metric_name, src.alias} - {""}

    select_items: list[str] = []
    group_items: list[str] = []
    for proj in tree.expressions:
        if isinstance(proj, exp.Star):
            raise ValueError(
                f"metrics.{metric_name}: SELECT * is not supported — name the "
                "dimension columns and 'value' explicitly"
            )
        col = proj.this if isinstance(proj, exp.Alias) else proj
        if not isinstance(col, exp.Column):
            raise ValueError(
                f"metrics.{metric_name}: only plain column references are supported "
                f"in the select list, got {proj.sql(dialect=_DIALECT)!r}"
            )
        out_alias = proj.alias if isinstance(proj, exp.Alias) else col.name
        if col.name in ("value", metric_name):
            select_items.append(f"{parsed_expr.sql(dialect=_DIALECT)} AS {out_alias}")
        else:
            qualified = plan.resolve_dimension(
                col.name, base_tables, context=f"metrics.{metric_name}"
            )
            select_items.append(f"{qualified} AS {out_alias}")
            group_items.append(qualified)

    where_sql: str | None = None
    where = tree.args.get("where")
    if where is not None:
        cond = where.this.copy()
        # Strip qualifiers that address the virtual metrics relation; the columns
        # resolve against the real tables after expansion.
        for c in cond.find_all(exp.Column):
            if c.table in relation_names:
                c.set("table", None)
        where_sql = cond.sql(dialect=_DIALECT)

    out = plan.build_select(select_items, group_items, where_sql)
    limit = tree.args.get("limit")
    if limit is not None:
        out.set("limit", limit.copy())
    order = tree.args.get("order")
    if order is not None:
        out.set("order", order.copy())
    return out


def generate_view_metrics_sql(  # REQ-1318
    spec: "ViewMetricsSpec",
    metrics: dict[str, "Metric"],
    tables: Mapping[str, Any],
    relationships: Sequence[Mapping[str, Any]],
) -> str:
    """Compile a declarative metric-composed view spec into its SELECT statement.

    One SELECT carrying every spec metric as a column named by the metric, the spec
    dimensions as grouped output columns, and the spec filters ANDed into WHERE.
    Unknown metric or unresolvable dimension is a hard ValueError naming the offender.
    """
    named: list[tuple[str, Expr]] = []
    for name in spec.metrics:
        metric = metrics.get(name)
        if metric is None:
            raise ValueError(f"view_metrics: unknown metric {name!r}")
        named.append((name, _parse_expression(name, metric.expression)))

    base_tables = _metric_base_tables(named, tables, context="view_metrics")
    plan = _JoinPlan(base_tables[0], tables, relationships)
    for extra in base_tables[1:]:
        plan.require_table(extra, context="view_metrics")

    select_items: list[str] = []
    group_items: list[str] = []
    for dim in spec.dimensions:
        qualified = plan.resolve_dimension(dim, base_tables, context="view_metrics")
        select_items.append(f"{qualified} AS {dim}")
        group_items.append(qualified)
    for name, parsed in named:
        select_items.append(f"{parsed.sql(dialect=_DIALECT)} AS {name}")

    where_sql: str | None = None
    if spec.filters:
        conds = []
        for f in spec.filters:
            try:
                conds.append(sqlglot.parse_one(f, read=_DIALECT))
            except Exception as exc:
                raise ValueError(f"view_metrics: unparseable filter {f!r}: {exc}") from exc
        combined: Expr = conds[0]
        for c in conds[1:]:
            combined = exp.and_(combined, c)
        where_sql = combined.sql(dialect=_DIALECT)

    return plan.build_select(select_items, group_items, where_sql).sql(dialect=_DIALECT)


def expand_metric_calls_in_sql(  # REQ-1318
    sql: str, metrics: dict[str, "Metric"]
) -> tuple[str, list[str]]:
    """Inline every ``metric('name')`` scalar call in free-hand view SQL.

    Each call is replaced by the metric's (parenthesized) expression text. Returns the
    rewritten SQL and the referenced metric names, in first-reference order. SQL with
    no ``metric()`` call is returned untouched. Unknown metric name is a hard ValueError.
    """
    try:
        tree = sqlglot.parse_one(sql, read=_DIALECT)
    except Exception as exc:
        raise ValueError(f"view_sql: SQL parse error: {exc}") from exc

    referenced: list[str] = []
    hit = False
    for node in list(tree.find_all(exp.Anonymous)):
        if node.name.lower() != "metric":
            continue
        args = node.expressions
        if len(args) != 1 or not (isinstance(args[0], exp.Literal) and args[0].is_string):
            raise ValueError(
                f"metric() takes exactly one string literal argument, got "
                f"{node.sql(dialect=_DIALECT)!r}"
            )
        name = args[0].this
        metric = metrics.get(name)
        if metric is None:
            raise ValueError(f"view_sql: unknown metric {name!r} in metric() call")
        node.replace(exp.paren(_parse_expression(name, metric.expression)))
        if name not in referenced:
            referenced.append(name)
        hit = True

    if not hit:
        return sql, []
    return tree.sql(dialect=_DIALECT), referenced
