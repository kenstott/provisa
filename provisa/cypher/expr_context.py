# Copyright (c) 2026 Kenneth Stott
# Canary: 41e19ad1-fcde-4522-b2c4-bb17a155431a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``TranslatorExprContext`` — resolves the scoped leaves of a Cypher expression against ``_Translator``
state, and addresses embedded-query nodes out as correlated subqueries (REQ-913).

This is the seam between the context-free expression visitor (``expr_visitor.ExprLowering``) and the
translator. It reads — never mutates, except the shared parameter registry — the translator's symbol
tables, so lowering an expression produces exactly what the old regex pipeline produced, but built as
``sqlglot.exp`` nodes. Embedded queries (``EXISTS/COUNT/COLLECT { … }``, pattern comprehensions) recurse
into a fresh ``_Translator`` that inherits the outer ``_var_table`` for correlation.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp

from provisa.cypher.expr_ast import MapProjection, PatternComprehension, SubqueryExpr

if TYPE_CHECKING:
    from provisa.cypher.translator import _Translator


# The single-hop pattern a pattern comprehension supports: (src)-[:REL]->(tgt[:Label]). Whitespace is
# permissive because the clause parser re-joins tokens with spaces (e.g. ``( p ) - [ : REL ] -> ( c )``).
_PC_RE = re.compile(
    r"\(\s*([A-Za-z_]\w*)\s*(?::[A-Za-z_]\w*)?\s*\)"
    r"\s*-\s*\[\s*(?::\s*([A-Za-z_]\w+))?\s*\]\s*->\s*"
    r"\(\s*([A-Za-z_]\w*)\s*(?::\s*([A-Za-z_]\w+))?\s*\)",
    re.IGNORECASE,
)


# Graph-aware functions resolve against traversal state (var → id column, rel type, JSON shape, …).
# Their lowering is not yet ported to the AST path, so an expression using one defers to the legacy
# path via NotImplementedError (see _Translator._lower_expr).
_GRAPH_FUNCTIONS = frozenset(
    {
        "id",
        "labels",
        "keys",
        "type",
        "exists",
        "relationships",
        "relationship",
        "nodes",
        "startnode",
        "endnode",
        "properties",
    }
)


def _anon(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


def _col(name: str, table: str) -> exp.Column:
    """A qualified column with a quoted name and an UNQUOTED table alias — matching the text path's
    ``alias."col"`` shape (the alias is a generated identifier, so quoting it only adds churn)."""
    return exp.Column(
        this=exp.to_identifier(name, quoted=True), table=exp.to_identifier(table, quoted=False)
    )


class TranslatorExprContext:
    """An ``ExprContext`` backed by a live ``_Translator``."""

    def __init__(self, translator: _Translator) -> None:
        self._t = translator

    # -- symbol leaves ----------------------------------------------------- #

    def resolve_variable(self, name: str) -> exp.Expression:
        # A CALL-subquery return var is qualified with its CROSS JOIN LATERAL alias; otherwise a bare
        # variable denotes its query-scope alias (CTE-sourced vars keep their mapped alias).
        lateral = self._t._call_var_to_lateral.get(name)
        if lateral is not None:
            return _col(name, lateral)
        alias, meta = self._t._var_table.get(name, (name, None))
        # A WITH-projected scalar (in _cte_sources, no node mapping) lives as a column named ``name``
        # inside CTE ``alias`` — resolve to that qualified column. Returning the bare CTE table alias
        # instead makes the engine read the whole-row struct (rendered client-side as [object Object]).
        if meta is None and alias != name and name in self._t._cte_sources:
            return _col(name, alias)
        return exp.column(alias)

    def resolve_property(self, obj: exp.Expression, name: str) -> exp.Expression:
        # Only a simple ``var.prop`` resolves against the symbol tables; anything else (nested access,
        # a computed object) falls through to a quoted member access on the lowered object.
        var = obj.name if isinstance(obj, exp.Column) and not obj.table else None
        t = self._t
        if var is not None:
            if var in t._all_rels_rel_vars:
                return _anon(
                    "JSON_EXTRACT_SCALAR", exp.column(var), exp.Literal.string(f"$.{name}")
                )
            if var in t._all_rels_node_vars:
                return _anon(
                    "JSON_EXTRACT_SCALAR",
                    exp.column(var),
                    exp.Literal.string(f"$.properties.{name}"),
                )
            if var in t._map_unwind_vars:
                cast = exp.Cast(
                    this=_anon("element_at", exp.column(var), exp.Literal.string(name)),
                    to=exp.DataType.build("json"),
                )
                return _anon("JSON_EXTRACT_SCALAR", cast, exp.Literal.string("$"))
            info = t._var_table.get(var)
            if info and info[1] is not None:
                sql_alias = info[1].properties.get(name)
                if sql_alias:
                    return _col(sql_alias, var)
                if name in info[1].native_filter_columns:
                    return _col(f"_nf_{name}", var)
            return _col(name, var)
        # Nested/computed object: attach the property as a quoted member of the lowered object.
        table = obj.name if isinstance(obj, exp.Column) else None
        return exp.column(name, table=table, quoted=True)

    def resolve_parameter(self, name: str) -> exp.Expression:
        t = self._t
        if name not in t._param_seen:
            t._param_order.append(name)
            t._param_seen.add(name)
        idx = t._param_order.index(name) + 1
        return exp.Parameter(this=exp.Literal.number(idx))

    def resolve_function(
        self, name: str, args: list[exp.Expression], *, distinct: bool
    ) -> exp.Expression:
        lname = name.lower()
        if lname in _GRAPH_FUNCTIONS or lname == "length":
            return self._graph_fn(lname, args)
        if lname in ("count", "collect") and len(args) == 1:
            agg = self._agg_node(lname, args[0])
            if agg is not None:
                return agg
        # Build the raw call; Cypher→engine renames/casts are applied once, post-lowering, by the same
        # AST transform the old path uses (`_rewrite_cypher_fn_node`). DISTINCT is carried on aggregates.
        call = exp.Anonymous(this=name, expressions=args)
        if distinct and args:
            call.set("expressions", [exp.Distinct(expressions=[args[0]]), *args[1:]])
        return call

    @staticmethod
    def _arg_var(arg: exp.Expression) -> str | None:
        """The variable name a graph function is applied to (its single bare-column argument)."""
        return arg.name if isinstance(arg, exp.Column) and not arg.table else None

    def _agg_node(self, fn: str, arg: exp.Expression) -> exp.Expression | None:
        """COUNT(n)/COLLECT(n) on a node variable → aggregate over its id column (COUNT dedupes, since
        JOINs can repeat a node); on a relationship variable COUNT(r) → COUNT(*). None if not a var."""
        var = self._arg_var(arg)
        if var is None:
            return None
        t = self._t
        if var in t._rel_var_types or var in t._all_rels_rel_vars:
            return exp.Count(this=exp.Star()) if fn == "count" else None
        info = t._var_table.get(var)
        if info and info[1] is not None:
            id_ref = _col(info[1].id_column, var)
            if fn == "count":
                return exp.Count(this=exp.Distinct(expressions=[id_ref]))
            return _anon("collect", id_ref)
        return None

    def _graph_fn(self, name: str, args: list[exp.Expression]) -> exp.Expression:
        """Lower a graph-aware function against traversal state, mirroring ``_rewrite_graph_fns``.
        Reuses the translator's node/edge JSON builders. Raises NotImplementedError when the variable
        is unresolved, so the whole expression falls back to the legacy path."""
        if name == "exists":  # exists(x) → x IS NOT NULL — the only fn on an arbitrary expression
            return exp.Not(this=exp.Is(this=args[0], expression=exp.Null()))
        var = self._arg_var(args[0]) if args else None
        if var is None:
            raise NotImplementedError(f"graph function {name!r} needs a variable argument")
        result = self._graph_scalar_fn(name, var)
        if result is None:
            result = self._graph_json_fn(name, var)
        if result is None:
            raise NotImplementedError(f"graph function {name!r} unresolved for {var!r}")
        return result

    def _graph_scalar_fn(self, name: str, var: str) -> exp.Expression | None:
        """Scalar/array graph functions: length, id, labels, keys, type."""
        t = self._t
        nm = (t._var_table.get(var) or (None, None))[1]
        if name == "length":
            return (
                _col("hops", "_t")
                if t._shortestpath_hops_col is not None
                else exp.Literal.number(1)
            )
        if name == "id":
            if nm is not None:
                return _col(nm.id_column, var)
            return _col("__id", var) if var in t._domain_nodes else None
        if name == "labels":
            return exp.Array(expressions=[exp.Literal.string(nm.label)]) if nm is not None else None
        if name == "keys":
            if nm is None:
                return None
            return exp.Array(expressions=[exp.Literal.string(k) for k in sorted(nm.properties)])
        if name == "type":
            rel_type = t._rel_var_types.get(var)
            return exp.Literal.string(rel_type) if rel_type is not None else None
        return None

    def _graph_json_fn(self, name: str, var: str) -> exp.Expression | None:
        """Graph functions that build a JSON node/edge object or array: relationships, nodes,
        startNode, endNode, properties."""
        t = self._t
        if name in ("relationships", "relationship"):
            steps = t._path_steps.get(var) or t._path_steps.get(t._varlen_rel_vars.get(var, ""))
            if steps is None:
                return exp.Null()
            return exp.Anonymous(
                this="JSON_ARRAY", expressions=[t._build_edge_object(*e) for e in steps[1]]
            )
        if name == "nodes":
            if var in t._path_vars and (steps := t._path_steps.get(var)) is not None:
                return exp.Anonymous(
                    this="JSON_ARRAY",
                    expressions=[t._build_node_object_expr(a, m) for a, m in steps[0]],
                )
            return None
        if name == "startnode":
            return self._path_endpoint(var, start=True)
        if name == "endnode":
            return self._path_endpoint(var, start=False)
        if name == "properties":
            return self._properties_object(var)
        return None

    def _properties_object(self, var: str) -> exp.Expression | None:
        info = self._t._var_table.get(var)
        if info is not None and info[1] is not None:
            nm, sql_alias = info[1], info[0]
            exprs: list[exp.Expression] = []
            for prop_name, col_name in nm.properties.items():
                exprs.append(exp.Literal.string(prop_name))
                exprs.append(_col(col_name, sql_alias))
            return exp.Anonymous(this="JSON_OBJECT", expressions=exprs)
        if var in self._t._rel_var_types:
            return exp.Anonymous(this="JSON_OBJECT", expressions=[])
        return None

    def _path_endpoint(self, var: str, *, start: bool) -> exp.Expression:
        """startNode(r)/endNode(r): the node-object for a relationship var's endpoint, or a path var's
        source/target node."""
        t = self._t
        endpoints = t._rel_var_endpoints.get(var)
        if endpoints:
            src_alias, src_nm, tgt_alias, tgt_nm, _ = endpoints
            return t._build_node_object_expr(
                *((src_alias, src_nm) if start else (tgt_alias, tgt_nm))
            )
        path_info = t._path_vars.get(var)
        if path_info:
            node_alias = path_info[0] if start else path_info[1]
            node_info = t._var_table.get(node_alias)
            if node_info and node_info[1] is not None:
                return t._build_node_object_expr(node_alias, node_info[1])
            return exp.Column(this=exp.Star(), table=exp.to_identifier(node_alias))
        raise NotImplementedError(f"endpoint of {var!r} unresolved")

    def resolve_label_predicate(self, operand: exp.Expression, labels: list[str]) -> exp.Expression:
        # Labels are Provisa's fixed types, and a variable's type is fixed by its MATCH binding, so a
        # label test is a compile-time constant: does the bound node's type match the requested label?
        var = operand.name if isinstance(operand, exp.Column) else None
        info = self._t._var_table.get(var) if var else None
        nm = info[1] if info else None
        if nm is None:
            # Domain-only node (no concrete mapping): compare against the resolved domain name.
            domain = self._t._domain_nodes.get(var) if var else None
            matched = domain is not None and ":".join(labels) in (domain, *labels)
            return exp.true() if matched else exp.false()
        identifiers = {nm.label, nm.type_name, nm.table_label, nm.domain_label} - {None}
        requested = ":".join(labels)
        single, qualified = 1, 2  # a label is either a bare name or domain:object_type
        matched = (
            requested in identifiers
            or (len(labels) == single and labels[0] in {nm.table_label, nm.domain_label})
            or (
                len(labels) == qualified
                and labels[0] == nm.domain_label
                and labels[1] == nm.table_label
            )
        )
        return exp.true() if matched else exp.false()

    # -- embedded queries → correlated subqueries (the recursion boundary) - #

    def resolve_subquery(self, node: SubqueryExpr) -> exp.Expression:
        inner_select = self._translate_embedded(node.body, node.kind)
        if node.kind == "EXISTS":
            return exp.Exists(this=inner_select)
        if node.kind == "COUNT":
            sub = exp.Subquery(
                this=inner_select, alias=exp.TableAlias(this=exp.to_identifier("_cnt_sub"))
            )
            counted = exp.select(exp.Count(this=exp.Star())).from_(sub)
            return exp.Subquery(this=counted)
        return _anon("ARRAY", inner_select)  # COLLECT

    def resolve_map_projection(self, node: MapProjection) -> exp.Expression:
        # n{.a, x: e, .*} → MAP(ARRAY['a','x',...], ARRAY[n."a", (e), ...]) — dotted selectors use the
        # raw property name; literal entries lower their value expression (matching the text path).
        from provisa.cypher.expr_visitor import ExprLowering

        info = self._t._var_table.get(node.var)
        nm = info[1] if info else None
        keys: list[exp.Expression] = []
        vals: list[exp.Expression] = []
        for prop in node.properties:
            keys.append(exp.Literal.string(prop))
            vals.append(_col(prop, node.var))
        if node.all_props and nm is not None:
            for prop in sorted(nm.properties.keys()):
                keys.append(exp.Literal.string(prop))
                vals.append(_col(prop, node.var))
        for key, value in node.literal_entries:
            keys.append(exp.Literal.string(key))
            vals.append(ExprLowering(self).lower(value))
        return _anon("MAP", exp.Array(expressions=keys), exp.Array(expressions=vals))

    def resolve_pattern_comprehension(self, node: PatternComprehension) -> exp.Expression:
        # [ (src)-[:REL]->(tgt) WHERE p | proj ] → a correlated ARRAY subquery over the target table,
        # joined back to src — the same "embedded query addressed out as a subquery" shape.
        from provisa.cypher.expr_visitor import ExprLowering

        m = _PC_RE.search(node.pattern)
        if m is None:
            raise NotImplementedError(f"unsupported pattern comprehension: {node.pattern!r}")
        src_var, rel_type, tgt_var, tgt_label = m.group(1), m.group(2), m.group(3), m.group(4)
        t = self._t
        rel = t._lm.relationships.get(rel_type.upper()) if rel_type else None
        if rel is None:
            src_nm = (t._var_table.get(src_var) or (None, None))[1]
            if src_nm is not None:
                cands = [
                    r
                    for r in t._lm.relationships.values()
                    if r.source_label == src_nm.type_name
                    and (not tgt_label or r.target_label == tgt_label)
                ]
                rel = cands[0] if cands else None
        if rel is None:
            raise NotImplementedError(
                f"unresolved relationship in pattern comprehension: {node.pattern!r}"
            )
        tgt_nm = t._lm.nodes.get(rel.target_label)
        if tgt_nm is None:
            raise NotImplementedError(
                f"unresolved target node in pattern comprehension: {node.pattern!r}"
            )

        # Lower the projection (and predicate) with the target var temporarily in scope.
        saved = t._var_table.get(tgt_var)
        t._var_table[tgt_var] = (tgt_var, tgt_nm)
        try:
            proj = ExprLowering(self).lower(node.projection)
            pred = ExprLowering(self).lower(node.predicate) if node.predicate is not None else None
        finally:
            if saved is None:
                t._var_table.pop(tgt_var, None)
            else:
                t._var_table[tgt_var] = saved

        src_alias = t._var_table.get(src_var, (src_var, None))[0]
        if rel.source_constant is not None:
            src_ref: exp.Expression = cast("exp.Expression", exp.convert(rel.source_constant))
        else:
            src_ref = _col(rel.join_source_column, src_alias)
        join_cond: exp.Expression = exp.EQ(
            this=src_ref,
            expression=_col(rel.join_target_column, tgt_var),
        )
        where = join_cond if pred is None else exp.And(this=join_cond, expression=pred)
        tgt_table = exp.table_(
            tgt_nm.sql_table_name, db=tgt_nm.schema_name, catalog=tgt_nm.catalog_name, quoted=True
        )
        select = exp.select(proj).from_(exp.alias_(tgt_table, tgt_var)).where(where)
        return _anon("ARRAY", select)

    # -- recursion helper -------------------------------------------------- #

    def _translate_embedded(self, body: str, kind: str) -> exp.Expression:
        """Translate an embedded Cypher body against the correlated outer scope (same recursion the
        text path used): parse, run a fresh ``_Translator`` seeded with the outer ``_var_table``,
        merge parameters back, and return the inner SELECT as an AST."""
        from provisa.cypher.parser import parse_cypher
        from provisa.cypher.translator import _Translator

        b = body.strip()
        if kind in ("EXISTS", "COUNT") and "RETURN" not in b.upper():
            b = b + " RETURN 1"
        elif kind == "COLLECT" and "RETURN" not in b.upper():
            b = b + " RETURN *"

        inner_ast = parse_cypher(b)
        inner_tr = _Translator(inner_ast, self._t._lm, self._t._params)
        inner_tr._var_table.update(self._t._var_table)
        inner_select, inner_params, _ = inner_tr.translate()

        for p in inner_params:
            if p not in self._t._param_seen:
                self._t._param_order.append(p)
                self._t._param_seen.add(p)
        return inner_select
