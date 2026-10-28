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

import copy
from collections import ChainMap
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp

from provisa.gql.select_builder import NODE_KEY_PROPERTY, node_key
from provisa.gql.expr_ast import (
    CypherExpr,
    MapProjection,
    Parameter,
    PatternComprehension,
    Property,
    SubqueryExpr,
    Term,
    Variable,
)
from provisa.gql.translator_types import CypherTranslateError, map_key_column
from provisa.gql.translator_helpers import (  # REQ-1586
    _make_junction_joins,
    edge_properties_column,
    via_alias_for,
)
from provisa.gql.translator_helpers import via_column as _via_column  # REQ-1586

if TYPE_CHECKING:
    from provisa.gql.ir import CypherAST, PathPattern
    from provisa.gql.translator import _Translator


def _comprehension_hop(pattern: PathPattern) -> tuple[str, str | None, str, str | None] | None:
    """``(source var, rel type, target var, target label)`` of the first hop of ``pattern`` a
    pattern comprehension reads: ``(a[:L])-[[:T]]->(b[:L])``, both ends named, one label or none
    each, the relationship unnamed, of one type or none, one hop to the right, and nothing inline."""
    for i, rel in enumerate(pattern.rels):
        src, tgt = pattern.nodes[i], pattern.nodes[i + 1]
        if (
            src.variable
            and tgt.variable
            and len(src.labels) <= 1
            and len(tgt.labels) <= 1
            and not src.properties
            and not tgt.properties
            and rel.variable is None
            and len(rel.types) <= 1
            and rel.direction == "right"
            and not rel.variable_length
            and not rel.properties
        ):
            return (
                src.variable,
                rel.types[0] if rel.types else None,
                tgt.variable,
                (tgt.labels[0] if tgt.labels else None),
            )
    return None


#: The Cypher functions whose value is a string.
_STRING_FUNCTIONS = frozenset(
    {
        "tostring",
        "tostringornull",
        "toupper",
        "tolower",
        "upper",
        "lower",
        "trim",
        "ltrim",
        "rtrim",
        "replace",
        "substring",
        "left",
        "right",
        "type",
    }
)

_TEXT_TYPES = ("text", "varchar", "char", "character", "string", "citext", "nvarchar", "nchar")


#: The functions whose value is a list.
_LIST_FUNCTIONS = frozenset(
    {"range", "tail", "keys", "labels", "nodes", "relationships", "collect", "split"}
)


def is_text_type(registered: str) -> bool:
    """Whether a registered column type holds text (varchar(64), character varying, ...)."""
    return registered.strip().lower().startswith(_TEXT_TYPES)


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

    def resolve_term(self, node: Term) -> exp.Expression:
        return self._t._term_sql(node)

    def resolve_variable(self, name: str) -> exp.Expression:
        # REQ-2135: a variable a triple binds is the column of its term.
        term = self._t._term_vars.get(name)
        if term is not None:
            return term.copy()
        # A CALL-subquery return var is qualified with its CROSS JOIN LATERAL alias; otherwise a bare
        # variable denotes its query-scope alias (CTE-sourced vars keep their mapped alias).
        lateral = self._t._call_var_to_lateral.get(name)
        if lateral is not None:
            return _col(name, lateral)
        # An UNWIND-of-collect variable names the UNNEST column itself (``UNNEST(a) AS _uw0(node)``),
        # so it stays a bare identifier — resolving it to the join alias would strip the name
        # resolve_property needs to spell the map access (element_at(node, 'prop')).
        if name in self._t._map_unwind_vars:
            return exp.column(name)
        alias, meta = self._t._var_table.get(name, (name, None))
        # A WITH-projected scalar (in _cte_sources, no node mapping) lives as a column named ``name``
        # inside CTE ``alias`` — resolve to that qualified column. Returning the bare CTE table alias
        # instead makes the engine read the whole-row struct (rendered client-side as [object Object]).
        if meta is None and alias != name and name in self._t._cte_sources:
            return _col(name, alias)
        # An UNWIND variable is its UNNEST's column, not the alias's whole row (a struct).
        if meta is None and alias in self._t._unwind_aliases:
            return _col(name, alias)
        return exp.column(alias)

    def resolve_property(self, obj: exp.Expression, name: str) -> exp.Expression:
        # Only a simple ``var.prop`` resolves against the symbol tables; anything else (nested access,
        # a computed object) falls through to a quoted member access on the lowered object.
        var = obj.name if isinstance(obj, exp.Column) and not obj.table else None
        t = self._t
        if var is not None:
            if var in t._all_rels_rel_vars:
                # The edge's own properties, never its JSON: that carries an id built from both
                # endpoints' keys (translator_union._build_all_rels_union).
                return _anon(
                    "JSON_EXTRACT_SCALAR",
                    exp.column(edge_properties_column(var)),
                    exp.Literal.string(f"$.{name}"),
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
                typed = t._map_unwind_types.get(var, {}).get(name)
                if typed is not None:
                    # A key every map gives a value of one type: that value, not its text.
                    return exp.Cast(this=cast, to=exp.DataType.build(typed))
                return _anon("JSON_EXTRACT_SCALAR", cast, exp.Literal.string("$"))
            # REQ-1586: an attribute of a junction-backed relationship is a column of the junction
            # row, so r.attr anchors on the junction alias — in RETURN, in expressions, and in a
            # WHERE, where it becomes a predicate on the join rather than on assembled rows.
            via_bound = t._rel_var_via.get(var)
            if via_bound is not None:
                via_alias, via_map = via_bound
                col_name = via_map.attributes.get(name)
                if col_name is not None:
                    return _via_column(via_alias, col_name)
                if via_map.type_column is not None and name == via_map.type_attribute:
                    # The row's own type, as the junction stores it.
                    return _via_column(via_alias, via_map.type_column)
                # A property the relationship has not is null, as a node's is.
                return exp.Null()
            if var in t._rel_var_types:
                # An FK/PK relationship has no properties.
                return exp.Null()
            info = t._var_table.get(var)
            if info and info[1] is not None:
                nm = info[1]
                if name == NODE_KEY_PROPERTY:
                    return node_key(info[0], nm)
                sql_alias = nm.properties.get(name)
                if sql_alias:
                    return _col(sql_alias, info[0])
                if name in nm.native_filter_columns:
                    return _col(f"_nf_{name}", info[0])
                # A property the label has not -- one it does not publish, or the role may not
                # see -- is null, as a graph's missing property is: a column is never read by a
                # name the label does not give it.
                return exp.Null()
            return _col(name, info[0] if info else var)
        if isinstance(obj, exp.Column) and obj.table and obj.name in t._map_vars:
            # A map literal a WITH carries: its key is the column that WITH carries it in (#287).
            if name not in t._map_vars[obj.name]:
                return exp.Null()
            return exp.column(
                map_key_column(obj.name, t._map_vars[obj.name][name]), table=obj.table, quoted=True
            )
        if isinstance(obj, exp.Column) and obj.name in t._map_key_maps:
            # A map literal inside a map a WITH carries: its key's value, from its JSON.
            cast = exp.Cast(
                this=_anon("element_at", obj.copy(), exp.Literal.string(name)),
                to=exp.DataType.build("json"),
            )
            return _anon("JSON_EXTRACT_SCALAR", cast, exp.Literal.string("$"))
        # Nested/computed object: attach the property as a quoted member of the lowered object.
        table = obj.name if isinstance(obj, exp.Column) else None
        return exp.column(name, table=table, quoted=True)

    def property_is_table_column(self, obj: exp.Expression, name: str) -> bool:
        """True exactly where :meth:`resolve_property` answers with a registered table's (or a
        junction's) own column."""
        var = obj.name if isinstance(obj, exp.Column) and not obj.table else None
        t = self._t
        if (
            var is None
            or var in t._all_rels_rel_vars
            or var in t._all_rels_node_vars
            or var in t._map_unwind_vars
            or var in t._map_vars
        ):
            return False
        via_bound = t._rel_var_via.get(var)
        if via_bound is not None:
            return via_bound[1].attributes.get(name) is not None or (
                via_bound[1].type_column is not None and name == via_bound[1].type_attribute
            )
        info = t._var_table.get(var)
        if info and info[1] is not None and name == NODE_KEY_PROPERTY:
            # A single key is the table's own column; a compound one is a string built from it.
            return len(info[1].key_columns) <= 1
        return bool(info and info[1] is not None and info[1].properties.get(name))

    def property_type(self, obj: exp.Expression, name: str) -> str | None:
        """The registered data type of ``var.name`` where ``var`` is bound to one registered table
        (REQ-778), or the type a node drawn from several tables reads it as; None where the
        property is no such table's column."""
        var = obj.name if isinstance(obj, exp.Column) and not obj.table else None
        domain = self._t._domain_nodes.get(var) if var else None
        if domain is not None:
            # A node drawn from several tables: the one type its UNION reads the property as.
            from provisa.gql.translator_union import union_labels, union_type

            held = union_type(name, union_labels(self._t._lm, domain), self._t._lm)
            return held.sql(dialect="postgres") if held is not None else None
        if not self.property_is_table_column(obj, name):
            return None
        info = self._t._var_table.get(obj.name)
        if not info or info[1] is None:
            return None
        return dict(info[1].property_types).get(name)

    def temporal_kind(self, expr: CypherExpr) -> str | None:
        """The temporal kind ``expr`` has (REQ-2173), or None."""
        from provisa.gql import temporal

        return temporal.kind_of(
            expr, ChainMap(self._t._temporal_kinds, self._t._param_kinds), self._property_kind
        )

    def temporal_element_kind(self, expr: CypherExpr) -> str | None:
        """The temporal kind every item of a list literal has (an UNWIND's variable), or None."""
        from provisa.gql import temporal

        return temporal.element_kind(
            expr, ChainMap(self._t._temporal_kinds, self._t._param_kinds), self._property_kind
        )

    def is_text(self, expr: CypherExpr) -> bool:
        """Whether ``expr`` is a string: a string literal, a string function's value, a
        concatenation with a string, a registered text column, or a variable bound to one."""
        from provisa.gql.expr_ast import Binary, FunctionCall, Literal, Paren

        if isinstance(expr, Paren):
            return self.is_text(expr.inner)
        if isinstance(expr, Literal):
            return expr.kind == "string"
        if isinstance(expr, Variable):
            return expr.name in self._t._text_vars
        if isinstance(expr, Parameter):
            # The value the client gives it, a string.
            return expr.name in self._t._params and isinstance(self._t._params[expr.name], str)
        if isinstance(expr, Property):
            if not isinstance(expr.obj, Variable):
                return False
            registered = self.property_type(exp.column(expr.obj.name), expr.name)
            return registered is not None and is_text_type(registered)
        if isinstance(expr, FunctionCall):
            name = expr.name.lower()
            if name in _STRING_FUNCTIONS or name.startswith("apoc.text."):
                return True
            # reverse() of a string is a string, of a list a list.
            return name == "reverse" and len(expr.args) == 1 and self.is_text(expr.args[0])
        if isinstance(expr, Binary) and expr.op == "+":
            return self.is_text(expr.left) or self.is_text(expr.right)
        return False

    def is_list(self, expr: CypherExpr) -> bool:
        """Whether ``expr`` is a list as the query writes it: a list literal, a comprehension, a
        slice, a function answering a list, a list + anything, a CASE every branch of which is a
        list, or a variable bound to one."""
        from provisa.gql.expr_ast import (
            Binary,
            Case,
            FunctionCall,
            ListComprehension,
            ListLiteral,
            Paren,
            Slice,
        )

        if isinstance(expr, Paren):
            return self.is_list(expr.inner)
        if isinstance(expr, (ListLiteral, ListComprehension, Slice)):
            return True
        if isinstance(expr, Variable):
            return expr.name in self._t._list_vars
        if isinstance(expr, Parameter):
            # The value the client gives it, a list.
            return expr.name in self._t._params and isinstance(self._t._params[expr.name], list)
        if isinstance(expr, FunctionCall):
            name = expr.name.lower()
            if name == "reverse":
                return len(expr.args) == 1 and self.is_list(expr.args[0])
            return name in _LIST_FUNCTIONS
        if isinstance(expr, Binary) and expr.op == "+":
            return self.is_list(expr.left) or self.is_list(expr.right)
        if isinstance(expr, Case):
            branches = [then for _, then in expr.whens]
            if expr.default is not None:
                branches.append(expr.default)
            return all(self.is_list(b) for b in branches)
        return False

    def given_value(self, expr: CypherExpr) -> tuple[bool, object]:
        name = None
        if isinstance(expr, Parameter):
            name = expr.name
        elif isinstance(expr, Variable):
            name = self._t._param_vars.get(expr.name)
        if name is None or name not in self._t._params:
            return False, None
        return True, self._t._params[name]

    def is_null(self, expr: CypherExpr) -> bool:
        from provisa.gql.expr_ast import Literal

        if isinstance(expr, Literal):
            return expr.kind == "null"
        return isinstance(expr, Variable) and expr.name in self._t._null_vars

    def is_json_value(self, expr: CypherExpr) -> bool:
        from provisa.gql.expr_ast import Index, Paren

        while isinstance(expr, Paren):
            expr = expr.inner
        if isinstance(expr, Index):
            # An item of a variable bound to a list of several types.
            return isinstance(expr.obj, Variable) and expr.obj.name in self._t._json_list_vars
        return isinstance(expr, Variable) and expr.name in self._t._json_value_vars

    def holds_list_items(self, expr: CypherExpr) -> bool:
        """Whether every item of a list literal is a list (an UNWIND's variable)."""
        from provisa.gql.expr_ast import ListLiteral

        return (
            isinstance(expr, ListLiteral)
            and bool(expr.items)
            and all(self.is_list(i) for i in expr.items)
        )

    def holds_text_items(self, expr: CypherExpr) -> bool:
        """Whether every item of a list literal is a string (an UNWIND's variable)."""
        from provisa.gql.expr_ast import ListLiteral

        return (
            isinstance(expr, ListLiteral)
            and bool(expr.items)
            and all(self.is_text(i) for i in expr.items)
        )

    def temporal_zone(self, expr: CypherExpr) -> str | None:
        """The zone a time or date time the query builds names (REQ-2173), or None."""
        from provisa.gql import temporal

        return temporal.zone_of(
            expr, ChainMap(self._t._temporal_zones, self._t._param_zones), self.temporal_kind
        )

    def temporal_element_zone(self, expr: CypherExpr) -> str | None:
        """The zone every item of a list literal names (an UNWIND's variable), or None."""
        from provisa.gql.expr_ast import ListLiteral

        if not isinstance(expr, ListLiteral) or not expr.items:
            return None
        zones = {self.temporal_zone(item) for item in expr.items}
        return zones.pop() if len(zones) == 1 else None

    def _property_kind(self, node: Property) -> str | None:
        """A registered column's temporal kind, by its registered type."""
        from provisa.gql import temporal

        if not isinstance(node.obj, Variable):
            return None
        registered = self.property_type(exp.column(node.obj.name), node.name)
        return temporal.kind_of_type(registered) if registered else None

    def resolve_parameter(self, name: str) -> exp.Expression:
        from provisa.gql import spatial, temporal

        t = self._t
        if name not in t._param_seen:
            t._param_order.append(name)
            t._param_seen.add(name)
        idx = t._param_order.index(name) + 1
        placeholder = exp.Parameter(this=exp.Literal.number(idx))
        # REQ-2173: a temporal value is bound as its text (temporal.bindable), read as its kind;
        # a point as its list.
        held = t._param_kinds.get(temporal.parameter_key(name))
        if held is None:
            return placeholder
        held_as = spatial.SQL_TYPE if held == spatial.POINT else temporal.SQL_TYPES[held]
        return exp.Cast(this=placeholder, to=exp.DataType.build(held_as))

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
        if lname == "group_concat":
            # REQ-2135: SPARQL's GROUP_CONCAT, each engine's own string aggregate over the values'
            # text, the separator its second argument. Ordered by the text, since an engine
            # (Trino's LISTAGG) requires an order, and so the string is the same each time.
            if distinct:
                raise CypherTranslateError("GROUP_CONCAT(DISTINCT ...) is not supported")
            text = exp.Cast(this=args[0], to=exp.DataType.build("VARCHAR"))
            return exp.GroupConcat(
                this=exp.Order(this=text, expressions=[exp.Ordered(this=text.copy())]),
                separator=args[1],
            )
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
        if args and isinstance(args[0], exp.Null):
            return exp.Null()  # nodes(null), length(null): null (openCypher)
        var = self._arg_var(args[0]) if args else None
        result = None
        if var is not None:
            result = self._graph_scalar_fn(name, var)
            if result is None:
                result = self._graph_json_fn(name, var)
        if result is None and name == "keys" and len(args) == 1:
            # keys() of a map -- a literal, a parameter, a variable bound to one, or null -- is the
            # map's own keys, in order (null for null).
            return _anon("map_keys", args[0])
        if var is None:
            raise NotImplementedError(f"graph function {name!r} needs a variable argument")
        if result is None:
            raise NotImplementedError(f"graph function {name!r} unresolved for {var!r}")
        return result

    def _graph_scalar_fn(self, name: str, var: str) -> exp.Expression | None:
        """Scalar/array graph functions: length, id, labels, keys, type."""
        t = self._t
        nm = (t._var_table.get(var) or (None, None))[1]
        if name == "length":
            # A recursive walk counts its hops (#309); shortestPath reads the same column.
            recursive = var in t._path_vars and t._path_vars[var][2]
            return (
                _col("hops", "_t")
                if t._shortestpath_hops_col is not None or recursive
                else exp.Literal.number(1)
            )
        if name == "id":
            if nm is not None:
                return _col(nm.id_column, var)
            return _col("__id", var) if var in t._domain_nodes else None
        if name == "labels":
            if nm is not None:
                return exp.Array(expressions=[exp.Literal.string(nm.label)])
            # A node matched with no label: the label of the table its row is from.
            return exp.Array(expressions=[_col("__label", var)]) if var in t._domain_nodes else None
        if name == "keys":
            if nm is None:
                # REQ-1586: keys(r) on a junction-backed edge lists its attribute names.
                via_bound = t._rel_var_via.get(var)
                if via_bound is not None:
                    return exp.Array(
                        expressions=[exp.Literal.string(k) for k in sorted(via_bound[1].attributes)]
                    )
                return None
            return exp.Array(expressions=[exp.Literal.string(k) for k in sorted(nm.properties)])
        if name == "type":
            named = t._rel_var_type_exprs.get(var)
            if named is not None:
                return named.copy()  # an alternation: the type its row's discriminator names
            rel_type = t._rel_var_types.get(var)
            if rel_type is None:
                return None
            from provisa.gql.translator import _type_literal

            # Marked with its variable: an extra UNION branch names its own type (_of_branch).
            return _type_literal(var, rel_type)
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
            # REQ-1586: a junction-backed edge's properties are the junction row's own columns.
            via_bound = self._t._rel_var_via.get(var)
            if via_bound is not None:
                via_alias, via_map = via_bound
                via_exprs: list[exp.Expression] = []
                for prop_name, col_name in via_map.attributes.items():
                    via_exprs.append(exp.Literal.string(prop_name))
                    via_exprs.append(_via_column(via_alias, col_name))
                return exp.Anonymous(this="JSON_OBJECT", expressions=via_exprs)
            return exp.Anonymous(this="JSON_OBJECT", expressions=[])
        return None

    def _path_endpoint(self, var: str, *, start: bool) -> exp.Expression:
        """startNode(r)/endNode(r): the node-object for a relationship var's endpoint, or a path var's
        source/target node."""
        t = self._t
        endpoints = t._rel_var_endpoints.get(var)
        if endpoints:
            src_alias, src_nm, tgt_alias, tgt_nm, is_reversed = endpoints
            disp_start = (tgt_alias, tgt_nm) if is_reversed else (src_alias, src_nm)
            disp_end = (src_alias, src_nm) if is_reversed else (tgt_alias, tgt_nm)
            return t._build_node_object_expr(*(disp_start if start else disp_end))
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

    def resolve_graph_element(self, name: str) -> exp.Expression | None:
        t = self._t
        endpoints = t._rel_var_endpoints.get(name)
        if endpoints is not None and name in t._rel_var_types:
            src_alias, src_nm, tgt_alias, tgt_nm, is_reversed = endpoints
            return t._build_edge_object(
                t._rel_var_type_exprs.get(name) or t._rel_var_types[name],
                src_alias,
                src_nm,
                tgt_alias,
                tgt_nm,
                is_reversed,
                t._rel_var_via.get(name),
            )
        alias, nm = t._var_table.get(name, (name, None))
        if nm is not None:
            from provisa.gql.select_builder import node_json

            return node_json(alias, nm)
        return None

    def resolve_graph_identity(
        self, name: str
    ) -> tuple[tuple[str, ...], list[exp.Expression]] | None:
        t = self._t
        endpoints = t._rel_var_endpoints.get(name)
        if endpoints is not None and name in t._rel_var_types:
            src_alias, src_nm, tgt_alias, tgt_nm, is_reversed = endpoints
            if is_reversed:
                # Its ends as the relationship is registered: source first.
                src_alias, src_nm, tgt_alias, tgt_nm = tgt_alias, tgt_nm, src_alias, src_nm
            if name in t._rel_var_type_exprs:
                raise CypherTranslateError(
                    f"{name} is one of several relationship types; compare it by type(...) and "
                    "its ends"
                )
            kind = ("rel", t._rel_var_types[name], src_nm.table_name, tgt_nm.table_name)
            return kind, [node_key(src_alias, src_nm), node_key(tgt_alias, tgt_nm)]
        if name in t._rel_var_types:
            return None
        alias, nm = t._var_table.get(name, (name, None))
        if nm is None:
            return None
        kind = ("node", nm.catalog_name, nm.schema_name, nm.table_name)
        return kind, [node_key(alias, nm)]

    def resolve_graph_absent(self, name: str) -> exp.Expression | None:
        t = self._t
        if name in t._rel_var_types:
            matched = t._rel_var_matched.get(name)
            if matched is not None:
                # Between two held nodes (#303): absent where its condition does not hold.
                return exp.Case(
                    ifs=[exp.If(this=matched.copy(), true=exp.false())], default=exp.true()
                )
            endpoints = t._rel_var_endpoints.get(name)
            if name in t._optional_rel_vars and endpoints is not None:
                # As RETURN r reads it (select_builder): absent where its far end joined no row.
                _, _, tgt_alias, tgt_nm, _ = endpoints
                return exp.Is(this=_col(tgt_nm.id_column, tgt_alias), expression=exp.Null())
            if name not in t._optional_rel_vars:
                return exp.false()
            return None
        alias, nm = t._var_table.get(name, (name, None))
        if nm is not None:
            return exp.Is(this=_col(nm.id_column, alias), expression=exp.Null())
        return None

    def resolve_map_projection(self, node: MapProjection) -> exp.Expression:
        # n{.a, x: e, .*} → MAP(ARRAY['a','x',...], ARRAY[n."a", (e), ...]) — dotted selectors use the
        # raw property name; literal entries lower their value expression (matching the text path).
        from provisa.gql.expr_visitor import ExprLowering

        info = self._t._var_table.get(node.var)
        nm = info[1] if info else None
        keys: list[exp.Expression] = []
        vals: list[exp.Expression] = []
        # Each selector read as var.prop is: the property's column, under the variable's alias;
        # each value encoded to JSON, as a map literal's are, so a map of several types is one.
        for prop in node.properties:
            keys.append(exp.Literal.string(prop))
            vals.append(_anon("to_json", self.resolve_property(exp.column(node.var), prop)))
        if node.all_props and nm is not None:
            for prop in sorted(nm.properties.keys()):
                keys.append(exp.Literal.string(prop))
                vals.append(_anon("to_json", self.resolve_property(exp.column(node.var), prop)))
        for key, value in node.literal_entries:
            keys.append(exp.Literal.string(key))
            vals.append(_anon("to_json", ExprLowering(self).lower(value)))
        return _anon("MAP", exp.Array(expressions=keys), exp.Array(expressions=vals))

    def resolve_pattern_comprehension(self, node: PatternComprehension) -> exp.Expression:
        # [ (src)-[:REL]->(tgt) WHERE p | proj ] → a correlated ARRAY subquery over the target table,
        # joined back to src — the same "embedded query addressed out as a subquery" shape.
        from provisa.gql.expr_visitor import ExprLowering

        hop = _comprehension_hop(node.pattern)
        if hop is None:
            raise NotImplementedError(
                "unsupported pattern comprehension: a pattern of one hop, (a)-[:T]->(b), is read"
            )
        src_var, rel_type, tgt_var, tgt_label = hop
        t = self._t
        rel = t._lm.relationships.get(rel_type.upper()) if rel_type else None
        if rel is None and rel_type and rel_type not in t._lm.aliases:
            # REQ-603: a type that is not registered is refused, never read as some other
            # relationship of the source.
            from provisa.gql.translator_types import UnregisteredRelationshipType

            registered = set(t._lm.aliases) | {r.rel_type for r in t._lm.relationships.values()}
            raise UnregisteredRelationshipType([rel_type], registered)
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
                f"unresolved relationship in pattern comprehension: {src_var}-[{rel_type or ''}]->{tgt_var}"
            )
        tgt_nm = t._lm.nodes.get(rel.target_label)
        if tgt_nm is None:
            raise NotImplementedError(
                f"unresolved target node in pattern comprehension: {src_var}-[{rel_type or ''}]->{tgt_var}"
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
        tgt_table = exp.table_(
            tgt_nm.sql_table_name, db=tgt_nm.schema_name, catalog=tgt_nm.catalog_name, quoted=True
        )
        if rel.via is not None:
            # REQ-1586: the comprehension correlates on the junction row, not on the target row —
            # the junction scan is the FROM, its correlation to the outer source (with the
            # discriminator) is the WHERE, and the target joins onto it.
            via_join, tgt_join = _make_junction_joins(
                rel, False, tgt_nm, tgt_var, src_alias, "INNER", via_alias_for(rel, tgt_var)
            )
            where: exp.Expression = (
                via_join["on"] if pred is None else exp.And(this=via_join["on"], expression=pred)
            )
            select = (
                exp.select(proj)
                .from_(via_join["table"])
                .join(tgt_join["table"], on=tgt_join["on"], join_type=tgt_join["join_type"])
                .where(where)
            )
            return _anon("ARRAY", select)
        if rel.source_constant is not None:
            src_ref: exp.Expression = cast("exp.Expression", exp.convert(rel.source_constant))
        else:
            src_ref = _col(rel.join_source_column, src_alias)
        join_cond: exp.Expression = exp.EQ(
            this=src_ref,
            expression=_col(rel.join_target_column, tgt_var),
        )
        where = join_cond if pred is None else exp.And(this=join_cond, expression=pred)
        select = exp.select(proj).from_(exp.alias_(tgt_table, tgt_var)).where(where)
        return _anon("ARRAY", select)

    # -- recursion helper -------------------------------------------------- #

    def _translate_embedded(self, body: CypherAST, kind: str) -> exp.Expression:
        """Translate an embedded statement against the correlated outer scope: a fresh
        ``_Translator`` over its own copy, seeded with the outer ``_var_table``, parameters merged
        back; returns the inner SELECT as an AST."""
        from provisa.gql.translator import _Translator

        inner_ast = copy.deepcopy(body)
        inner_tr = _Translator(inner_ast, self._t._lm, self._t._params)
        inner_tr._var_table.update(self._t._var_table)
        # A node a WITH carries is held in that WITH's rows, under its name there.
        inner_tr._cte_sources = set(self._t._cte_sources)
        # The outer statement's nodes are its row, which the inner statement goes on from (#160),
        # a WITH's carried nodes included.
        inner_tr._lateral_bound = {v for v, (_, nm) in self._t._var_table.items() if nm is not None}
        inner_select, inner_params, _ = inner_tr.translate()

        for p in inner_params:
            if p not in self._t._param_seen:
                self._t._param_order.append(p)
                self._t._param_seen.add(p)
        return inner_select
