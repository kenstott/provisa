# Copyright (c) 2026 Kenneth Stott
# Canary: 33f05113-5de7-4d90-ba37-9d18654e4854
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate CypherAST to a SQLGlot SELECT AST.

Stage 1 of the Cypher pipeline:
  CypherAST + CypherLabelMap + params → (exp.Select, ordered param list)

Mapping:
  MATCH (n:Label)        → FROM schema.table AS n
  -[:REL]->              → JOIN … ON n.col = m.col
  OPTIONAL MATCH         → LEFT JOIN
  WHERE                  → WHERE
  RETURN                 → SELECT
  ORDER BY / SKIP / LIMIT → SQL equivalents
  WITH                   → CTE / subquery
"""

# Requirements: REQ-345, REQ-346, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-394, REQ-397, REQ-409

from __future__ import annotations

import copy
import re
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

import sqlglot.expressions as exp

from provisa.gql import temporal
from provisa.gql.expr_ast import Binary, CypherExpr, MapLiteral, Paren, Property, Variable, walk
from provisa.gql.ir import (
    CypherAST,
    MatchClause,
    MatchStep,
    NodePattern,
    PathPattern,
    ProcedureCall,
    ReturnItem,
    UnwindClause,
    WhereClause,
    WithClause,
    OrderItem,
)
from provisa.gql.label_map import CypherLabelMap, JunctionMapping, NodeMapping
from provisa.gql.path_expr import PathExprMixin
from provisa.gql.terms import TermsMixin
from provisa.gql.path_functions import PathFunctionsMixin
from provisa.gql.select_builder import SelectBuilderMixin
from provisa.gql.correlated_call import CorrelatedCallMixin
from provisa.gql.group_by import GroupByMixin
from provisa.gql.translator_helpers import (
    _collects_maps,
    _properties_of,
    _fold_where_into_optional_joins,
    _optional_step_matches_whole,
    _node_table_expr,
    _clause_vars,
    _rewrite_cypher_fn_node,
    _safe_alias,
)
from provisa.gql.mistakes import unknown_label
from provisa.gql.translator_types import (
    CALL_MARKER,
    CUT_COLUMN,
    CypherCrossSourceError as CypherCrossSourceError,  # re-exported for bolt/flight/neo4j error paths
    CypherTranslateError,
    GraphVarKind,
)
from provisa.gql.translator_rel import _RelJoinMixin
from provisa.gql.translator_union import _UnionMixin


if TYPE_CHECKING:
    from provisa.gql.expr_context import TranslatorExprContext


def cypher_to_sql(  # REQ-345, REQ-347, REQ-352
    ast: CypherAST,
    label_map: CypherLabelMap,
    params: dict[str, Any],
) -> tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]:
    """Translate CypherAST to SQLGlot Select.

    Returns (sql_ast, ordered_param_names, graph_vars).
    """
    translator = _Translator(ast, label_map, params)
    return translator.translate()


def _is_json_list(e: exp.Expression) -> bool:
    """Whether ``e`` is a list literal of JSON-encoded values (GraphVarKind.JSON_LIST)."""
    while isinstance(e, (exp.Alias, exp.Paren)):
        e = e.this
    return (
        isinstance(e, exp.Array)
        and bool(e.expressions)
        and all(
            isinstance(x, exp.Anonymous) and str(x.this).upper() == "TO_JSON" for x in e.expressions
        )
    )


def _holds_json_maps(e: exp.Expression) -> bool:
    """Whether ``e`` answers a map literal (``MAP(keys, to_json values)``) or a list of them: an
    ARRAY[...] of maps, an ARRAY_AGG of one, or an ARRAY(SELECT map) -- COLLECT { }'s rows -- or a
    CASE whose every result is one or null."""
    while isinstance(e, (exp.Alias, exp.Paren)):
        e = e.this
    if isinstance(e, exp.Coalesce):
        # collect(): its ARRAY_AGG, nulls filtered out, else [] (translator_helpers).
        e = e.this
    if isinstance(e, exp.Filter):
        e = e.this
    # The translator builds MAP, ARRAY_AGG and ARRAY(SELECT ...) as calls by name; a parse of its
    # SQL makes them their own node types.
    called = str(e.this).upper() if isinstance(e, exp.Anonymous) else None
    if isinstance(e, exp.Map) or called == "MAP":
        return True
    if isinstance(e, exp.Case):
        # A map or null by a condition -- an OPTIONAL MATCH's map of what matched nothing.
        results = [i.args["true"] for i in e.args.get("ifs") or []] + [e.args.get("default")]
        found = [r for r in results if r is not None and not isinstance(r, exp.Null)]
        return bool(found) and all(_holds_json_maps(r) for r in found)
    if isinstance(e, exp.ArrayAgg):
        return _holds_json_maps(e.this)
    if called == "ARRAY_AGG":
        return len(e.expressions) == 1 and _holds_json_maps(e.expressions[0])
    if (isinstance(e, exp.Array) or called == "ARRAY") and e.expressions:
        (first, *rest) = e.expressions
        if isinstance(first, (exp.Select, exp.Subquery)) and not rest:
            inner = first.this if isinstance(first, exp.Subquery) else first
            return (
                isinstance(inner, exp.Select)
                and len(inner.expressions) == 1
                and _holds_json_maps(inner.expressions[0])
            )
        return all(_holds_json_maps(x) for x in e.expressions)
    return False


def refuse_unregistered_relationship_types(ast: CypherAST, label_map: CypherLabelMap) -> None:
    """REQ-603: refuse a statement whose MATCH names a relationship type that is not registered.

    The translator lowers an unknown type to a predicate that is never true (best effort, no
    crash), which answers no rows where the statement should be refused; so the types are checked
    on the AST, before anything is lowered. Called by ``_Translator.translate`` -- the one
    translation every Cypher surface performs (HTTP, Bolt, Flight), and the one every statement
    inside a statement goes through too (EXISTS { }, COUNT { }, COLLECT { }, CALL { }) -- so none
    checks it for itself and none goes without. A pattern comprehension has no statement of its
    own and is checked where it is lowered (``expr_context.resolve_pattern_comprehension``)."""
    from provisa.gql.ir import PathFunction, PathPattern
    from provisa.gql.translator_types import UnregisteredRelationshipType

    # A registered type: one the label map holds a relationship of, by its type or by an alias.
    registered = set(label_map.aliases) | {r.rel_type for r in label_map.relationships.values()}
    unknown: set[str] = set()
    for clause in ast.match_clauses:
        pattern = clause.pattern
        path = pattern.pattern if isinstance(pattern, PathFunction) else pattern
        if isinstance(path, PathPattern):
            for rel in path.rels:
                if len(rel.types) > 1 and not rel.variable_length:
                    continue  # refused as an alternation on a single hop, by its own message
                unknown.update(t for t in rel.types if t not in registered)
    if unknown:
        raise UnregisteredRelationshipType(sorted(unknown), registered)


def cypher_calls_to_sql_list(  # REQ-571
    ast: CypherAST,
    label_map: CypherLabelMap,
    params: dict[str, Any],
) -> list[tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]]:
    """Translate each CALL {} subquery independently.

    Returns one (sql_ast, ordered_params, graph_vars) tuple per CALL {} block.
    """
    results = []
    for call_sq in ast.call_subqueries:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(call_sq.body, label_map, params)
        results.append((sql_ast, ordered_params, graph_vars))
    return results


TYPE_OF = "cypher_type_of"  # meta key: the relationship variable a type(r) literal names


_E = TypeVar("_E", bound=exp.Expr)


def _of_branch(node: _E, rel_types: dict[str, str]) -> _E:
    """``node`` as an extra UNION branch reads it: each ``type(r)`` its branch's own type."""
    if not rel_types:
        return node

    def _typed(n: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        var = n.meta.get(TYPE_OF) if isinstance(n, exp.Literal) else None
        if var in rel_types:
            return _type_literal(var, rel_types[var])
        return n

    return cast(_E, node.copy().transform(_typed))


_FLIPPED = {"right": "left", "left": "right", "none": "none"}


def _from_carried(clause: MatchClause, carried: set[str]) -> MatchClause:
    """A pattern after a WITH, read from the node the WITH carries: ``(b)-[:K]->(a)`` with ``a``
    carried is ``(a)<-[:K]-(b)``, which goes on from ``a``'s rows instead of reading ``a`` again
    beside them. A pattern that reaches a carried node only in its middle, or names its path,
    has no such reading and is refused."""
    pattern = clause.pattern
    if not isinstance(pattern, PathPattern) or not carried:
        return clause
    names = [n.variable for n in pattern.nodes]
    if names[0] in carried or not set(names) & carried:
        return clause
    if names[-1] not in carried or clause.variable is not None:
        raise CypherTranslateError(
            "A pattern after WITH goes on from a node the WITH carries: start it (or end it) at "
            "that node"
        )
    return replace(
        clause,
        pattern=PathPattern(
            nodes=pattern.nodes[::-1],
            rels=[replace(r, direction=_FLIPPED[r.direction]) for r in pattern.rels[::-1]],
        ),
    )


def _type_literal(var: str, rel_type: str) -> exp.Literal:
    lit = exp.Literal.string(rel_type)
    lit.meta[TYPE_OF] = var
    return lit


#: An ISO 8601 date-time written as a string (REQ-409).
_ISO_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?")


def _with_uncut_column(query: "exp.Select | exp.Union") -> "exp.Select | exp.Union":
    """A union part with the cut column added as 0, to line up with a part that carries it."""
    if isinstance(query, exp.Union):
        query.set("this", _with_uncut_column(query.this))
        query.set("expression", _with_uncut_column(query.expression))
        return query
    return query.select(exp.alias_(exp.Literal.number(0), alias=CUT_COLUMN))


def _cut_across_union(union: "exp.Select | exp.Union") -> exp.Select:
    """A UNION that removes duplicates, with the cut column made the same on every row: each part
    carries its own, and two rows that differ only in it are one row (REQ-2135)."""
    columns = [
        exp.column(name, table="_cutu", quoted=True)
        for name in union.named_selects
        if name != CUT_COLUMN
    ]
    cut = exp.Window(this=exp.Max(this=exp.column(CUT_COLUMN, table="_cutu", quoted=True)))
    return (
        exp.select(*columns, exp.alias_(cut, alias=CUT_COLUMN))
        .distinct()
        .from_(exp.alias_(exp.Subquery(this=union), alias="_cutu"))
    )


def _timestamp_literal(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """REQ-409: a WHERE's ISO 8601 date-time string is the timestamp it writes, so it compares with
    a timestamp column on every engine (Trino refuses a varchar there)."""
    if isinstance(node, exp.Literal) and node.is_string and _ISO_TIMESTAMP.fullmatch(node.this):
        return exp.Cast(this=node, to=exp.DataType.build("TIMESTAMP"))
    return node


#: A segment's WHEREs: the MATCHes' (filtering the rows) and each OPTIONAL MATCH's own, with the
#: variables it introduces (_steps_where).
_StageWheres = tuple["WhereClause | None", list[tuple[set[str], "WhereClause | None"]]]


#: The one row a statement that opens with an OPTIONAL MATCH starts from (_optional_start).
_OPTIONAL_START = "_optional_start"


def _optional_start(
    from_clause: "exp.Expression",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    joins: list[dict],
    matches: list[MatchClause],
    carried: str | None,
    lateral_bound: set[str],
) -> "tuple[exp.Expression, list[dict]]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A statement that opens with an OPTIONAL MATCH answers one row, its variables null, where
    the pattern matches nothing (REQ-345): it starts from one row and the pattern is joined to it as
    any OPTIONAL MATCH is, so its WHERE conditions the match (_fold_where_into_optional_joins) and
    it matches whole or not at all (_optional_step_matches_whole). After a WITH, the rows the WITH
    carries are where it starts; in a CALL body, the outer row its imported variables are bound to
    (``lateral_bound``)."""
    if carried is not None or lateral_bound or not matches or not matches[0].optional:
        return from_clause, joins
    one = exp.Subquery(
        this=exp.select(exp.alias_(exp.Literal.number(1), "_one")),
        alias=exp.TableAlias(this=exp.to_identifier(_OPTIONAL_START)),
    )
    return one, [{"table": from_clause, "on": exp.true(), "join_type": "LEFT"}, *joins]


def _and(
    left: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    right: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> "exp.Expression | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if left is None:
        return right
    if right is None:
        return left
    return exp.And(this=left, expression=right)


def _steps_where(
    steps: list,
) -> tuple[WhereClause | None, list[tuple[set[str], "WhereClause | None"]]]:
    """A segment's MATCH steps' WHEREs, each its own clause's, as openCypher scopes it (REQ-345).
    A WHERE after a MATCH filters the rows: those WHEREs ANDed, none dropped for another, returned
    first. A WHERE after an OPTIONAL MATCH is that match's condition and never removes a row,
    whatever it names: returned with the variables the OPTIONAL MATCH introduces, whose join it
    conditions (_fold_where_into_optional_joins). Every OPTIONAL MATCH is returned, with its WHERE
    or None: its pattern -- every comma-separated part of it -- matches whole or not at all
    (_optional_step_matches_whole)."""
    required: list[CypherExpr] = []
    optional: list[tuple[set[str], WhereClause | None]] = []
    seen: set[str] = set()
    for step in steps:
        new: set[str] = set()
        for m in step.matches:
            mine = _clause_vars(m)
            new |= mine - seen
            seen |= mine
        if step.matches and step.matches[-1].optional:
            optional.append((new, step.where))
        elif step.where is not None:
            required.append(step.where.expression)
    if not required:
        return None, optional
    if len(required) == 1:
        return WhereClause(required[0]), optional
    joined: CypherExpr = Paren(required[0])
    for w in required[1:]:
        joined = Binary("AND", joined, Paren(w))
    return WhereClause(joined), optional


class _Translator(  # REQ-345, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-394, REQ-409
    PathExprMixin,
    TermsMixin,
    PathFunctionsMixin,
    SelectBuilderMixin,
    CorrelatedCallMixin,
    GroupByMixin,
    _RelJoinMixin,
    _UnionMixin,
):
    def __init__(
        self,
        ast: CypherAST,
        label_map: CypherLabelMap,
        params: dict[str, Any],
    ) -> None:
        self._ast = ast
        self._lm = label_map
        self._params = params
        # variable_name → (alias, NodeMapping | None)
        self._var_table: dict[str, tuple[str, NodeMapping | None]] = {}
        # graph vars in the RETURN clause
        self._graph_vars: dict[str, GraphVarKind] = {}
        self._param_order: list[str] = []
        self._param_seen: set[str] = set()
        self._cte_sources: set[str] = set()
        # var → domain_name for nodes resolved via a domain label only
        self._domain_nodes: dict[str, str] = {}
        # extra (from, joins, path_step_overrides) branches from multi-path shortestPath/allShortestPaths
        # (FROM, joins, path steps by alias, relationship type by variable) of each extra branch
        self._extra_path_branches: list[tuple[exp.Expression | None, list[dict], dict, dict]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # #293: each node a WITH carries beside another node, by its variable: (the WITH's CTE,
        # the node's columns). Its columns are carried as "<var>.<column>", and a segment after
        # the WITH reads the node as its own row of them (_expose_split_nodes).
        self._split_carried: dict[str, tuple[str, list[str]]] = {}
        # WITH RECURSIVE CTEs for self-referential variable-length paths
        self._recursive_ctes: list[tuple[str, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # Set by PathFunctionsMixin when a recursive shortestPath is emitted
        self._shortestpath_hops_col: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        self._shortestpath_is_all: bool = False
        # REQ-2135: the variables a triple binds → the column holding each one's RDF term
        self._term_vars: dict[str, exp.Expression] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # REQ-2135: one check per path bounded by the depth limit, true when it was cut there
        self._cut_checks: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # Counter for unique UNNEST alias names across the translation
        self._unwind_count: int = 0
        # UNWIND variables whose source array contains MAP elements (e.g. collect({...}))
        self._map_unwind_vars: set[str] = set()
        # Variables whose value is a MAP array (from collect({...}) or similar)
        self._map_array_vars: set[str] = set()
        # Each UNWIND's UNNEST alias (``UNNEST(a) AS _uw0(x)``): its variable is that alias's column
        self._unwind_aliases: set[str] = set()
        # A map literal a WITH carries -> its keys, each carried as a column of its own, "m.key",
        # which m.key reads (#287)
        self._map_vars: dict[str, set[str]] = {}
        # REQ-2173: a variable a WITH or UNWIND binds to a temporal value -> its kind (temporal.py)
        self._temporal_kinds: dict[str, str] = {}
        # REQ-2173: a variable bound to a time or date time a query names a zone for -> the zone
        self._temporal_zones: dict[str, str] = {}
        # A variable a WITH or UNWIND binds to a string (size() of it counts its characters)
        self._text_vars: set[str] = set()
        # A variable a WITH or UNWIND binds to a list (+ appends to it)
        self._list_vars: set[str] = set()
        # REQ-2173: a $param a client gives a temporal value -> its kind, and the zone it names
        self._param_kinds: dict[str, str] = {}
        self._param_zones: dict[str, str] = {}
        for _name, _value in params.items():
            _kind = temporal.value_kind(_value)
            if _kind is not None:
                self._param_kinds[temporal.parameter_key(_name)] = _kind
                _zone = temporal.value_zone(_value)
                if _zone is not None:
                    self._param_zones[temporal.parameter_key(_name)] = _zone
        # path_var → (src_var, tgt_var, is_recursive) for RETURN p support
        self._path_vars: dict[str, tuple[str, str, bool]] = {}
        # path_var → (step_nodes, step_edges) for flat-JOIN paths
        self._path_steps: dict[str, tuple[list, list]] = {}
        # vars from outer scope bound via CALL { WITH x ... } — skip as FROM source
        self._lateral_bound: set[str] = set()
        # ON conditions from lateral-bound first-node relationships → added as WHERE
        self._lateral_conditions: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # CALL subquery return variable → lateral alias (e.g. "d_list" → "_call0")
        self._call_var_to_lateral: dict[str, str] = {}
        # relationship variable → resolved rel_type string (for type(r) resolution)
        self._rel_var_types: dict[str, str] = {}
        # A single-hop alternation's relationship variable -> the type its row names (a CASE on
        # the junction's type column), and each alternation's discriminator value -> type.
        self._rel_var_type_exprs: dict[str, exp.Expression] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        self._alternation_types: dict[int, dict[str, str]] = {}
        # relationship variable → (src_alias, src_nm, tgt_alias, tgt_nm, is_reversed)
        self._rel_var_endpoints: dict[str, tuple[str, "NodeMapping", str, "NodeMapping", bool]] = {}
        self._optional_rel_vars: set[str] = set()
        self._rel_var_matched: dict[str, exp.Expression] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        self._held_end_conditions: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # REQ-1586: relationship variable → (junction alias, JunctionMapping) for a junction-backed
        # edge. This is what makes the junction row's columns reachable as the relationship's
        # attributes — in RETURN r, in r.attr expressions, and in a WHERE on r.attr.
        self._rel_var_via: dict[str, tuple[str, "JunctionMapping"]] = {}
        # id(RelPattern) → (rel_type, src_alias, src_nm, tgt_alias, tgt_nm, is_reversed) for
        # every resolved rel step, including anonymous/unnamed ones, so flat-JOIN path
        # building can reconstruct full {nodes, edges} even when endpoints have no variable.
        self._rel_step_endpoints: dict[int, tuple] = {}
        # REQ-1586: (id(NodePattern), table) -> alias for a node with no variable. An anonymous endpoint
        # aliases to its table name, so a self-referencing pattern -- ()-[:KIND_OF]->() over one
        # table, which is what a junction edge between two rows of the same entity is -- would
        # otherwise give both ends the same alias and emit an ambiguous self-join.
        self._anon_node_aliases: dict[tuple[int, str], str] = {}
        # vars that are pre-built JSON from an all-rels union subquery
        self._passthrough_vars: set[str] = set()
        # relationship variables whose value is JSON_OBJECT({id,type,startNode,endNode})
        self._all_rels_rel_vars: set[str] = set()
        # node variables whose value is JSON_OBJECT({id,label,tableLabel,properties:{...}})
        self._all_rels_node_vars: set[str] = set()
        # alias of the all-rels union subquery (when built)
        self._all_rels_alias: str | None = None
        # column names in the all-rels subquery for src node, rel, tgt node JSON objects
        self._all_rels_src_col: str | None = None
        self._all_rels_rel_col: str | None = None
        self._all_rels_tgt_col: str | None = None
        # varlen rel variable → outer path variable (e.g. c from [c*..5] → r from MATCH r = ...)
        self._varlen_rel_vars: dict[str, str] = {}

    def _build_cte_segment(
        self,
        n: int,
        match_steps: list,
        unwinds: list,
        with_clause: WithClause,
        carried: str | None,
    ) -> tuple[str, exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build one CTE (from, joins, select, where, group-by) for a WITH segment.

        ``carried``: the CTE of the WITH before it, whose rows this segment goes on from.
        """
        all_matches = [
            _from_carried(m, self._cte_sources) for step in match_steps for m in step.matches
        ]
        stage_where, optional_wheres = _steps_where(match_steps)
        held: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if all_matches:
            from_clause, joins = self._build_from_joins(all_matches)
            held = list(self._held_end_conditions)
            from_clause, joins = _optional_start(
                from_clause, list(joins), all_matches, carried, self._lateral_bound
            )
            joins = self._beside_carried_rows(from_clause, list(joins), carried)
            if unwinds:
                _, uw_joins = self._build_unwind_joins(unwinds, has_from=True)
                joins.extend(uw_joins)
        elif carried is not None:
            # A WITH right after a WITH goes on from the rows that WITH carries (#288).
            from_clause = exp.Table(this=exp.Identifier(this=carried))
            joins = []
            if unwinds:
                _, uw_joins = self._build_unwind_joins(unwinds, has_from=True)
                joins.extend(uw_joins)
        elif unwinds:
            from_clause, joins = self._build_unwind_joins(unwinds, has_from=False)
            assert from_clause is not None
        else:
            # A WITH that reads no pattern (WITH 1 AS x): one row, read from no table.
            from_clause, joins = None, []
        if from_clause is not None:
            from_clause, joins = self._expose_split_nodes(from_clause, joins)

        select_exprs = self._build_with_select_items(with_clause.items)
        where_expr = self._build_where(stage_where)
        for step_vars, owned in optional_wheres:
            if owned is not None:
                joins, kept = _fold_where_into_optional_joins(
                    self._lower_where(owned), step_vars, joins
                )
                where_expr = _and(where_expr, kept)
            joins = _optional_step_matches_whole(joins, step_vars)

        stage_query = exp.select(*select_exprs)
        if from_clause is not None:
            stage_query = stage_query.from_(from_clause)
        for join in joins:
            stage_query = stage_query.join(
                join["table"], on=join["on"], join_type=join["join_type"]
            )
        if where_expr:
            stage_query = stage_query.where(where_expr)
        for cond in held:
            stage_query = stage_query.where(cond)
        if with_clause.distinct:
            stage_query = stage_query.distinct()
        with_group_exprs = self._build_group_by_for_with(with_clause.items)
        if with_group_exprs:
            stage_query = stage_query.group_by(*with_group_exprs)
        # The WITH's ORDER BY, SKIP and LIMIT pick the rows it carries on, before its WHERE.
        if with_clause.order_by:
            stage_query = stage_query.order_by(
                *self._build_order_by(with_clause.order_by, with_clause.items)
            )
        if with_clause.limit is not None:
            stage_query = stage_query.limit(with_clause.limit)
        if with_clause.skip is not None:
            stage_query = stage_query.offset(with_clause.skip)
        if with_clause.where is not None:
            # The WITH's WHERE reads what the WITH carries, which here is the wrapped stage.
            before = (self._var_table, self._cte_sources, self._split_carried)
            self._update_var_table_for_with(with_clause.items, "_inner")
            with_where_expr = self._build_where(with_clause.where)
            exposed = self._split_node_joins()
            self._var_table, self._cte_sources, self._split_carried = before
            if with_where_expr:
                # The stage's own columns: a carried node read beside them adds its own (#293).
                star = (
                    exp.Column(this=exp.Star(), table=exp.to_identifier("_inner"))
                    if exposed
                    else exp.Star()
                )
                wrapper = exp.select(star)
                wrapper = wrapper.from_(exp.alias_(exp.Subquery(this=stage_query), alias="_inner"))
                for join in exposed:
                    wrapper = wrapper.join(join["table"], join_type=join["join_type"])
                stage_query = wrapper.where(with_where_expr)

        cte_name = f"_w{n}"
        return cte_name, stage_query

    def _split_node_joins(self) -> list[dict]:
        """Each node a WITH carries beside another, read from its row of the WITH's CTE as a table
        of its own columns under its own name (#293)."""
        joins = []
        for key, (cte, cols) in self._split_carried.items():
            row = exp.select(
                *(
                    exp.alias_(exp.column(f"{key}.{c}", table=cte, quoted=True), c, quoted=True)
                    for c in cols
                )
            )
            table = exp.alias_(exp.Lateral(this=exp.Subquery(this=row)), alias=key)
            joins.append({"table": table, "on": None, "join_type": "CROSS"})
        return joins

    def _expose_split_nodes(
        self,
        from_clause: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The segment's FROM with the carried nodes read from their WITH's rows (#293): a pattern
        starting at one goes on from that WITH's CTE, and each such node is joined, per row, right
        after it, before any join that reads it."""
        if not self._split_carried:
            return from_clause, joins
        (cte,) = {c for c, _ in self._split_carried.values()}
        exposed = self._split_node_joins()
        # The CTE is read once, first: a join of it _beside_carried_rows set after is dropped.
        rest = [
            j for j in joins if not (isinstance(j["table"], exp.Table) and j["table"].name == cte)
        ]
        if isinstance(from_clause, exp.Table) and from_clause.name in (cte, *self._split_carried):
            return exp.Table(this=exp.Identifier(this=cte)), [*exposed, *rest]
        carried = {
            "table": exp.Table(this=exp.Identifier(this=cte)),
            "on": None,
            "join_type": "CROSS",
        }
        return from_clause, [carried, *exposed, *rest]

    @staticmethod
    def _beside_carried_rows(
        from_clause: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
        carried: str | None,
    ) -> list[dict]:
        """A MATCH after a WITH goes on from the rows the WITH carries (#147): where its own
        patterns do not read them, they are set beside its rows as a second MATCH's are, for the
        WHERE to relate."""
        if carried is None:
            return joins
        read = {
            t.name
            for part in [from_clause, *(j["table"] for j in joins)]
            for t in part.find_all(exp.Table)
            if not t.db
        }
        if carried in read:
            return joins
        table = exp.Table(this=exp.Identifier(this=carried))
        return [*joins, {"table": table, "on": None, "join_type": "CROSS"}]

    def _build_final_from(
        self,
        final_match_steps: list,
        final_unwinds: list,
        cte_defs: list[tuple[str, exp.Expression]],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> tuple[exp.Expression | None, list[dict], list[MatchClause], "_StageWheres"]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM/joins for the final segment.

        Returns (from_clause, joins, all_matches, its WHEREs as _steps_where gives them).
        """
        all_matches = [
            _from_carried(m, self._cte_sources) for step in final_match_steps for m in step.matches
        ]
        stage_where = _steps_where(final_match_steps)

        if not all_matches and not final_unwinds and cte_defs:
            last_cte_name = cte_defs[-1][0]
            from_clause: exp.Expression = exp.Table(this=exp.Identifier(this=last_cte_name))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            joins: list[dict] = []
        elif not all_matches and final_unwinds:
            if cte_defs:
                last_cte_name = cte_defs[-1][0]
                from_clause = exp.Table(this=exp.Identifier(this=last_cte_name))
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(uw_joins)
            else:
                uw_from, uw_joins = self._build_unwind_joins(final_unwinds, has_from=False)
                assert uw_from is not None
                from_clause = uw_from
                joins = list(uw_joins)
        elif all_matches:
            from_clause, joins = self._build_from_joins(all_matches)
            carried = cte_defs[-1][0] if cte_defs else None
            from_clause, joins = _optional_start(
                from_clause, list(joins), all_matches, carried, self._lateral_bound
            )
            joins = self._beside_carried_rows(from_clause, list(joins), carried)
            if final_unwinds:
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(joins) + uw_joins
        else:
            # A statement that reads no pattern (RETURN 1 + 1): one row, read from no table.
            return None, [], [], stage_where

        from_clause, joins = self._expose_split_nodes(from_clause, joins)
        return from_clause, joins, all_matches, stage_where

    def _apply_where_and_fold(
        self,
        stage_where: "_StageWheres",
        joins: list[dict],
    ) -> tuple["exp.Expression | None", list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build the WHERE of the MATCHes and fold each OPTIONAL MATCH's own WHERE into its join.

        Also updates self._extra_path_branches with the folded joins.
        """
        required, optional_wheres = stage_where
        where_expr = self._build_where(required)
        for step_vars, owned in optional_wheres:
            if owned is not None:
                joins, kept = _fold_where_into_optional_joins(
                    self._lower_where(owned), step_vars, joins
                )
                where_expr = _and(where_expr, kept)
                self._extra_path_branches = [
                    (
                        bf,
                        _fold_where_into_optional_joins(
                            _of_branch(self._lower_where(owned), types), step_vars, bj
                        )[0],
                        bps,
                        types,
                    )
                    for bf, bj, bps, types in self._extra_path_branches
                ]
            joins = _optional_step_matches_whole(joins, step_vars)
            self._extra_path_branches = [
                (bf, _optional_step_matches_whole(bj, step_vars), bps, types)
                for bf, bj, bps, types in self._extra_path_branches
            ]
        return where_expr, joins

    def _build_main_query(
        self,
        from_clause: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
        where_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        select_exprs: list["exp.Expr"],
    ) -> "exp.Select":
        """Assemble the main SELECT query from components. ``from_clause`` is None for a statement
        that reads no pattern: a SELECT with no FROM."""
        query = exp.select(*select_exprs)
        if from_clause is not None:
            query = query.from_(from_clause)
        if self._ast.return_clause and self._ast.return_clause.distinct:
            query = query.distinct()
        for join in joins:
            query = query.join(join["table"], on=join["on"], join_type=join["join_type"])
        if where_expr:
            query = query.where(where_expr)
        for lat_cond in self._lateral_conditions:
            query = query.where(lat_cond)
        for held in self._held_end_conditions:
            query = query.where(held)
        group_exprs = (
            self._build_group_by(self._ast.return_clause) if self._ast.return_clause else []
        )
        if group_exprs:
            query = query.group_by(*group_exprs)
        if self._ast.return_clause and self._ast.return_clause.having is not None:
            query = query.having(self._parse_expr(self._ast.return_clause.having))
        return query

    def _apply_extra_branches(
        self,
        result: "exp.Select | exp.Union",
        select_exprs: list["exp.Expr"],
        where_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "exp.Select | exp.Union":
        """Apply UNION ALL extra branches from multi-path shortestPath/allShortestPaths."""
        for extra_from, extra_joins, extra_path_steps_map, rel_types in self._extra_path_branches:
            branch_select_exprs = []
            for s_expr in select_exprs:
                alias_name = getattr(s_expr, "alias", None)
                if alias_name and alias_name in extra_path_steps_map:
                    sn, se = extra_path_steps_map[alias_name]
                    new_path = self._build_path_json(sn, se)
                    branch_select_exprs.append(exp.alias_(new_path, alias_name))
                else:
                    branch_select_exprs.append(_of_branch(s_expr, rel_types))
            assert extra_from is not None
            branch = exp.select(*branch_select_exprs).from_(extra_from)
            if self._ast.return_clause and self._ast.return_clause.distinct:
                branch = branch.distinct()
            for j in extra_joins:
                branch = branch.join(j["table"], on=j["on"], join_type=j["join_type"])
            if where_expr:
                branch = branch.where(_of_branch(where_expr, rel_types))
            result = exp.Union(this=result, expression=branch, distinct=False)
        return result

    def _construct_union(
        self,
        from_clause: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
        where_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        selects: list[list["exp.Expr"]],
    ) -> "exp.Select | exp.Union":
        """REQ-2135: a CONSTRUCT's triples, one select per template triple, put together with
        UNION ALL: the graph is a set, and its writer keeps each triple once."""
        result: exp.Select | exp.Union | None = None
        for select_exprs in selects:
            query = self._build_main_query(
                from_clause.copy() if from_clause is not None else None,
                [{**j, "table": j["table"].copy(), "on": j["on"].copy()} for j in joins],
                where_expr.copy() if where_expr is not None else None,
                select_exprs,
            )
            result = (
                query
                if result is None
                else exp.Union(this=result, expression=query, distinct=False)
            )
        assert result is not None
        return result

    def _cut_column(self) -> "exp.Expr":
        """REQ-2135: 1 when any path bounded by the depth limit was cut there, else 0."""
        cut = self._cut_checks[0]
        for check in self._cut_checks[1:]:
            cut = exp.Or(this=cut, expression=check)
        self._graph_vars[CUT_COLUMN] = GraphVarKind.CUT
        return exp.alias_(
            exp.Case(ifs=[exp.If(this=cut, true=exp.Literal.number(1))]).else_(
                exp.Literal.number(0)
            ),
            alias=CUT_COLUMN,
        )

    def _fold_union_parts(self, result: "exp.Select | exp.Union") -> "exp.Select | exp.Union":
        """Fold UNION / UNION ALL parts into result. A part that carries the cut column and one
        that does not are made to match: the one without it is never cut (REQ-2135)."""
        for i, (sub_ast, is_all) in enumerate(self._ast.union_parts):
            sub_sql, sub_params, sub_graph_vars = cypher_to_sql(sub_ast, self._lm, self._params)
            for p in sub_params:
                if p not in self._param_seen:
                    self._param_order.append(p)
                    self._param_seen.add(p)
            head_cut = CUT_COLUMN in self._graph_vars
            if CUT_COLUMN in sub_graph_vars and not head_cut:
                result = _with_uncut_column(result)
            elif head_cut and CUT_COLUMN not in sub_graph_vars:
                sub_sql = _with_uncut_column(sub_sql)
            self._graph_vars.update(sub_graph_vars)
            # If the sub-branch has a per-branch LIMIT/SKIP, wrap it in a subquery so
            # the limit applies only to that branch, not the whole union.
            if sub_ast.limit is not None or sub_ast.skip is not None:
                sub_sql = exp.select(exp.Star()).from_(
                    exp.alias_(exp.Subquery(this=sub_sql), alias=f"_ub{i}")
                )
            result = exp.Union(this=result, expression=sub_sql, distinct=not is_all)
        if CUT_COLUMN in self._graph_vars and not all(a for _, a in self._ast.union_parts):
            result = _cut_across_union(result)
        return result

    def _apply_order_limit(
        self,
        result: "exp.Select | exp.Union",
        order_exprs: list["exp.Expression"],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "exp.Select | exp.Union":
        """Apply ORDER BY / LIMIT / OFFSET, wrapping in outer SELECT if needed."""
        has_ordering = order_exprs or self._ast.limit is not None or self._ast.skip is not None
        needs_outer = bool(self._ast.union_parts) or bool(self._extra_path_branches)
        if needs_outer and has_ordering:
            outer = exp.select(exp.Star()).from_(
                exp.alias_(exp.Subquery(this=result), alias="_union")
            )
            if order_exprs:
                outer = outer.order_by(*order_exprs)
            if self._ast.limit is not None:
                outer = outer.limit(self._ast.limit)
            if self._ast.skip is not None:
                outer = outer.offset(self._ast.skip)
            return outer
        if order_exprs:
            result = result.order_by(*order_exprs)
        if self._ast.limit is not None:
            result = result.limit(self._ast.limit)
        if self._ast.skip is not None:
            result = result.offset(self._ast.skip)
        return result

    def translate(self) -> tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if self._ast.return_clause is None and self._ast.construct is None:
            raise CypherTranslateError(
                "Cannot translate a CALL {}-only query directly. "
                "Use cypher_calls_to_sql_list() instead."
            )

        refuse_unregistered_relationship_types(self._ast, self._lm)
        segments = self._group_pipeline()
        cte_defs: list[tuple[str, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        for n, (match_steps, unwinds, with_clause) in enumerate(segments[:-1]):
            assert with_clause is not None
            cte_name, stage_query = self._build_cte_segment(
                n, match_steps, unwinds, with_clause, cte_defs[-1][0] if cte_defs else None
            )
            cte_defs.append((cte_name, stage_query))
            self._update_var_table_for_with(with_clause.items, cte_name)

        final_match_steps, final_unwinds, _ = segments[-1]
        from_clause, joins, all_matches, stage_where = self._build_final_from(
            final_match_steps, final_unwinds, cte_defs
        )

        lateral_joins = self._translate_correlated_calls(self._ast.call_subqueries)
        joins = list(joins) + lateral_joins

        if self._ast.construct is not None:
            # REQ-2135: one select list per template triple, over the same matches.
            selects: list[list[exp.Expr]] = self._construct_selects(self._ast.construct)
        else:
            assert self._ast.return_clause is not None
            selects = [self._build_select(self._ast.return_clause)]
        if self._cut_checks:
            cut = self._cut_column()
            for select_exprs in selects:
                select_exprs.append(cut.copy())
        where_expr, joins = self._apply_where_and_fold(stage_where, joins)

        order_exprs = self._build_order_by(
            self._ast.order_by,
            self._ast.return_clause.items if self._ast.return_clause is not None else [],
        )

        if self._ast.construct is not None:
            if self._extra_path_branches:
                raise CypherTranslateError("A CONSTRUCT's pattern has no several-path branch")
            if not all(is_all for _, is_all in self._ast.union_parts):
                raise CypherTranslateError("A CONSTRUCT's parts are put together with UNION ALL")
            result: exp.Select | exp.Union = self._construct_union(
                from_clause, joins, where_expr, selects
            )
        else:
            select_exprs = selects[0]
            query = self._build_main_query(from_clause, joins, where_expr, select_exprs)
            result = self._apply_extra_branches(query, select_exprs, where_expr)

        for cte_name, cte_query in cte_defs:
            result = result.with_(cte_name, as_=cte_query)
        for cte_name, cte_expr in self._recursive_ctes:
            result = result.with_(cte_name, as_=cte_expr, recursive=True)

        # If this is the first branch of a UNION ALL and has a per-branch LIMIT/SKIP,
        # apply it to just this branch before folding the union. Without this the limit
        # would be placed on the outer wrapper and constrain the whole union result.
        if (
            self._ast.union_parts
            and not self._ast.modifiers_of_whole
            and (self._ast.limit is not None or self._ast.skip is not None)
        ):
            if self._ast.limit is not None:
                result = result.limit(self._ast.limit)
            if self._ast.skip is not None:
                result = result.offset(self._ast.skip)
            result = exp.select(exp.Star()).from_(
                exp.alias_(exp.Subquery(this=result), alias="_ub_first")
            )
            self._ast.limit = None
            self._ast.skip = None

        result = self._fold_union_parts(result)

        if self._shortestpath_hops_col is not None:
            order_exprs = [self._shortestpath_hops_col] + list(order_exprs)
            if not self._shortestpath_is_all and self._ast.limit is None:
                self._ast.limit = 1

        result = self._apply_order_limit(result, order_exprs)
        result = self._reconcile_params_with_placeholders(result)
        self._mark_json_maps(result)
        return result, self._param_order, self._graph_vars

    def _mark_json_maps(self, result: "exp.Select | exp.Union") -> None:
        """REQ-345: name each returned column that holds map literals, so the answer decodes their
        JSON-encoded values. A UNION's columns are its first branch's."""
        select: exp.Expression = result
        while isinstance(select, exp.Union):
            select = select.this
        if not isinstance(select, exp.Select):
            return
        for projection in select.expressions:
            name = projection.alias_or_name
            if not name or name.lower() in self._graph_vars:
                continue
            if _holds_json_maps(projection):
                self._graph_vars[name.lower()] = GraphVarKind.JSON_MAP
            elif _is_json_list(projection):
                self._graph_vars[name.lower()] = GraphVarKind.JSON_LIST

    def _reconcile_params_with_placeholders(
        self,
        result: "exp.Select | exp.Union",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "exp.Select | exp.Union":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Drop/renumber ``$N`` params no longer present in the final query.

        A rewrite that drops part of the statement (a WHERE folded to a literal) discards every
        ``$N`` placeholder that lived inside it while ``self._param_order`` still lists the params
        those placeholders bound to. Left
        alone, the caller's positional ``[parameters.get(name) for name in ordered_params]`` either
        hands the executor more bound values than the query has placeholders for (crash) or, worse,
        shifts every surviving param's value onto the wrong ``$N`` (silent corruption) — so both
        pruning and renumbering must happen together, not just one or the other.
        """

        # $N surfaces as two distinct AST shapes depending which path built it: the text path
        # (e.g. UNWIND's bare-$param source, kept off the dialect parser deliberately — see
        # _build_unwind_expr) parses with no dialect, so `$1` becomes a plain identifier column;
        # the AST/predicate path parses with dialect="postgres" via sqlglot's own grammar, so `$1`
        # becomes a native Parameter(this=Literal(1)) node. Both must be found and rewritten.
        def _ordinal_of(node: "exp.Expression") -> int | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            if isinstance(node, exp.Column):
                ident = node.this
                if isinstance(ident, exp.Identifier) and re.fullmatch(r"\$\d+", ident.this or ""):
                    return int(ident.this[1:])
            elif isinstance(node, exp.Parameter) and isinstance(node.this, exp.Literal):
                try:
                    return int(node.this.this)
                except (TypeError, ValueError):
                    return None
            return None

        placeholders: list[tuple["exp.Expression", int]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for node in result.find_all(exp.Column, exp.Parameter):
            ordinal = _ordinal_of(node)
            if ordinal is not None:
                placeholders.append((node, ordinal))

        used_ordinals = {ordinal for _, ordinal in placeholders}
        if not self._param_order or used_ordinals == set(range(1, len(self._param_order) + 1)):
            return result  # common case: every registered param is still referenced, in order
        kept_old_ordinals = sorted(used_ordinals)
        renumber = {old: new for new, old in enumerate(kept_old_ordinals, start=1)}
        for node, old in placeholders:
            if old in renumber:
                new = renumber[old]
                if isinstance(node, exp.Column):
                    node.this.set("this", f"${new}")
                else:
                    node.set("this", exp.Literal(this=new, is_string=False))
        self._param_order = [self._param_order[old - 1] for old in kept_old_ordinals]
        return result

    def _group_pipeline(
        self,
    ) -> list[tuple[list[MatchStep], list[UnwindClause | ProcedureCall], WithClause | None]]:
        segments: list = []
        current_matches: list[MatchStep] = []
        current_unwinds: list[UnwindClause | ProcedureCall] = []
        for item in self._ast.pipeline:
            if isinstance(item, MatchStep):
                current_matches.append(item)
            elif isinstance(item, (UnwindClause, ProcedureCall)):
                current_unwinds.append(item)  # REQ-2168: a call's rows are a source as UNWIND's
            elif isinstance(item, WithClause):
                segments.append((list(current_matches), list(current_unwinds), item))
                current_matches = []
                current_unwinds = []
        segments.append((list(current_matches), list(current_unwinds), None))
        return segments

    def _build_unwind_expr(self, expr: CypherExpr) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """An UNWIND source as a SQLGlot expression. In an UNNEST a parameter is the executor's
        ``$N`` placeholder as a name, which every dialect writes as it is (a parameter node is
        ``@N`` in some), and a variable is its bare name, resolved by the engine."""
        if isinstance(expr, Variable):
            return exp.Column(this=exp.Identifier(this=expr.name, quoted=False))

        def _placeholder(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            if isinstance(node, exp.Parameter):
                return exp.Column(this=exp.Identifier(this=f"${node.this.this}", quoted=False))
            return node

        return self._lower_expr(expr).transform(_placeholder)

    def _build_unwind_joins(
        self,
        unwinds: list[UnwindClause | ProcedureCall],
        has_from: bool,
    ) -> tuple[exp.Expression | None, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM/CROSS JOIN sources for UNWIND clauses.

        If has_from is False, the first UNWIND becomes the FROM expression.
        Returns (from_expr_or_None, [cross_join_dicts]).
        """
        from_expr: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        cross_joins: list[dict] = []
        for uw in unwinds:
            if isinstance(uw, ProcedureCall):  # REQ-2168
                source = self._procedure_source(uw)
                if not has_from and from_expr is None:
                    from_expr = source
                    has_from = True
                else:
                    cross_joins.append({"table": source, "on": None, "join_type": "CROSS"})
                continue
            alias = f"_uw{self._unwind_count}"
            self._unwind_count += 1
            array_expr = self._build_unwind_expr(uw.expression)
            # Detect MAP-element arrays: a list holding map literals, or a variable assigned from
            # collect({...}) in a preceding WITH clause.
            source = uw.expression
            if any(isinstance(node, MapLiteral) for node in walk(source)) or (
                isinstance(source, Variable) and source.name in self._map_array_vars
            ):
                self._map_unwind_vars.add(uw.variable)
            unnest = exp.Unnest(
                expressions=[array_expr],
                alias=exp.TableAlias(
                    this=exp.Identifier(this=alias),
                    columns=[exp.Identifier(this=uw.variable)],
                ),
            )
            self._var_table[uw.variable] = (alias, None)
            self._unwind_aliases.add(alias)
            element = self._temporal_context().temporal_element_kind(uw.expression)
            if element is not None:
                self._temporal_kinds[uw.variable] = element
                zone = self._temporal_context().temporal_element_zone(uw.expression)
                if zone is not None:
                    self._temporal_zones[uw.variable] = zone
            if self._temporal_context().holds_text_items(uw.expression):
                self._text_vars.add(uw.variable)
            if self._temporal_context().holds_list_items(uw.expression):
                self._list_vars.add(uw.variable)
            if not has_from and from_expr is None:
                from_expr = unnest
                has_from = True
            else:
                cross_joins.append({"table": unnest, "on": None, "join_type": "CROSS"})
        return from_expr, cross_joins

    def _call_value(self, arg: CypherExpr, call: ProcedureCall, position: int) -> Any:
        """A CALL argument's value: a literal, a ``$parameter``'s value, or a list or map of
        them (``apoc.meta.data({sample: $SAMPLE})``)."""
        from provisa.gql.expr_ast import ListLiteral, Literal, Parameter

        if isinstance(arg, Literal):
            return arg.value
        if isinstance(arg, ListLiteral):
            return [self._call_value(item, call, position) for item in arg.items]
        if isinstance(arg, MapLiteral):
            return {k: self._call_value(v, call, position) for k, v in arg.entries}
        if isinstance(arg, Parameter):
            if arg.name not in self._params:
                raise CypherTranslateError(
                    f"CALL {call.name}: parameter ${arg.name} (argument {position}) was not supplied"
                )
            return self._params[arg.name]
        raise CypherTranslateError(
            f"CALL {call.name}: argument {position} is not a value or a $parameter"
        )

    def _procedure_source(self, call: ProcedureCall) -> exp.Expression:  # REQ-2168
        """A composed CALL as a source: the marker table function (CALL_MARKER) aliased with the
        variables it yields, filtered by its WHERE. Each argument's value is written into it, so
        a statement holding one is translated for its parameters' values."""
        import json

        alias = f"_call{self._unwind_count}"
        self._unwind_count += 1
        variables = [var for _, var in call.yields]
        values = [self._call_value(a, call, i + 1) for i, a in enumerate(call.args)]
        marker = exp.Anonymous(
            this=CALL_MARKER,
            expressions=[
                exp.Literal.string(call.name),
                exp.Literal.string(json.dumps([col for col, _ in call.yields])),
                exp.Literal.string(json.dumps(values, default=str)),
            ],
        )
        table = exp.Table(
            this=marker,
            alias=exp.TableAlias(
                this=exp.Identifier(this=alias),
                columns=[exp.Identifier(this=v, quoted=True) for v in variables],
            ),
        )
        for var in variables:
            self._var_table[var] = (alias, None)
        self._unwind_aliases.add(alias)
        condition = self._build_where(call.where)
        if condition is None:
            return table
        return exp.Subquery(
            this=exp.select(exp.Star()).from_(table).where(condition),
            alias=exp.TableAlias(this=exp.Identifier(this=alias)),
        )

    def _register_node(self, node: "NodePattern") -> None:
        """Register a single node into var_table and domain_nodes if not already present."""
        if not node.variable or node.variable in self._var_table:
            return
        if node.label_alternation and len(node.labels) > 1:
            ad_hoc = f"__alt_{node.variable}__"
            if ad_hoc not in self._lm.domains:
                from provisa.gql.label_map import CypherLabelMap

                self._lm = CypherLabelMap(
                    nodes=self._lm.nodes,
                    relationships=self._lm.relationships,
                    domains={**self._lm.domains, ad_hoc: node.labels},
                )
            self._domain_nodes[node.variable] = ad_hoc
            self._var_table[node.variable] = (node.variable, None)
        elif node.labels:
            type_label, domain_label = self._resolve_node_type(node.labels)
            if type_label:
                self._var_table[node.variable] = (node.variable, self._lm.nodes[type_label])
            else:
                self._domain_nodes[node.variable] = domain_label  # type: ignore[assignment]
                self._var_table[node.variable] = (node.variable, None)
        else:
            self._domain_nodes[node.variable] = "__all__"
            self._var_table[node.variable] = (node.variable, None)

    def _build_first_node_from(self, first_node: "NodePattern") -> "exp.Expression | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM expr for the first node; None if lateral-bound or not resolvable."""
        fv = first_node.variable
        if fv and fv in self._lateral_bound:
            return None
        if fv and fv in self._cte_sources:
            sql_alias = self._var_table[fv][0]
            return exp.Table(this=exp.Identifier(this=sql_alias))
        if fv and fv in self._domain_nodes:
            return self._build_domain_union(fv, self._domain_nodes[fv])
        if fv and fv in self._var_table and self._var_table[fv][1]:
            nm = self._var_table[fv][1]
            assert nm is not None
            if getattr(nm, "traversal_only", False):
                raise CypherTranslateError(
                    f"Node '{nm.label}' is in a domain outside your access. "
                    f"Start the pattern from a node in your own domain and traverse to '{nm.label}' via a relationship."
                )
            return _node_table_expr(nm, fv)
        if first_node.labels:
            type_label, _ = self._resolve_node_type(first_node.labels)
            nm = self._lm.nodes.get(type_label) if type_label else None
            if nm:
                if getattr(nm, "traversal_only", False):
                    raise CypherTranslateError(
                        f"Node '{nm.label}' is in a domain outside your access. "
                        f"Start the pattern from a node in your own domain and traverse to '{nm.label}' via a relationship."
                    )
                assert type_label is not None
                alias = fv or nm.table_name
                return _node_table_expr(nm, alias)
        return None

    def _build_standalone_node_join(self, fv: "str | None", clause: MatchClause) -> "dict | None":
        """Build a JOIN dict for a standalone node (no relationships) in a non-first clause."""
        if not fv or fv in self._cte_sources:
            return None
        join_type = "LEFT" if clause.optional else "CROSS"
        on_clause = exp.true() if join_type == "LEFT" else None
        if fv in self._domain_nodes:
            join_table = self._build_domain_union(fv, self._domain_nodes[fv])
            return {"table": join_table, "on": on_clause, "join_type": join_type}
        if fv in self._var_table and self._var_table[fv][1]:
            nm = self._var_table[fv][1]
            assert nm is not None
            join_table = _node_table_expr(nm, fv)
            return {"table": join_table, "on": on_clause, "join_type": join_type}
        return None

    def _temporal_context(self) -> "TranslatorExprContext":
        """The context temporal kinds are read in (REQ-2173)."""
        from provisa.gql.expr_context import TranslatorExprContext

        return TranslatorExprContext(self)

    def _lower_expr(self, expr: CypherExpr) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Lower an expression tree to sqlglot node-to-node against the translator's traversal state
        (REQ-913). Raises on a construct the context cannot handle, so a gap surfaces loudly instead
        of degrading. The tree is the IR's: lowering works on its own copy."""
        from provisa.gql.expr_context import TranslatorExprContext
        from provisa.gql.expr_visitor import ExprLowering

        node = ExprLowering(TranslatorExprContext(self)).lower(copy.deepcopy(expr))
        return node.transform(_rewrite_cypher_fn_node)

    def _build_where(self, where: WhereClause | None) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if where is None:
            return None
        return self._lower_where(where)

    def _lower_where(self, where: WhereClause) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        return self._lower_expr(where.expression).transform(_timestamp_literal)

    def _parse_expr(self, expr: CypherExpr) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """An expression tree as a SQLGlot expression (REQ-913)."""
        return self._lower_expr(expr)

    def _build_order_by(
        self, order_by: list[OrderItem], returned: list[ReturnItem]
    ) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``order_by`` as SQL. ``alias.key`` of a map literal the clause names ``alias`` orders by
        that key's own value: the map's SQL column holds it JSON-encoded."""
        maps = {
            r.alias: dict(r.expression.entries)
            for r in returned
            if r.alias and isinstance(r.expression, MapLiteral)
        }
        exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for item in order_by:
            target = item.expression
            if (
                isinstance(target, Property)
                and isinstance(target.obj, Variable)
                and target.name in maps.get(target.obj.name, {})
            ):
                target = maps[target.obj.name][target.name]
            inner = self._parse_expr(target)
            if isinstance(inner, exp.Null):
                # A key null for every row (a property the label has not) orders nothing, and an
                # engine refuses a literal one.
                continue
            # Cypher orders null as the largest value: last ascending, first descending.
            desc = item.direction == "DESC"
            exprs.append(exp.Ordered(this=inner, desc=desc, nulls_first=desc))
        return exprs

    def _carried_map_keys(self, expr: CypherExpr, alias: str) -> list[exp.Expr]:
        """A map literal the WITH carries as ``alias``: each key's own value as a column of its
        own, ``"alias.key"``, which ``alias.key`` after the WITH reads -- the value as it is, not
        its JSON-encoded copy in the map (#287)."""
        if not isinstance(expr, MapLiteral):
            return []
        self._map_vars[alias] = {key for key, _value in expr.entries}
        return [
            exp.alias_(self._lower_expr(value), f"{alias}.{key}", quoted=True)
            for key, value in expr.entries
        ]

    def _carried_nodes(self, items: list[ReturnItem]) -> dict[str, NodeMapping]:
        """The nodes a WITH carries by their keys, when it carries two or more: each is carried as
        its own columns, ``<key>.<column>``, so two tables' columns of one name never meet (#293)."""
        nodes = {}
        for item in items:
            expr = item.expression
            if isinstance(expr, Variable) and expr.name in self._var_table:
                meta = self._var_table[expr.name][1]
                if isinstance(meta, NodeMapping):
                    nodes[item.alias or expr.name] = meta
        return nodes if len(nodes) > 1 else {}

    def _node_columns(self, nm: NodeMapping) -> list[str]:
        """Every column of ``nm``'s table a later clause can read: its properties, its keys and
        the join columns of the relationships that reach it."""
        cols = {*nm.properties.values(), *nm.physical_properties.values(), nm.id_column}
        cols.update(nm.pk_columns)
        rels = [
            *self._lm.relationships.values(),
            *(r for rs in self._lm.aliases.values() for r in rs),
        ]
        for rel in rels:
            for end, column, expr in (
                (rel.source_label, rel.join_source_column, rel.source_expr),
                (rel.target_label, rel.join_target_column, rel.target_expr),
            ):
                if end not in (nm.label, nm.type_name):
                    continue
                if expr is not None:
                    raise CypherTranslateError(
                        f"A WITH carrying {nm.label} beside another node: its relationship "
                        f"{rel.rel_type} joins on an expression"
                    )
                cols.add(column)
        return sorted(c for c in cols if c)

    def _build_with_select_items(self, items: list[ReturnItem]) -> list[exp.Expr]:
        exprs: list[exp.Expr] = []
        split = self._carried_nodes(items)
        for item in items:
            expr = item.expression
            alias = item.alias
            bare = expr.name if isinstance(expr, Variable) else None
            if bare is not None and (alias or bare) in split:
                held_in = self._var_table[bare][0]
                key = alias or bare
                exprs.extend(
                    exp.alias_(exp.column(c, table=held_in, quoted=True), f"{key}.{c}", quoted=True)
                    for c in self._node_columns(split[key])
                )
                continue
            if bare is not None and bare in self._var_table:
                expr_text = bare
                held_in, meta = self._var_table[expr_text]
                if expr_text in self._cte_sources and meta is None:
                    # A value an earlier WITH carries: one column of that WITH's rows.
                    carried = exp.column(expr_text, table=held_in, quoted=True)
                    exprs.append(exp.alias_(carried, alias or expr_text, quoted=True))
                    # A map literal goes on with the columns its keys are read from (#287).
                    keys = self._map_vars.get(expr_text)
                    if keys is not None:
                        self._map_vars[alias or expr_text] = keys
                        exprs.extend(
                            exp.alias_(
                                exp.column(f"{expr_text}.{key}", table=held_in, quoted=True),
                                f"{alias or expr_text}.{key}",
                                quoted=True,
                            )
                            for key in sorted(keys)
                        )
                    continue
                if meta is None and held_in in self._unwind_aliases:
                    # An UNWIND's or a CALL's variable is one column of its source, not a row.
                    exprs.append(
                        exp.alias_(self._parse_expr(expr), alias or expr_text, quoted=True)
                    )
                    continue
                tbl_col = exp.Column(
                    this=exp.Star(),
                    table=exp.Identifier(this=held_in),
                )
                if alias:
                    exprs.append(exp.alias_(tbl_col, alias))
                else:
                    exprs.append(tbl_col)
            else:
                parsed = self._parse_expr(expr)
                if alias:
                    exprs.append(exp.alias_(parsed, alias))
                    exprs.extend(self._carried_map_keys(expr, alias))
                else:
                    exprs.append(parsed)
        return exprs

    def _update_var_table_for_with(self, items: list[ReturnItem], cte_name: str) -> None:
        new_var_table: dict[str, tuple[str, Any]] = {}
        kinds: dict[str, str] = {}
        zones: dict[str, str] = {}
        texts: set[str] = set()
        lists: set[str] = set()
        split = self._carried_nodes(items)
        self._split_carried = {k: (cte_name, self._node_columns(nm)) for k, nm in split.items()}
        for item in items:
            name = item.alias or (
                item.expression.name if isinstance(item.expression, Variable) else None
            )
            if name and self._temporal_context().is_text(item.expression):
                texts.add(name)
            if name and self._temporal_context().is_list(item.expression):
                lists.add(name)
            found = self._temporal_context().temporal_kind(item.expression)
            if name and found is not None:
                kinds[name] = found
                zone = self._temporal_context().temporal_zone(item.expression)
                if zone is not None:
                    zones[name] = zone
            expr = item.expression
            alias = item.alias
            bare = expr.name if isinstance(expr, Variable) else None
            key = alias or (bare if bare is not None else _safe_alias(item.name.strip()))
            if key in split:
                # Read as its own row of the carried columns, under its own name.
                new_var_table[key] = (key, split[key])
            elif bare is not None and bare in self._var_table:
                original_meta = self._var_table[bare][1]
                new_var_table[key] = (cte_name, original_meta)
                if bare in self._map_array_vars:
                    self._map_array_vars.add(key)
            else:
                new_var_table[key] = (cte_name, None)
                # collect({...}) produces a MAP array — track the alias for UNWIND resolution
                if _collects_maps(expr) and key:
                    self._map_array_vars.add(key)
        self._var_table = new_var_table
        self._cte_sources = set(new_var_table.keys())
        self._temporal_kinds = kinds
        self._temporal_zones = zones
        self._text_vars = texts
        self._list_vars = lists

    def _resolve_node_type(self, labels: list[str]) -> tuple[str | None, str | None]:
        """Return (type_name_key, domain_label). Raises on ambiguity.

        Handles all label combinations regardless of order:
          (n:SalesAnalytics:Orders)  — domain + table
          (n:Orders:SalesAnalytics)  — reversed (AND'd, same result)
          (n:Orders)                 — table only (unique or union)
          (n:SalesAnalytics)         — domain only (union over all tables in domain)
          (n:SalesAnalytics_Orders)  — legacy full type_name (backward compat)
        """
        # Normalize labels to canonical case before lookup
        labels = [self._lm.canonical_label(label) for label in labels]
        # Classify each label
        full_type: list[str] = [label for label in labels if label in self._lm.nodes]
        domain_hits: list[str] = [label for label in labels if label in self._lm.domains]
        table_hits: list[str] = [label for label in labels if label in self._lm.nodes_by_table]
        known = set(full_type) | set(domain_hits) | set(table_hits)
        unknown = [label for label in labels if label not in known]
        if unknown:
            # REQ-778: a label no table has is the user's mistake -- even beside one that is
            # known, which would otherwise widen the read to the whole domain.
            raise unknown_label(unknown[0], self._lm)

        # Legacy: full type_name used directly (e.g. SalesAnalytics_Orders)
        if full_type:
            if len(full_type) > 1:
                raise CypherTranslateError(
                    f"Ambiguous labels — multiple full type labels: {full_type}"
                )
            return full_type[0], domain_hits[0] if domain_hits else None

        # Domain + table (any order): intersect to get unique type_name
        if domain_hits and table_hits:
            domain_set = set(self._lm.domains[domain_hits[0]])
            table_set = set(self._lm.nodes_by_table[table_hits[0]])
            candidates = domain_set & table_set
            if len(candidates) == 1:
                return candidates.pop(), domain_hits[0]
            if len(candidates) > 1:
                raise CypherTranslateError(
                    f"Ambiguous: labels {labels} match multiple types: {sorted(candidates)}"
                )
            raise CypherTranslateError(f"No node type found for labels {labels}")

        # Table only: resolve if unambiguous, otherwise build domain-style union
        if table_hits and not domain_hits:
            candidates = self._lm.nodes_by_table[table_hits[0]]
            if len(candidates) == 1:
                return candidates[0], None
            # Ambiguous table label across domains — treat as ad-hoc domain union
            ad_hoc = f"__tbl_{table_hits[0]}__"
            if ad_hoc not in self._lm.domains:
                from provisa.gql.label_map import CypherLabelMap

                self._lm = CypherLabelMap(
                    nodes=self._lm.nodes,
                    relationships=self._lm.relationships,
                    domains={**self._lm.domains, ad_hoc: candidates},
                    nodes_by_table=self._lm.nodes_by_table,
                )
            return None, ad_hoc

        # Domain only
        if domain_hits:
            return None, domain_hits[0]

        raise unknown_label(labels[0], self._lm)

    def _referenced_var_props(self, var: str) -> list[str]:
        """The property names referenced as ``var.prop`` in the statement, in order."""
        exprs: list[CypherExpr] = []
        for step in self._ast.pipeline:
            if isinstance(step, MatchStep) and step.where:
                exprs.append(step.where.expression)
            elif isinstance(step, WithClause):
                exprs.extend(i.expression for i in step.items)
                if step.where:
                    exprs.append(step.where.expression)
        if self._ast.return_clause:
            exprs.extend(i.expression for i in self._ast.return_clause.items)
        return _properties_of(var, exprs)

    def _node_as_one_value(self, var: str) -> bool:
        """Whether ``var`` is a domain node the statement returns bare and reads no property of:
        each type's arm of its union then carries the node as one JSON value of its own
        properties, not a column for every property of every type."""
        if var not in self._domain_nodes or not self._ast.return_clause:
            return False
        is_bare = any(
            isinstance(item.expression, Variable) and item.expression.name == var
            for item in self._ast.return_clause.items
        )
        return is_bare and not self._referenced_var_props(var)

    def _collect_var_props(self, var: str) -> list[str]:
        """Return ordered list of property names referenced as var.prop in the AST.

        When the var appears bare in RETURN (e.g. ``RETURN n``) with no explicit
        property projections, all properties from the domain's node types are
        included, for a union over its relationships to carry its ends with.
        """
        props = self._referenced_var_props(var)
        if self._node_as_one_value(var):
            domain = self._domain_nodes[var]
            type_labels = (
                list(self._lm.nodes.keys())
                if domain == "__all__"
                else self._lm.domains.get(domain, [])
            )
            _reserved = {"id", "label"}
            all_props: set[str] = set()
            for label in type_labels:
                nm = self._lm.nodes.get(label)
                if nm:
                    all_props.update(k for k in nm.properties.keys() if k not in _reserved)
            props = sorted(all_props)

        return props
