# Copyright (c) 2026 Kenneth Stott
# Canary: 2609e639-50c0-44e8-83aa-99bff64b248b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Path expressions lowered to SQL (REQ-2135): a relationship pattern whose ``path`` is set.

Every path expression lowers to a STEP RELATION, a subquery of the pairs of nodes it joins, each
row ``(from_type, from_id, to_type, to_id)``. An edge is the union of its relationship's hops; an
inverse swaps the ends; a sequence joins step relations end to start; an alternative is their
union; a repetition is a recursive CTE over its body's step relation, bounded by the query or by
the operator's depth limit. The pattern's two nodes then join the step relation's ends, so a path
expression sits in a MATCH as one relationship does.

A repetition's path mode decides what it keeps: WALK keeps a row per walk, REACH (SPARQL's
property paths) each pair of ends once. TRAIL, ACYCLIC and SIMPLE are refused by name.
"""

# Requirements: REQ-2135, REQ-348

from __future__ import annotations

from typing import cast

import sqlglot.expressions as exp

from provisa.core import settings_registry
from provisa.gql.ir import (
    AltPath,
    EdgeStep,
    MatchClause,
    NegatedEdges,
    NodePattern,
    PathExpr,
    PathMode,
    RelPattern,
    RepeatPath,
    SeqPath,
)
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.path_functions import _apply_hop
from provisa.gql.translator_types import CypherTranslateError

_ENDS = ("from_type", "from_id", "to_type", "to_id")


def _col(name: str, table: str | None = None, quoted: bool = False) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=name, quoted=quoted),
        table=exp.Identifier(this=table) if table else None,
    )


def _table(nm: NodeMapping, alias: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return cast(
        exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        exp.alias_(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            exp.Table(
                this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                db=exp.Identifier(this=nm.schema_name, quoted=True),
                catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
            ),
            alias=alias,
        ),
    )


def _union_all(selects: list[exp.Select]) -> exp.Select | exp.Union:
    result: exp.Select | exp.Union = selects[0]
    for s in selects[1:]:
        result = exp.Union(this=result, expression=s, distinct=False)
    return result


def _meets(left: str, right: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``left`` ends where ``right`` starts."""
    return exp.And(
        this=exp.EQ(this=_col("to_type", left), expression=_col("from_type", right)),
        expression=exp.EQ(this=_col("to_id", left), expression=_col("from_id", right)),
    )


def _cut_at(name: str, limit: int, mode: PathMode) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """True when the walk in CTE ``name`` went past ``limit``. A WALK is cut by any longer walk.
    REACH is cut only by a pair of ends first reached past the limit: when the hop after the limit
    reaches no new pair, no later hop can, so a cycle the limit stops is not a cut."""
    past = (
        exp.select(exp.Literal.number(1))
        .from_(exp.alias_(exp.Table(this=exp.Identifier(this=name)), alias="_past"))
        .where(exp.GT(this=_col("hops", "_past"), expression=exp.Literal.number(limit)))
    )
    if mode == PathMode.REACH:
        same_ends = [exp.EQ(this=_col(c, "_within"), expression=_col(c, "_past")) for c in _ENDS]
        within = (
            exp.select(exp.Literal.number(1))
            .from_(exp.alias_(exp.Table(this=exp.Identifier(this=name)), alias="_within"))
            .where(exp.LTE(this=_col("hops", "_within"), expression=exp.Literal.number(limit)))
            .where(*same_ends)
        )
        past = past.where(exp.Not(this=exp.Exists(this=within)))
    return exp.Exists(this=past)


class PathExprMixin:  # REQ-2135
    """Mixin for _Translator: lowers a relationship pattern's path expression."""

    # Injected by _Translator.__init__
    _lm: CypherLabelMap
    _var_table: dict
    _recursive_ctes: list  # list[(cte_name, exp.Expression)]
    _cut_checks: list
    #: Set per path expression (_translate_path_expr): its ends' keys are held as text.
    _textual_path_keys: bool = False

    def _translate_path_expr(
        self, clause: MatchClause, nodes: list[NodePattern], rel: RelPattern, joined: set[str]
    ) -> list[dict]:
        """The joins of ``(src) path (tgt)``, the start node already in the statement. ``joined``
        holds the variables the statement had joined before this pattern: an end node among them
        is matched where it is rather than joined again."""
        assert rel.path is not None
        if rel.variable:
            raise CypherTranslateError(
                "A path expression binds no relationship variable; name its end nodes instead"
            )
        if rel.mode not in (PathMode.WALK, PathMode.REACH):
            raise CypherTranslateError(
                f"The path mode {rel.mode.value} is not supported; WALK and REACH are"
            )
        if rel.direction != "right":
            raise CypherTranslateError(
                "A path expression runs from its first node to its last; write the other way "
                "round as an inverse, and both ways as an alternative of the two"
            )
        src_node, tgt_node = nodes[0], nodes[1]
        src_nm = self._path_end(src_node)
        tgt_nm = self._path_end(tgt_node)
        # Where a node the path meets has a compound key, every node is told apart by its key
        # text (every key column), and the step relation's id columns hold text throughout.
        reached = [self._lm.nodes[label] for label in self._end_labels(rel.path)]
        self._textual_path_keys = any(len(nm.key_columns) > 1 for nm in [src_nm, tgt_nm, *reached])
        step = self._step_relation(rel.path, rel.mode)

        src_alias = self._end_alias(src_node, src_nm)
        tgt_alias = self._end_alias(tgt_node, tgt_nm)
        step_alias = f"_pe{len(self._recursive_ctes)}_{src_alias}"
        join_type = "LEFT" if clause.optional else "INNER"
        starts = exp.And(
            this=exp.EQ(
                this=_col("from_type", step_alias),
                expression=exp.Literal.string(src_nm.type_name),
            ),
            expression=exp.EQ(
                this=_col("from_id", step_alias),
                expression=self._end_key(src_nm, src_alias),
            ),
        )
        ends = exp.And(
            this=exp.EQ(
                this=_col("to_type", step_alias),
                expression=exp.Literal.string(tgt_nm.type_name),
            ),
            expression=exp.EQ(
                this=self._end_key(tgt_nm, tgt_alias),
                expression=_col("to_id", step_alias),
            ),
        )
        step_table = exp.alias_(exp.Subquery(this=step), alias=step_alias)
        if tgt_node.variable is not None and tgt_node.variable in joined:
            return [
                {
                    "table": step_table,
                    "on": exp.And(this=starts, expression=ends),
                    "join_type": join_type,
                }
            ]
        return [
            {"table": step_table, "on": starts, "join_type": join_type},
            {"table": _table(tgt_nm, tgt_alias), "on": ends, "join_type": join_type},
        ]

    def _end_key(self, nm: NodeMapping, alias: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The key a step relation holds ``alias``'s node by: its key column, or its key text."""
        if self._textual_path_keys:
            from provisa.gql.select_builder import node_key_text

            return node_key_text(alias, nm)
        return _col(nm.id_column, alias, quoted=True)

    def _end_alias(self, node: NodePattern, nm: NodeMapping) -> str:
        if node.variable is None:
            raise CypherTranslateError("Each end of a path expression is a named node")
        return self._var_table[node.variable][0]

    def _path_end(self, node: NodePattern) -> NodeMapping:
        if len(node.labels) != 1 or node.labels[0] not in self._lm.nodes:
            raise CypherTranslateError(
                "Each end of a path expression is a node of one registered label; "
                f"{node.variable or 'a node'} has {node.labels or 'none'}"
            )
        return self._lm.nodes[node.labels[0]]

    # --- step relations ---------------------------------------------------------------------------

    def _step_relation(self, path: PathExpr, mode: PathMode) -> exp.Select | exp.Union:
        if isinstance(path, EdgeStep | NegatedEdges):
            rels = self._rels_of(path)
            if not rels:
                raise CypherTranslateError(
                    f"No relationship of type {path.rel_type!r}"
                    if isinstance(path, EdgeStep)
                    else f"No relationship of a type other than {', '.join(path.rel_types)}"
                )
            return self._hops(rels, path.inverse)
        if isinstance(path, AltPath):
            return _union_all([self._as_select(self._step_relation(o, mode)) for o in path.options])
        if isinstance(path, SeqPath):
            return self._sequence([self._step_relation(p, mode) for p in path.parts])
        return self._repeat(path, mode)

    @staticmethod
    def _as_select(step: exp.Select | exp.Union) -> exp.Select:
        if isinstance(step, exp.Select):
            return step
        return exp.select(*_ENDS).from_(exp.alias_(exp.Subquery(this=step), alias="_alt"))

    def _hops(self, rels: list[RelationshipMapping], inverse: bool) -> exp.Select | exp.Union:
        """One row per edge of these relationships, walked source to target or, inverse, back."""
        selects: list[exp.Select] = []
        for rel in rels:
            src = self._lm.nodes.get(rel.source_label)
            tgt = self._lm.nodes.get(rel.target_label)
            if src is None or tgt is None:
                continue
            a = (exp.Literal.string(src.type_name), self._end_key(src, "_a"))
            b = (exp.Literal.string(tgt.type_name), self._end_key(tgt, "_b"))
            first, second = (b, a) if inverse else (a, b)
            hop = exp.select(
                *(exp.alias_(e, alias=n) for e, n in zip((*first, *second), _ENDS, strict=True))
            ).from_(_table(src, "_a"))
            selects.append(_apply_hop(hop, rel, "_a", tgt, "_b", _table(tgt, "_b")))
        if not selects:
            raise CypherTranslateError("No relationship of the path joins two registered labels")
        return _union_all(selects)

    @staticmethod
    def _sequence(steps: list[exp.Select | exp.Union]) -> exp.Select | exp.Union:
        result = steps[0]
        for step in steps[1:]:
            result = (
                exp.select(
                    exp.alias_(_col("from_type", "_l"), alias="from_type"),
                    exp.alias_(_col("from_id", "_l"), alias="from_id"),
                    exp.alias_(_col("to_type", "_r"), alias="to_type"),
                    exp.alias_(_col("to_id", "_r"), alias="to_id"),
                )
                .from_(exp.alias_(exp.Subquery(this=result), alias="_l"))
                .join(
                    exp.alias_(exp.Subquery(this=step), alias="_r"),
                    on=_meets("_l", "_r"),
                    join_type="INNER",
                )
            )
        return result

    def _repeat(self, path: RepeatPath, mode: PathMode) -> exp.Select | exp.Union:
        """A recursive CTE over the body's step relation, from one repetition up to the bound."""
        by_limit = path.max is None
        limit = settings_registry.value("graph.depth_limit") if by_limit else path.max
        assert limit is not None
        if path.min > limit:
            raise CypherTranslateError(
                f"A path repeated at least {path.min} times can't be walked within {limit} hops"
            )
        body = self._step_relation(path.body, mode)
        name = f"_path{len(self._recursive_ctes)}"
        bound = limit + 1 if by_limit else limit
        cte = exp.Union(
            this=exp.select(*_ENDS, exp.alias_(exp.Literal.number(1), alias="hops")).from_(
                exp.alias_(exp.Subquery(this=body), alias="_b0")
            ),
            expression=exp.select(
                _col("from_type", "_t"),
                _col("from_id", "_t"),
                _col("to_type", "_s"),
                _col("to_id", "_s"),
                exp.alias_(
                    exp.Add(this=_col("hops", "_t"), expression=exp.Literal.number(1)),
                    alias="hops",
                ),
            )
            .from_(exp.alias_(exp.Table(this=exp.Identifier(this=name)), alias="_t"))
            .join(
                exp.alias_(exp.Subquery(this=body.copy()), alias="_s"),
                on=_meets("_t", "_s"),
                join_type="INNER",
            )
            .where(exp.LT(this=_col("hops", "_t"), expression=exp.Literal.number(bound))),
            distinct=False,
        )
        self._recursive_ctes.append((name, cte))

        kept = exp.select(*_ENDS).from_(exp.Table(this=exp.Identifier(this=name)))
        conditions: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if path.min > 1:
            conditions.append(exp.GTE(this=_col("hops"), expression=exp.Literal.number(path.min)))
        if by_limit:
            conditions.append(exp.LTE(this=_col("hops"), expression=exp.Literal.number(limit)))
            self._cut_checks.append(_cut_at(name, limit, mode))
        for c in conditions:
            kept = kept.where(c)
        step: exp.Select | exp.Union = kept
        if path.min == 0:
            step = _union_all([kept, *self._identity(path.body)])
        if mode == PathMode.REACH:
            step = (
                exp.select(*_ENDS)
                .distinct()
                .from_(exp.alias_(exp.Subquery(this=step), alias="_reach"))
            )
        return step

    def _identity(self, body: PathExpr) -> list[exp.Select]:
        """The zero-length rows of a repetition: each node of a label its body starts or ends at,
        paired with itself."""
        selects = []
        for label in sorted(self._end_labels(body)):
            nm = self._lm.nodes[label]
            selects.append(
                exp.select(
                    exp.alias_(exp.Literal.string(nm.type_name), alias="from_type"),
                    exp.alias_(self._end_key(nm, "_n"), alias="from_id"),
                    exp.alias_(exp.Literal.string(nm.type_name), alias="to_type"),
                    exp.alias_(self._end_key(nm, "_n"), alias="to_id"),
                ).from_(_table(nm, "_n"))
            )
        return selects

    def _end_labels(self, path: PathExpr) -> set[str]:
        if isinstance(path, EdgeStep | NegatedEdges):
            return {
                label
                for r in self._rels_of(path)
                for label in (r.source_label, r.target_label)
                if label in self._lm.nodes
            }
        parts: tuple[PathExpr, ...]
        if isinstance(path, AltPath):
            parts = path.options
        elif isinstance(path, SeqPath):
            parts = path.parts
        else:
            parts = (path.body,)
        return set().union(*(self._end_labels(p) for p in parts))

    def _rels_of(self, path: EdgeStep | NegatedEdges) -> list[RelationshipMapping]:
        if isinstance(path, EdgeStep):
            return [r for r in self._lm.relationships.values() if r.rel_type == path.rel_type]
        return [
            r
            for r in self._lm.relationships.values()
            if r.rel_type not in path.rel_types and not r.constant_source
        ]
