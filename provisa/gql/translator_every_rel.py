# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2f8e61-9a3c-4b7e-8c1d-2e6f0a9b4c73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A hop no one pair of tables reads, joined to the nodes the statement already holds (REQ-345).

A relationship with an end the pattern names no type of is read from the union of every
relationship it may be (translator_union._build_all_rels_union). A statement reading only that
hop reads the union as its FROM; this mixin reads every other such hop: one whose end is a node
an earlier MATCH, an earlier hop or a WITH holds, a self-loop ``(n)-[r]->(n)``, a relationship
variable already bound, and a pattern of a later MATCH. Each hop is its own union under its own
alias, and an end the statement already holds is a condition on that node's identity -- the
same ``label|key`` text every node value carries (select_builder.node_identity) -- never a
second binding of its variable. A self-loop is that condition between the hop's two ends.

Within one MATCH a relationship is matched once (openCypher's relationship uniqueness): two hops
of it never bind the same edge. An OPTIONAL MATCH of several such hops is matched whole: its
hops are joined to each other inside one derived table, which the statement joins where every
end it already holds agrees, so where it does not, each of its variables is null.

Methods operate on _Translator state via self; mixed into _Translator, never instantiated alone.
"""

# Requirements: REQ-345

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp

from provisa.gql.select_builder import node_identity
from provisa.gql.translator_union import key_names
from provisa.gql.translator_types import CypherTranslateError

if TYPE_CHECKING:
    from provisa.gql.ir import MatchClause, NodePattern, RelPattern
    from provisa.gql.label_map import CypherLabelMap, NodeMapping


def _value_of(column: str, member: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``member`` of the node or edge value a union's ``column`` carries, as text."""
    return exp.Anonymous(
        this="JSON_EXTRACT_SCALAR",
        expressions=[exp.column(column), exp.Literal.string(f"$.{member}")],
    )


def _same_edge(left: str, right: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether two union columns carry the same edge: its id, of its type."""
    return exp.And(
        this=exp.EQ(this=_value_of(left, "id"), expression=_value_of(right, "id")),
        expression=exp.EQ(this=_value_of(left, "type"), expression=_value_of(right, "type")),
    )


@dataclass
class _Hop:
    """One hop read from its own union: its join, and its conditions on rows it does not read."""

    join: dict
    #: Conditions between this hop and an earlier hop of the same pattern, or between its ends.
    inner: list[exp.Expression] = field(default_factory=list)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    #: Conditions on a node, or an edge, the statement held before the pattern.
    outer: list[exp.Expression] = field(default_factory=list)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


class _EveryRelMixin:  # mixin for _Translator
    if TYPE_CHECKING:
        _lm: CypherLabelMap
        _var_table: dict[str, tuple[str, NodeMapping | None]]
        _domain_nodes: dict[str, str]
        _passthrough_vars: set[str]
        _all_rels_rel_vars: set[str]
        _all_rels_node_vars: set[str]
        _rel_var_types: dict[str, str]
        _held_end_conditions: list[exp.Expression]
        _all_rels_src_col: str | None
        _all_rels_rel_col: str | None
        _all_rels_tgt_col: str | None

        def _build_all_rels_union(
            self,
            src_var: str | None,
            rel_var: str | None,
            tgt_var: str | None,
            src_domain: str | None = None,
            tgt_domain: str | None = None,
            *,
            src_type: str | None = None,
            tgt_type: str | None = None,
            rel_type: str | None = None,
            alias: str = "_all_rels",
            undirected: bool = False,
            src_props_of: str | None = None,
            tgt_props_of: str | None = None,
            loop: bool = False,
            src_keys: bool = False,
            tgt_keys: bool = False,
        ) -> exp.Expression: ...

        def _node_alias(self, node, nm: NodeMapping, *, as_source: bool) -> str: ...

        def _resolve_node_type(self, labels: list[str]) -> tuple[str | None, str | None]: ...

    def _every_rel_state(self) -> None:
        """The state a statement's hops read from their own unions share, set once per build."""
        #: A union column -> the alias of the subquery that gives it (select_builder).
        self._all_rels_var_alias = getattr(self, "_all_rels_var_alias", {})
        #: Path variable -> its hops in pattern order, each (first node, edge, last node) column.
        self._every_rel_paths: dict[str, list[tuple[str, str, str]]] = getattr(
            self, "_every_rel_paths", {}
        )
        self._every_rel_count: int = getattr(self, "_every_rel_count", 0)
        #: The edge columns of the MATCH being read (relationship uniqueness): (column, clause).
        self._every_rel_edges: list[tuple[str, int]] = getattr(self, "_every_rel_edges", [])
        #: id(NodePattern) -> the union column an anonymous node of the pattern was read into.
        self._every_rel_anon: dict[int, str] = getattr(self, "_every_rel_anon", {})
        #: The hops of the clause being read, and the union columns they introduced.
        self._every_rel_hops: list[_Hop] = getattr(self, "_every_rel_hops", [])
        self._every_rel_clause_cols: set[str] = getattr(self, "_every_rel_clause_cols", set())

    def _start_every_rel_match(self) -> None:
        """A new MATCH: its relationships are unique among themselves, not against earlier ones."""
        self._every_rel_state()
        self._every_rel_edges = []

    def _start_every_rel_clause(self) -> None:
        self._every_rel_state()
        self._every_rel_hops = []
        self._every_rel_clause_cols = set()

    def _is_union_value(self, var: str) -> bool:
        """Whether ``var`` is a node or an edge a union of every relationship carries, as a value
        of no one table (here, or carried on by a WITH)."""
        if var in self._all_rels_rel_vars:
            return True
        return var in self._all_rels_node_vars and self._var_table.get(var, (None, None))[1] is None

    def _is_value_node(self, node: "NodePattern") -> bool:
        """Whether ``node`` is held as the node value a union carries, of no one table."""
        if node.variable is None:
            return id(node) in getattr(self, "_every_rel_anon", {})
        return node.variable in self._all_rels_node_vars

    def _path_identity(self, path_var: str) -> "exp.Expression | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The identity of a path read from unions of every relationship: its nodes' identities
        and its edges', in order (null where it matched none). None for any other path."""
        hops = self._every_rel_paths.get(path_var) if hasattr(self, "_every_rel_paths") else None
        alias = getattr(self, "_all_rels_alias", None)
        ends = getattr(self, "_path_vars", {}).get(path_var, ("", "", False))
        if not hops and alias and not (ends[0] and ends[1]):
            # The statement's one read of every relationship (translator_union).
            cols = (self._all_rels_src_col, self._all_rels_rel_col, self._all_rels_tgt_col)
            if None in cols:
                return None
            hops = [cast("tuple[str, str, str]", cols)]
        if not hops:
            return None
        aliases = {**getattr(self, "_all_rels_var_alias", {})}

        def member(col: str, name: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            return exp.Anonymous(
                this="JSON_EXTRACT_SCALAR",
                expressions=[
                    exp.column(col, table=aliases.get(col, alias)),
                    exp.Literal.string(f"$.{name}"),
                ],
            )

        parts = [member(hops[0][0], "id")]
        for _, edge, last in hops:
            parts += [member(edge, "id"), member(edge, "type"), member(last, "id")]
        identity = parts[0]
        for part in parts[1:]:
            identity = exp.DPipe(
                this=exp.DPipe(this=identity, expression=exp.Literal.string("|")), expression=part
            )
        return identity

    def _named_type(self, node: "NodePattern") -> "NodeMapping | None":
        """The one table ``node`` is of, where the statement or its labels name one."""
        if node.variable:
            nm = self._var_table.get(node.variable, (None, None))[1]
            if nm is not None or node.variable in self._all_rels_node_vars:
                return nm
        if node.labels:
            type_label, _ = self._resolve_node_type(node.labels)
            return self._lm.nodes.get(type_label) if type_label else None
        return None

    def _held_row(
        self, node: "NodePattern", nm: "NodeMapping | None", *, as_source: bool
    ) -> "tuple[str, NodeMapping] | None":
        """The alias and the table of a node the statement holds as a row of one table, which a
        hop joins on that table's key; None for a node held as a value."""
        var = node.variable
        if nm is None:
            return None
        if var is None:
            if id(node) in self._every_rel_anon:
                return None
            return self._node_alias(node, nm, as_source=as_source), nm
        if var in self._all_rels_node_vars or var in self._domain_nodes:
            return None
        held = self._var_table.get(var)
        if held is None or held[1] is None or var in getattr(self, "_cte_sources", set()):
            return None
        return held[0], held[1]

    def _held_identity(
        self,
        node: "NodePattern",
        nm: "NodeMapping | None",
        *,
        as_source: bool,
    ) -> "tuple[exp.Expression, bool]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The identity of a node the statement already holds, and whether an earlier hop of
        the clause being read is what holds it."""
        var = node.variable
        if var is None:
            col = self._every_rel_anon.get(id(node))
            if col is not None:
                return _value_of(col, "id"), col in self._every_rel_clause_cols
            if nm is not None:
                return node_identity(self._node_alias(node, nm, as_source=as_source), nm), False
        elif var in self._all_rels_node_vars:
            return _value_of(var, "id"), var in self._every_rel_clause_cols
        elif var in self._domain_nodes:
            return exp.column("__id", table=var), False
        elif nm is not None:
            return node_identity(self._var_table[var][0], nm), False
        raise CypherTranslateError(
            f"A relationship from or to {var or 'an anonymous node'} is read where the statement "
            "holds that node as a node value"
        )

    def _every_rel_hop(
        self,
        rel: "RelPattern",
        src_node: "NodePattern",
        tgt_node: "NodePattern",
        clause: "MatchClause",
        clause_index: int,
        from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: "list[dict]",
        *,
        held_source: bool,
        held_target: bool,
        replaceable: bool,
        src_nm: "NodeMapping | None",
        tgt_nm: "NodeMapping | None",
    ) -> "exp.Expression | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Read ``rel`` from its own union, its held ends conditions on their identities. The
        union is the statement's FROM where ``replaceable`` (the pattern's first node is all the
        statement reads yet); else it is joined. Returns the FROM."""
        self._every_rel_state()
        if rel.variable_length:
            raise CypherTranslateError(
                "A variable-length relationship from or to a node the statement already holds is "
                "read between labelled nodes"
            )
        session = self._session_hop(rel)  # type: ignore[attr-defined]
        if len(rel.types) > 1 and not session:
            raise CypherTranslateError(
                "A relationship of several types between nodes the pattern names no type of is "
                "read one type at a time"
            )
        self._every_rel_count += 1
        alias = f"_all_rels_{self._every_rel_count}"
        # The union's source end is the pattern's first node, or its last where it reads backward.
        backward = rel.direction == "left"
        # A self-loop's last node is its first: one row on both ends of the edge (the union's
        # own condition), held where the first is.
        loop = tgt_node.variable is not None and tgt_node.variable == src_node.variable
        ends = [(src_node, held_source, src_nm, True), (tgt_node, held_target, tgt_nm, False)]
        if loop:
            ends = [(src_node, held_source, src_nm, True), (tgt_node, False, src_nm, False)]
        elif backward:
            ends.reverse()
        inner: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        outer: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        cols: list[str] = []
        props_of: list[str | None] = []
        types: list[str | None] = []
        domains: list[str | None] = []
        new_vars: list[str] = []
        keyed: list[bool] = []
        for side, (node, held, nm, as_source) in zip(("s", "t"), ends, strict=True):
            var = node.variable
            keyed.append(False)
            if held or (loop and side == "t") or var is None:
                col = f"_{alias}_{side}"
                props_of.append(var)
                # A graph session's hop reads each end as its node value, held by its identity.
                held_row = (
                    self._held_row(node, nm, as_source=as_source) if held and not session else None
                )
                if held_row is not None:
                    # A node of one table the statement holds: joined on that table's key, as a
                    # second read of its row (REQ-603), never on a value built from it.
                    held_alias, held_nm = held_row
                    keyed[-1] = True
                    keys = [
                        exp.EQ(
                            this=exp.column(f"{col}__key{i}"),
                            expression=exp.column(k, table=held_alias, quoted=True),
                        )
                        for i, k in enumerate(key_names(held_nm))
                    ]
                    joined: exp.Expression = keys[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                    for key in keys[1:]:
                        joined = exp.And(this=joined, expression=key)
                    outer.append(joined)
                elif held:
                    identity, of_clause = self._held_identity(node, nm, as_source=as_source)
                    cond = exp.EQ(this=_value_of(col, "id"), expression=identity)
                    (inner if of_clause else outer).append(cond)
                if var is None:
                    self._every_rel_anon[id(node)] = col
                domains.append(None)
            else:
                col = var
                props_of.append(None)
                new_vars.append(var)
                domain = self._domain_nodes.get(var)
                domains.append(domain if domain != "__all__" else None)
            cols.append(col)
            types.append(nm.type_name if nm is not None else None)

        rel_col = rel.variable or f"_{alias}_r"
        rel_bound = rel.variable is not None and (
            rel.variable in self._all_rels_rel_vars or rel.variable in self._rel_var_types
        )
        if rel_bound:
            if rel.variable in self._rel_var_types:
                raise CypherTranslateError(
                    f"The relationship {rel.variable}, bound to a relationship between labelled "
                    "nodes, is matched again between labelled nodes"
                )
            rel_col = f"_{alias}_r"
            same = _same_edge(rel_col, rel.variable)  # type: ignore[arg-type]
            (inner if rel.variable in self._every_rel_clause_cols else outer).append(same)
        for earlier, of_clause in self._every_rel_edges:
            # Relationship uniqueness: no two hops of one MATCH bind the same edge.
            distinct = exp.Not(this=exp.paren(_same_edge(rel_col, earlier)))
            (inner if of_clause == clause_index else outer).append(distinct)
        self._every_rel_edges.append((rel_col, clause_index))

        if session:
            # A graph session's relationships, every one a row of its one relation (path_walk).
            union = self._build_session_hop(  # type: ignore[attr-defined]
                cols[0],
                rel_col,
                cols[1],
                types=list(rel.types) or None,
                src_type=types[0],
                tgt_type=types[1],
                src_domain=domains[0],
                tgt_domain=domains[1],
                alias=alias,
                undirected=rel.direction == "none",
                loop=loop,
            )
        else:
            union = self._build_all_rels_union(
                cols[0],
                rel_col,
                cols[1],
                domains[0],
                domains[1],
                src_type=types[0],
                tgt_type=types[1],
                rel_type=rel.types[0].upper() if rel.types else None,
                alias=alias,
                undirected=rel.direction == "none",
                src_props_of=props_of[0],
                tgt_props_of=props_of[1],
                loop=loop,
                src_keys=keyed[0],
                tgt_keys=keyed[1],
            )
        for var in new_vars:
            self._domain_nodes.pop(var, None)
            self._var_table[var] = (var, None)
            self._passthrough_vars.add(var)
            self._all_rels_node_vars.add(var)
        if not rel_bound and rel.variable:
            self._passthrough_vars.add(rel.variable)
            self._all_rels_rel_vars.add(rel.variable)
        for col in (*cols, rel_col):
            self._all_rels_var_alias[col] = alias
            self._every_rel_clause_cols.add(col)

        if clause.variable:
            first, last = (cols[1], cols[0]) if backward else (cols[0], cols[1])
            self._every_rel_paths.setdefault(clause.variable, []).append((first, rel_col, last))

        if replaceable:
            self._held_end_conditions.extend(inner + outer)
            return union
        conds = inner + outer
        if clause.optional:
            join = {
                "table": union,
                "on": exp.and_(*conds) if conds else exp.true(),
                "join_type": "LEFT",
            }
        elif conds:
            join = {"table": union, "on": exp.and_(*conds), "join_type": "INNER"}
        else:
            # A pattern sharing no node with what the statement holds: beside every row of it,
            # as any disconnected pattern is (#306).
            join = {"table": union, "on": None, "join_type": "CROSS"}
        joins.append(join)
        self._every_rel_hops.append(_Hop(join, inner, outer))
        return from_expr

    def _group_every_rel_clause(self, clause: "MatchClause", joins: "list[dict]") -> None:
        """An OPTIONAL MATCH of several hops read from their own unions, matched whole: the hops
        joined to each other inside one derived table, which is joined where the nodes the
        statement held before it agree."""
        hops = self._every_rel_hops
        if not clause.optional or len(hops) < 2:
            return
        first = hops[0].join
        start = next(i for i, j in enumerate(joins) if j is first)
        if len(joins) - start != len(hops) or any(
            j is not h.join for j, h in zip(joins[start:], hops, strict=True)
        ):
            raise CypherTranslateError(
                "An OPTIONAL MATCH reading relationships between nodes it names no type of reads "
                "no relationship between labelled nodes beside them"
            )
        body = exp.select(exp.Star()).from_(first["table"])
        for hop in hops[1:]:
            on = exp.and_(*hop.inner) if hop.inner else exp.true()
            body = body.join(hop.join["table"], on=on, join_type="INNER")
        if hops[0].inner:
            body = body.where(exp.and_(*hops[0].inner))
        self._every_rel_count += 1
        alias = f"_all_rels_{self._every_rel_count}"
        for col in list(self._all_rels_var_alias):
            if col in self._every_rel_clause_cols:
                self._all_rels_var_alias[col] = alias
        outer = [c for h in hops for c in h.outer]
        joins[start:] = [
            {
                "table": exp.alias_(exp.Subquery(this=body), alias=alias),
                "on": exp.and_(*outer) if outer else exp.true(),
                "join_type": "LEFT",
            }
        ]
