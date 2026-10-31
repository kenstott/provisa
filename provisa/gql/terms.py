# Copyright (c) 2026 Kenneth Stott
# Canary: 56fb6930-16fb-4dd2-be06-d5fbd63cd133
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RDF terms, variable predicates and CONSTRUCT lowered to SQL (REQ-2135).

An RDF term of the answer is one JSON value:

- a subject, ``{"node": {"domain": d, "table": t, "key": [k1, ...]}}`` -- its table's identity and
  its key values, as typed as the table holds them; the answer's writer builds the IRI from them
  (:mod:`provisa.gql.iri`), so no IRI is built, or encoded, in SQL;
- an IRI, ``{"iri": "http://..."}``;
- a glossary term, ``{"term": name}`` -- the writer builds its vocabulary IRI (REQ-2163);
- a literal, ``{"literal": v}``, with ``"datatype"`` or ``"lang"`` when known.

A variable predicate the front end leaves as one (:meth:`provisa.sparql_query.compiler._Lowering.
predicate_reads` makes most a UNION ALL part per predicate) is a union, made at compile time, of
one branch per mapped predicate of the subject's label, each a row ``(s_id0..., p, o)`` joined on
the subject's whole key; more branches than graph.union_limit are refused, naming it. A CONSTRUCT is one SELECT of ``(s, p, o)`` per template triple over the
statement's matches, the selects put together with UNION ALL: an RDF graph is a set, and its writer
keeps each triple once.
"""

# Requirements: REQ-2135

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp

from provisa.core import settings_registry
from provisa.gql.expr_ast import (
    AxiomObject,
    CypherExpr,
    IriConst,
    Property,
    Term,
    TermIri,
    Variable,
)
from provisa.gql.group_by import _has_aggregate
from provisa.gql.iri import iri_base
from provisa.gql.ir import CypherAST, MatchClause, Predicate, TriplePattern, TripleTemplate
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.path_functions import _apply_hop
from provisa.gql.translator_types import CypherTranslateError, GraphVarKind


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


def _json(**pairs: exp.Expression) -> exp.JSONObject:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return exp.JSONObject(
        expressions=[
            exp.JSONKeyValue(this=exp.Literal.string(k), expression=v) for k, v in pairs.items()
        ]
    )


def axiom_table_iri(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    table_id: exp.Expression, table_iris: dict[int, str]
) -> exp.Expression:
    """The class IRI of the table ``table_id`` names (REQ-2163), NULL for a row of no table or of a
    table the role does not read."""
    if not table_iris:
        return exp.null()
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=table_id.copy(), expression=exp.Literal.number(i)),
                true=exp.Literal.string(iri),
            )
            for i, iri in table_iris.items()
        ],
        default=exp.null(),
    )


def _present(value: exp.Expression, term) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``term(value)``, NULL where ``value`` is (an OPTIONAL that matched nothing)."""
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=value.copy(), expression=exp.null()), true=exp.null())],
        default=term(value),
    )


def iri_term(iri: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return _json(iri=exp.Literal.string(iri))


def literal_term(
    value: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    datatype: str | None = None,
    lang: str | None = None,
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    pairs: dict[str, exp.Expression] = {"literal": value}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if datatype is not None:
        pairs["datatype"] = exp.Literal.string(datatype)
    if lang is not None:
        pairs["lang"] = exp.Literal.string(lang)
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=value.copy(), expression=exp.null()), true=exp.null())],
        default=_json(**pairs),
    )


def subject_key(nm: NodeMapping) -> list[str]:
    """The columns a subject of ``nm`` is named by, refused, by the table's name and not the
    column's, where the role is not served the whole of its key: no IRI of it can be built without
    reading a column hidden from the role, and any other column would name another thing."""
    if not nm.key_readable:
        raise CypherTranslateError(f"{nm.table_name}'s key is not readable by this role")
    return nm.pk_columns or [nm.id_column]


def node_term(alias: str, nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A subject as its table's identity and key values; NULL where an OPTIONAL match found none.
    Its IRI needs server.public_url, so a statement that answers with one is refused while that is
    unset."""
    iri_base()  # refuses here, at compile time, naming the setting
    if not nm.domain_id:
        raise CypherTranslateError(
            f"{nm.table_name} belongs to no domain, and a subject's IRI names its domain"
        )
    keys = [_col(k, alias, quoted=True) for k in subject_key(nm)]
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=keys[0].copy(), expression=exp.null()), true=exp.null())],
        default=_json(
            node=_json(
                domain=exp.Literal.string(nm.domain_id),
                table=exp.Literal.string(nm.table_name),
                key=exp.JSONArray(expressions=keys),
            )
        ),
    )


LIMITED_UNION_FACTOR = 10  # REQ-2135: a LIMIT query is allowed this many times graph.union_limit


def union_allowance(limited: bool) -> tuple[int, int]:  # REQ-2135
    """The branches a union may have, as (graph.union_limit, allowed). A query with LIMIT and no
    ORDER BY, DISTINCT or aggregate (``limited``) is allowed ten times the limit: the engine
    stops the UNION ALL once it has its rows. Past that every branch is still compiled."""
    limit = settings_registry.value("graph.union_limit")
    return limit, limit * LIMITED_UNION_FACTOR if limited else limit


def union_refusal(what: str, limit: int, allowed: int) -> str:  # REQ-2135
    """What a refused union says: what it needs, and the limit it hit -- the plain one or, for a
    LIMIT query, ten times it."""
    if allowed > limit:
        return (
            f"{what}, past {LIMITED_UNION_FACTOR} times the graph union limit of {limit} "
            f"({allowed}), which a query with a LIMIT is allowed"
        )
    return f"{what}, past the graph union limit of {limit}"


class TermsMixin:  # REQ-2135
    """Mixin for _Translator: RDF terms, variable predicates and CONSTRUCT."""

    # Injected by _Translator.__init__
    _lm: CypherLabelMap
    _var_table: dict
    _term_vars: dict  # variable → the column holding its term
    _graph_vars: dict
    _ast: CypherAST

    if TYPE_CHECKING:

        def _lower_expr(self, expr: CypherExpr) -> exp.Expression: ...  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # --- terms -------------------------------------------------------------------------------

    def _term_sql(self, node: Term) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        value = node.value
        if isinstance(value, IriConst):
            return iri_term(value.iri)
        if isinstance(value, TermIri):
            return _present(self._lower_expr(value.value), lambda name: _json(term=name))
        if isinstance(value, AxiomObject):
            # REQ-2163: an ontology._axioms row's object, whichever kind it holds.
            def col(name: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                return self._lower_expr(Property(Variable(value.node), name))

            term = col("object")
            iri = exp.Coalesce(
                this=col("object_iri"),
                expressions=[axiom_table_iri(col("object_table_id"), value.table_iris)],
            )
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Not(this=exp.Is(this=term.copy(), expression=exp.null())),
                        true=_json(term=term),
                    ),
                    exp.If(
                        this=exp.Not(this=exp.Is(this=iri.copy(), expression=exp.null())),
                        true=_json(iri=iri),
                    ),
                ],
                default=literal_term(col("object_literal")),
            )
        if isinstance(value, Variable):
            if value.name in self._term_vars:
                return self._term_vars[value.name].copy()
            bound = self._var_table.get(value.name)
            if bound is not None and bound[1] is not None:
                return node_term(bound[0], bound[1])
        return literal_term(self._lower_expr(value), node.datatype, node.lang)

    # --- variable predicates -----------------------------------------------------------------

    def _translate_triple(self, clause: MatchClause, triple: TriplePattern) -> list[dict]:
        """The join of a variable predicate's union onto its subject, binding its two variables."""
        bound = self._var_table.get(triple.subject)
        if bound is None or bound[1] is None:
            raise CypherTranslateError(
                f"A variable predicate's subject {triple.subject} is a node of one registered "
                "label, bound by a pattern before it"
            )
        alias, nm = bound
        branches = [b for p in triple.candidates for b in self._predicate_branches(nm, p)]
        ast = self._ast
        limit, allowed = union_allowance(
            ast.limit is not None
            and not ast.order_by
            and not (
                ast.return_clause is not None
                and (
                    ast.return_clause.distinct
                    or any(_has_aggregate(i.expression) for i in ast.return_clause.items)
                )
            )
        )
        if len(branches) > allowed:
            raise CypherTranslateError(
                union_refusal(
                    f"A variable predicate on {nm.label} reads {len(branches)} mapped predicates",
                    limit,
                    allowed,
                )
                + "; name the predicate, or raise the limit (graph.union_limit)"
            )
        if not branches:
            raise CypherTranslateError(f"{nm.label} has no mapped predicate a variable can match")
        union: exp.Select | exp.Union = branches[0]
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        tr = f"_tr{len(self._term_vars) // 2}_{triple.subject}"
        on: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for i, column in enumerate(subject_key(nm)):
            eq = exp.EQ(this=_col(f"s_id{i}", tr), expression=_col(column, alias, quoted=True))
            on = eq if on is None else exp.And(this=on, expression=eq)
        for var in (triple.predicate, triple.object):
            if var in self._term_vars or var in self._var_table:
                raise CypherTranslateError(f"{var} is bound twice")
        self._term_vars[triple.predicate] = _col("p", tr)
        self._term_vars[triple.object] = _col("o", tr)
        return [
            {
                "table": exp.alias_(exp.Subquery(this=union), alias=tr),
                "on": on,
                "join_type": "LEFT" if clause.optional else "INNER",
            }
        ]

    def _predicate_branches(self, nm: NodeMapping, p: Predicate) -> list[exp.Select]:
        if sum(x is not None for x in (p.property, p.rel_type, p.constant)) != 1:
            raise CypherTranslateError(
                f"The predicate <{p.iri}> reads a property, an edge or a constant, and only one"
            )
        s_id = [
            exp.alias_(_col(column, "_s", quoted=True), alias=f"s_id{i}")
            for i, column in enumerate(subject_key(nm))
        ]
        if p.constant is not None:
            return [
                exp.select(
                    *s_id,
                    exp.alias_(iri_term(p.iri), alias="p"),
                    exp.alias_(iri_term(p.constant), alias="o"),
                ).from_(_table(nm, "_s"))
            ]
        if p.property is not None:
            column = nm.properties.get(p.property)
            if column is None:
                raise CypherTranslateError(f"{nm.label} has no property {p.property}")
            value = _col(column, "_s", quoted=True)
            return [
                exp.select(
                    *s_id,
                    exp.alias_(iri_term(p.iri), alias="p"),
                    exp.alias_(literal_term(value, p.datatype), alias="o"),
                )
                .from_(_table(nm, "_s"))
                .where(exp.Not(this=exp.Is(this=value.copy(), expression=exp.null())))
            ]
        rels = [
            r
            for r in self._lm.relationships.values()
            if r.rel_type == p.rel_type and r.source_label == nm.type_name
        ]
        if not rels:
            raise CypherTranslateError(f"{nm.label} has no {p.rel_type} relationship")
        out = []
        for rel in rels:
            tgt = self._lm.nodes[rel.target_label]
            if not tgt.key_readable:
                continue  # its object can't be named: the edge is left out, as its table is
            hop = exp.select(
                *(c.copy() for c in s_id),
                exp.alias_(iri_term(p.iri), alias="p"),
                exp.alias_(node_term("_o", tgt), alias="o"),
            ).from_(_table(nm, "_s"))
            out.append(_apply_hop(hop, rel, "_s", tgt, "_o", _table(tgt, "_o")))
        return out

    # --- CONSTRUCT ---------------------------------------------------------------------------

    def _construct_selects(self, templates: list[TripleTemplate]) -> list[list[exp.Expr]]:
        """One select list ``(s, p, o)`` per template triple, each column a term."""
        for name in ("s", "p", "o"):
            self._graph_vars[name] = GraphVarKind.TERM
        out = []
        for t in templates:
            out.append(
                [
                    exp.alias_(self._term_sql(_as_term(e)), alias=name)
                    for name, e in (("s", t.subject), ("p", t.predicate), ("o", t.object))
                ]
            )
        return out


def _as_term(e: CypherExpr) -> Term:
    return e if isinstance(e, Term) else Term(e)
