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
- a literal, ``{"literal": v}``, with ``"datatype"`` or ``"lang"`` when known.

A variable predicate (``?s ?p ?o``) is a union, made at compile time, of one branch per mapped
predicate of the subject's label, each a row ``(s_id, p, o)``; more branches than graph.union_limit
are refused, naming it. A CONSTRUCT is one SELECT of ``(s, p, o)`` per template triple over the
statement's matches, the selects put together with UNION ALL: an RDF graph is a set, and its writer
keeps each triple once.
"""

# Requirements: REQ-2135

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp

from provisa.core import settings_registry
from provisa.gql.expr_ast import CypherExpr, IriConst, Term, Variable
from provisa.gql.iri import iri_base
from provisa.gql.ir import MatchClause, Predicate, TriplePattern, TripleTemplate
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


def node_term(alias: str, nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A subject as its table's identity and key values; NULL where an OPTIONAL match found none.
    Its IRI needs server.public_url, so a statement that answers with one is refused while that is
    unset."""
    iri_base()  # refuses here, at compile time, naming the setting
    if not nm.domain_id:
        raise CypherTranslateError(
            f"{nm.table_name} belongs to no domain, and a subject's IRI names its domain"
        )
    keys = [_col(k, alias, quoted=True) for k in (nm.pk_columns or [nm.id_column])]
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


class TermsMixin:  # REQ-2135
    """Mixin for _Translator: RDF terms, variable predicates and CONSTRUCT."""

    # Injected by _Translator.__init__
    _lm: CypherLabelMap
    _var_table: dict
    _term_vars: dict  # variable → the column holding its term
    _graph_vars: dict

    if TYPE_CHECKING:

        def _lower_expr(self, expr: CypherExpr) -> exp.Expression: ...  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # --- terms -------------------------------------------------------------------------------

    def _term_sql(self, node: Term) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        value = node.value
        if isinstance(value, IriConst):
            return iri_term(value.iri)
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
        limit = settings_registry.value("graph.union_limit")
        if len(branches) > limit:
            raise CypherTranslateError(
                f"A variable predicate on {nm.label} reads {len(branches)} mapped predicates, past "
                f"the graph union limit of {limit}; name the predicate, or raise the limit "
                "(graph.union_limit)"
            )
        if not branches:
            raise CypherTranslateError(f"{nm.label} has no mapped predicate a variable can match")
        union: exp.Select | exp.Union = branches[0]
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        tr = f"_tr{len(self._term_vars) // 2}_{triple.subject}"
        for var in (triple.predicate, triple.object):
            if var in self._term_vars or var in self._var_table:
                raise CypherTranslateError(f"{var} is bound twice")
        self._term_vars[triple.predicate] = _col("p", tr)
        self._term_vars[triple.object] = _col("o", tr)
        return [
            {
                "table": exp.alias_(exp.Subquery(this=union), alias=tr),
                "on": exp.EQ(
                    this=_col("s_id", tr), expression=_col(nm.id_column, alias, quoted=True)
                ),
                "join_type": "LEFT" if clause.optional else "INNER",
            }
        ]

    def _predicate_branches(self, nm: NodeMapping, p: Predicate) -> list[exp.Select]:
        if sum(x is not None for x in (p.property, p.rel_type, p.constant)) != 1:
            raise CypherTranslateError(
                f"The predicate <{p.iri}> reads a property, an edge or a constant, and only one"
            )
        s_id = exp.alias_(_col(nm.id_column, "_s", quoted=True), alias="s_id")
        if p.constant is not None:
            return [
                exp.select(
                    s_id,
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
                    s_id,
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
            hop = exp.select(
                s_id.copy(),
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
