# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9f5b3c-7a1e-4c8d-6f2b-9e4a1c7d3b5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CorrelatedCallMixin — CALL { WITH x MATCH ... } translation.

Translates correlated CALL subqueries to CROSS JOIN LATERAL expressions.
  MATCH (p:Person)
  CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
  RETURN p.name, friend
  →
  SELECT p."name", _call0.friend
  FROM persons AS p
  CROSS JOIN LATERAL (
      SELECT f."name" AS friend
      FROM persons AS f
      WHERE p."person_id" = f."id"
  ) AS _call0

Mixed into _Translator; relies on _lm, _var_table, _param_order, _param_seen.
"""

# Requirements: REQ-573

from __future__ import annotations


import sqlglot.expressions as exp

from provisa.gql.label_map import CypherLabelMap
from provisa.gql.ir import CallSubquery


def _all_optional(call: CallSubquery) -> bool:
    """Whether every MATCH of ``call``'s body is OPTIONAL."""
    from provisa.gql.ir import MatchStep

    matches = [
        m for step in call.body.pipeline if isinstance(step, MatchStep) for m in step.matches
    ]
    return bool(matches) and all(m.optional for m in matches)


def _aggregates(lateral: exp.Expression) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether the lateral's body aggregates its rows into one (a collect, a count, ...)."""
    select = lateral.find(exp.Select)
    if select is None:
        return False
    return any(
        isinstance(n, exp.AggFunc)
        or (isinstance(n, exp.Anonymous) and n.name.lower() in _AGGREGATES)
        for e in select.expressions
        for n in e.walk()
    )


# The aggregates a lowered body names as plain calls (expr_visitor builds ARRAY_AGG by name).
_AGGREGATES = frozenset({"array_agg", "count", "sum", "avg", "min", "max"})


class CorrelatedCallMixin:  # REQ-573
    """Mixin for _Translator: translates correlated CALL { WITH x MATCH ... }."""

    _lm: CypherLabelMap
    _var_table: dict
    _call_var_to_lateral: dict
    _param_order: list
    _param_seen: set
    _params: dict

    def _translate_correlated_calls(
        self,
        call_subqueries: list[CallSubquery],
    ) -> list[dict]:
        """Return CROSS LATERAL join dicts for each correlated CALL subquery."""
        lateral_joins: list[dict] = []
        for i, call in enumerate(call_subqueries):
            if not call.imported_vars:
                continue  # non-correlated: handled by cypher_calls_to_sql_list
            lateral_alias = f"_call{i}"
            lateral_expr = self._build_lateral(call, lateral_alias)
            if lateral_expr is not None:
                # A body of OPTIONAL MATCHes yields a row of nulls where nothing matches, so the
                # outer row is kept: the lateral is a LEFT join, never a CROSS one that drops it.
                # (An aggregating body yields its one row either way.)
                optional = _all_optional(call) and not _aggregates(lateral_expr)
                lateral_joins.append(
                    {
                        "table": lateral_expr,
                        "on": exp.true() if optional else None,
                        "join_type": "LEFT" if optional else "CROSS",
                    }
                )
                # Register return variables so the outer RETURN can qualify them
                if call.body.return_clause:
                    for item in call.body.return_clause.items:
                        var_name = item.alias or item.name.strip()
                        if var_name:
                            self._call_var_to_lateral[var_name] = lateral_alias
        return lateral_joins

    def _build_lateral(
        self,
        call: CallSubquery,
        alias: str,
    ) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Translate one correlated CALL body to a LATERAL subquery."""
        from provisa.gql.translator import _Translator
        from provisa.gql.translator_types import CypherMistake, CypherTranslateError

        # Build inner translator with outer _var_table pre-loaded for imported vars
        outer_bindings = {v: self._var_table[v] for v in call.imported_vars if v in self._var_table}
        if not outer_bindings:
            return None

        inner_translator = _Translator(call.body, self._lm, self._params)
        # Pre-populate inner _var_table and mark vars as lateral-bound
        inner_translator._var_table.update(outer_bindings)
        inner_translator._lateral_bound = set(outer_bindings)  # type: ignore[attr-defined]

        try:
            inner_select, inner_params, _ = inner_translator.translate()
        except CypherMistake:
            # REQ-603, REQ-778: the user's mistake is a refusal of the statement, not a body to
            # run some other way.
            raise
        except CypherTranslateError:
            return None

        # Propagate any new params discovered in inner query
        for p in inner_params:
            if p not in self._param_seen:
                self._param_order.append(p)
                self._param_seen.add(p)

        return exp.alias_(  # type: ignore[return-value]
            exp.Lateral(this=exp.Subquery(this=inner_select)),
            alias=alias,
        )
