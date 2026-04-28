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

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import CallSubquery, MatchClause, PathPattern


class CorrelatedCallMixin:
    """Mixin for _Translator: translates correlated CALL { WITH x MATCH ... }."""

    _lm: CypherLabelMap
    _var_table: dict
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
            lateral_expr = self._build_lateral(call, f"_call{i}")
            if lateral_expr is not None:
                lateral_joins.append({
                    "table": lateral_expr,
                    "on": None,
                    "join_type": "CROSS",
                })
        return lateral_joins

    def _build_lateral(
        self,
        call: CallSubquery,
        alias: str,
    ) -> exp.Expression | None:
        """Translate one correlated CALL body to a LATERAL subquery."""
        from provisa.cypher.translator import _Translator, CypherTranslateError

        # Build inner translator with outer _var_table pre-loaded for imported vars
        outer_bindings = {
            v: self._var_table[v]
            for v in call.imported_vars
            if v in self._var_table
        }
        if not outer_bindings:
            return None

        inner_translator = _Translator(call.body, self._lm, self._params)
        # Pre-populate inner _var_table and mark vars as lateral-bound
        inner_translator._var_table.update(outer_bindings)
        inner_translator._lateral_bound: set[str] = set(outer_bindings)

        try:
            inner_select, inner_params, _ = inner_translator.translate()
        except CypherTranslateError:
            return None

        # Propagate any new params discovered in inner query
        for p in inner_params:
            if p not in self._param_seen:
                self._param_order.append(p)
                self._param_seen.add(p)

        return exp.alias_(
            exp.Lateral(this=exp.Subquery(this=inner_select)),
            alias=alias,
        )
