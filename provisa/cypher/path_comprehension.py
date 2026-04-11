# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7b2a9e-6c4f-4b1a-8e5d-2f9c7a3b6d4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PathComprehensionMixin — pattern comprehension translation.

Translates Cypher pattern comprehensions to correlated ARRAY subqueries:
  [(a)-[:KNOWS]->(b) | b.name]
  → ARRAY(SELECT b."name" FROM "cat"."schema"."persons" AS b WHERE a."person_id" = b."id")

Mixed into _Translator; relies on _lm and _var_table.
"""

from __future__ import annotations

import re

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping


def _prop(expr: str) -> str:
    """Rewrite ident.prop → ident."prop" for SQL."""
    return re.sub(
        r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b",
        lambda m: f'{m.group(1)}."{m.group(2)}"',
        expr,
    )


# Pattern: [(src_var:Label?)-[:REL_TYPE?]->( tgt_var:Label?) | expr]
_PATH_COMP_RE = re.compile(
    r'\[\s*'
    r'\(\s*([A-Za-z_]\w*)\s*(?::[A-Za-z_]\w*)?\s*\)'  # (src_var[:Label]?)
    r'\s*-\[(?::([A-Za-z_]\w+))?\s*\]->\s*'            # -[:REL_TYPE?]->
    r'\(\s*([A-Za-z_]\w*)\s*(?::([A-Za-z_]\w+))?\s*\)' # (tgt_var[:Label]?)
    r'\s*\|\s*'
    r'([^\]]+?)'                                         # | expr
    r'\s*\]',
    re.IGNORECASE,
)


class PathComprehensionMixin:
    """Mixin for _Translator: translates pattern comprehensions."""

    _lm: CypherLabelMap
    _var_table: dict

    def _rewrite_path_comprehensions(self, text: str) -> str:
        """Rewrite Cypher path comprehensions to ARRAY(SELECT ...) subqueries."""
        def _replace(m: re.Match) -> str:
            src_var = m.group(1)
            rel_type = m.group(2).upper() if m.group(2) else None
            tgt_var = m.group(3)
            tgt_label = m.group(4)
            inner_expr = m.group(5).strip()

            # Resolve rel_mapping
            rel_mapping: RelationshipMapping | None = None
            src_info = self._var_table.get(src_var)
            src_nm: NodeMapping | None = src_info[1] if src_info else None

            if rel_type:
                rel_mapping = self._lm.relationships.get(rel_type)
            elif src_nm:
                candidates = self._lm.relationships_for(src_nm.type_name, tgt_label or "")
                if not candidates and tgt_label:
                    candidates = [
                        r for r in self._lm.relationships.values()
                        if r.source_label == (src_nm.type_name if src_nm else None)
                        and r.target_label == tgt_label
                    ]
                if candidates:
                    rel_mapping = candidates[0]

            if rel_mapping is None:
                return m.group(0)  # leave unchanged if unresolvable

            tgt_nm = self._lm.nodes.get(rel_mapping.target_label)
            if tgt_nm is None:
                return m.group(0)

            # Resolve inner expression: tgt_var.prop → tgt_var."prop"
            inner_sql = _prop(
                re.sub(rf'\b{re.escape(tgt_var)}\s*\.\s*', f'{tgt_var}.', inner_expr)
            )

            src_alias = self._var_table.get(src_var, (src_var, None))[0]
            subquery_sql = (
                f'SELECT {inner_sql} '
                f'FROM "{tgt_nm.catalog_name}"."{tgt_nm.schema_name}"."{tgt_nm.table_name}" AS {tgt_var} '
                f'WHERE {src_alias}."{rel_mapping.join_source_column}" = {tgt_var}."{rel_mapping.join_target_column}"'
            )
            return f'ARRAY({subquery_sql})'

        return _PATH_COMP_RE.sub(_replace, text)
