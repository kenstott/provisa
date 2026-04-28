# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3b1f9d-4c2a-4e8b-9f5d-1a6c8e2b4d7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SubqueryExprsMixin — G2/G3/G4 subquery expression rewriting.

EXISTS { MATCH ... }        → EXISTS (SELECT 1 FROM ...)
COUNT { MATCH ... }         → (SELECT count(*) FROM (...) AS _cnt)
COLLECT { MATCH ... RETURN expr } → ARRAY(SELECT ...)
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


def _extract_brace_body(text: str, brace_pos: int) -> str:
    """Return the text between { } starting at brace_pos (exclusive of braces)."""
    depth = 0
    for i, ch in enumerate(text[brace_pos:]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[brace_pos + 1 : brace_pos + i]
    raise ValueError("Unmatched brace in subquery expression")


_SUBQUERY_RE = re.compile(r"\b(EXISTS|COUNT|COLLECT)\s*\{", re.IGNORECASE)


class SubqueryExprsMixin:
    """Mixin that adds EXISTS/COUNT/COLLECT { } subquery expression rewriting."""

    def _rewrite_subquery_exprs(self, text: str) -> str:
        """Rewrite EXISTS/COUNT/COLLECT { } patterns to SQL correlated subquery text."""
        result: list[str] = []
        i = 0
        while i < len(text):
            m = _SUBQUERY_RE.search(text, i)
            if m is None:
                result.append(text[i:])
                break
            result.append(text[i : m.start()])
            keyword = m.group(1).upper()
            brace_start = m.end() - 1  # position of '{'
            body = _extract_brace_body(text, brace_start)
            end_pos = brace_start + len(body) + 2  # +2 for { and }
            sql_text = self._subquery_expr_to_sql(keyword, body.strip())
            result.append(sql_text)
            i = end_pos
        return "".join(result)

    def _subquery_expr_to_sql(self, keyword: str, body: str) -> str:
        from provisa.cypher.translator import _Translator, CypherTranslateError
        from provisa.cypher.parser import parse_cypher

        if keyword in ("EXISTS", "COUNT") and "RETURN" not in body.upper():
            body = body + " RETURN 1"
        elif keyword == "COLLECT" and "RETURN" not in body.upper():
            body = body + " RETURN *"

        inner_ast = parse_cypher(body)
        inner_tr = _Translator(inner_ast, self._lm, self._params)  # type: ignore[attr-defined]
        inner_tr._var_table.update(self._var_table)  # type: ignore[attr-defined]
        inner_select, inner_params, _ = inner_tr.translate()

        # Merge inner params into outer
        for p in inner_params:
            if p not in self._param_seen:  # type: ignore[attr-defined]
                self._param_order.append(p)  # type: ignore[attr-defined]
                self._param_seen.add(p)  # type: ignore[attr-defined]

        inner_sql = inner_select.sql(dialect="trino")

        if keyword == "EXISTS":
            return f"EXISTS ({inner_sql})"
        elif keyword == "COUNT":
            return f"(SELECT count(*) FROM ({inner_sql}) AS _cnt_sub)"
        else:  # COLLECT
            return f"ARRAY({inner_sql})"
