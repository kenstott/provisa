# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rewrite Cypher list comprehensions to SQLGlot-parseable lambda syntax.

Supported rewrites:
  [x IN list | f(x)]                       -> transform(list, x -> f(x))
  [x IN list WHERE p(x)]                   -> filter(list, x -> p(x))
  [x IN list WHERE p(x) | f(x)]            -> transform(filter(list, x -> p(x)), x -> f(x))
  any(x IN list WHERE p(x))                -> any_match(list, x -> p(x))
  all(x IN list WHERE p(x))                -> all_match(list, x -> p(x))
  none(x IN list WHERE p(x))               -> none_match(list, x -> p(x))
  single(x IN list WHERE p(x))             -> cardinality(filter(list, x -> p(x))) = 1
"""

from __future__ import annotations


class _ComprehensionParser:
    def __init__(self, text: str) -> None:
        self._text = text
        self._pos = 0

    def rewrite(self) -> str:
        parts = []
        while self._pos < len(self._text):
            c = self._text[self._pos]
            if c == '[':
                comp = self._try_list_comp()
                if comp is not None:
                    parts.append(comp)
                else:
                    parts.append('[')
                    self._pos += 1
            elif c in ('"', "'"):
                parts.append(self._read_string())
            elif c.isalpha() or c == '_':
                word = self._read_ident()
                if word.lower() in ('any', 'all', 'none', 'single'):
                    saved_pos = self._pos
                    self._skip_ws()
                    if self._pos < len(self._text) and self._text[self._pos] == '(':
                        comp = self._try_pred_comp(word)
                        if comp is not None:
                            parts.append(comp)
                        else:
                            parts.append(word)
                    else:
                        self._pos = saved_pos
                        parts.append(word)
                else:
                    parts.append(word)
            else:
                parts.append(c)
                self._pos += 1
        return ''.join(parts)

    def _read_ident(self) -> str:
        start = self._pos
        while self._pos < len(self._text) and (self._text[self._pos].isalnum() or self._text[self._pos] == '_'):
            self._pos += 1
        return self._text[start:self._pos]

    def _read_string(self) -> str:
        quote = self._text[self._pos]
        parts = [quote]
        self._pos += 1
        while self._pos < len(self._text):
            c = self._text[self._pos]
            parts.append(c)
            self._pos += 1
            if c == '\\' and self._pos < len(self._text):
                parts.append(self._text[self._pos])
                self._pos += 1
            elif c == quote:
                break
        return ''.join(parts)

    def _skip_ws(self) -> None:
        while self._pos < len(self._text) and self._text[self._pos] in ' \t\n':
            self._pos += 1

    def _consume_keyword(self, kw: str) -> bool:
        self._skip_ws()
        end = self._pos + len(kw)
        if self._text[self._pos:end].upper() == kw.upper():
            after = end
            if after >= len(self._text) or not (self._text[after].isalnum() or self._text[after] == '_'):
                self._pos = end
                return True
        return False

    def _peek_keyword(self, kw: str) -> bool:
        saved = self._pos
        result = self._consume_keyword(kw)
        self._pos = saved
        return result

    def _read_expr_until(self, stops: set) -> str:
        """Read expression text, stopping at any single-char stop or stop keyword at depth 0.

        stops may contain single chars like '|', ']', ')' and keywords like 'WHERE'.
        """
        parts = []
        depth = 0
        while self._pos < len(self._text):
            c = self._text[self._pos]
            if c in ('"', "'"):
                parts.append(self._read_string())
                continue
            if c in '([{':
                depth += 1
                parts.append(c)
                self._pos += 1
                continue
            if c in ')]}':
                if depth == 0:
                    break
                depth -= 1
                parts.append(c)
                self._pos += 1
                continue
            if depth == 0:
                if c in stops:
                    break
                if c.isalpha() or c == '_':
                    saved = self._pos
                    word = self._read_ident()
                    kw_stops = {s for s in stops if isinstance(s, str) and len(s) > 1}
                    if word.upper() in {k.upper() for k in kw_stops}:
                        self._pos = saved
                        break
                    parts.append(word)
                    continue
            parts.append(c)
            self._pos += 1
        return ''.join(parts).strip()

    def _try_list_comp(self) -> str | None:
        """Try to parse [var IN list WHERE? pred? | map?] at current [."""
        saved = self._pos
        self._pos += 1  # consume [
        self._skip_ws()
        if not (self._pos < len(self._text) and (self._text[self._pos].isalpha() or self._text[self._pos] == '_')):
            self._pos = saved
            return None
        var = self._read_ident()
        if not self._consume_keyword('IN'):
            self._pos = saved
            return None
        self._skip_ws()
        list_expr = self._read_expr_until({'WHERE', '|', ']'})
        if not list_expr:
            self._pos = saved
            return None
        where_pred = None
        map_expr = None
        if self._peek_keyword('WHERE'):
            self._consume_keyword('WHERE')
            self._skip_ws()
            where_pred = self._read_expr_until({'|', ']'})
        self._skip_ws()
        if self._pos < len(self._text) and self._text[self._pos] == '|':
            self._pos += 1
            self._skip_ws()
            map_expr = self._read_expr_until({']'})
        self._skip_ws()
        if self._pos >= len(self._text) or self._text[self._pos] != ']':
            self._pos = saved
            return None
        self._pos += 1  # consume ]
        if map_expr and where_pred:
            return f"transform(filter({list_expr}, {var} -> {where_pred}), {var} -> {map_expr})"
        elif map_expr:
            return f"transform({list_expr}, {var} -> {map_expr})"
        elif where_pred:
            return f"filter({list_expr}, {var} -> {where_pred})"
        else:
            self._pos = saved
            return None  # bare [x IN list] without pred or map — not a comprehension

    def _try_pred_comp(self, func_name: str) -> str | None:
        """Try to parse any/all/none/single(var IN list WHERE pred)."""
        saved = self._pos  # pos is at '('
        self._pos += 1  # consume (
        self._skip_ws()
        if not (self._pos < len(self._text) and (self._text[self._pos].isalpha() or self._text[self._pos] == '_')):
            self._pos = saved
            return None
        var = self._read_ident()
        if not self._consume_keyword('IN'):
            self._pos = saved
            return None
        self._skip_ws()
        list_expr = self._read_expr_until({'WHERE', ')'})
        if not list_expr:
            self._pos = saved
            return None
        if not self._peek_keyword('WHERE'):
            self._pos = saved
            return None
        self._consume_keyword('WHERE')
        self._skip_ws()
        where_pred = self._read_expr_until({')'})
        self._skip_ws()
        if self._pos >= len(self._text) or self._text[self._pos] != ')':
            self._pos = saved
            return None
        self._pos += 1  # consume )
        fn = func_name.lower()
        if fn == 'any':
            return f"any_match({list_expr}, {var} -> {where_pred})"
        elif fn == 'all':
            return f"all_match({list_expr}, {var} -> {where_pred})"
        elif fn == 'none':
            return f"none_match({list_expr}, {var} -> {where_pred})"
        elif fn == 'single':
            return f"cardinality(filter({list_expr}, {var} -> {where_pred})) = 1"
        self._pos = saved
        return None


def rewrite_list_comprehensions(text: str) -> str:
    """Rewrite Cypher list comprehensions to SQLGlot-parseable lambda syntax."""
    return _ComprehensionParser(text).rewrite()
