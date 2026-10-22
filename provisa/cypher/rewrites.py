# Copyright (c) 2026 Kenneth Stott
# Canary: 1c6fe90a-af59-4465-9b0e-429746c831a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rewrites of Cypher text: a double-quoted Cypher string as a SQL string, an ISO timestamp string
as a TIMESTAMP literal (REQ-409, REQ-410)."""

# Requirements: REQ-409, REQ-410

from __future__ import annotations

import re


_CYPHER_DQUOTE_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _rewrite_cypher_dquote_strings(expr: str) -> str:  # REQ-410
    """Cypher ``"str"`` → SQL ``'str'``, leaving quoted identifiers (after ``.``) untouched. Retained
    for the UNWIND text path + REQ-410; the AST grammar handles this on the predicate paths (REQ-913)."""
    result: list[str] = []
    pos = 0
    for m in _CYPHER_DQUOTE_RE.finditer(expr):
        start = m.start()
        result.append(expr[pos:start])
        keep = start > 0 and expr[start - 1] == "."  # property name after `.` — leave as-is
        result.append(m.group(0) if keep else "'" + m.group(1).replace("'", "\\'") + "'")
        pos = m.end()
    result.append(expr[pos:])
    return "".join(result)


_ISO_TS_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)'")


def _coerce_ts_literals(text: str) -> str:  # REQ-409
    """Wrap ISO-datetime string literals as ``TIMESTAMP '...'`` (retained for REQ-409)."""
    return _ISO_TS_LITERAL_RE.sub(lambda m: f"TIMESTAMP {m.group(0)}", text)
