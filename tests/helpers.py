# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cd12-ef3456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Test helpers shared across the test suite."""

from __future__ import annotations

import re


_ALIAS_RE = re.compile(r'\b(t|a|j|n|sub|cte)\d+\b', re.IGNORECASE)
_QUOTED_ALIAS_RE = re.compile(r'"(t|a|j|n|sub|cte)\d+"', re.IGNORECASE)


def _normalize_sql(sql: str) -> str:
    """Strip generated numeric aliases so assertions are alias-position-insensitive."""
    sql = _QUOTED_ALIAS_RE.sub('__alias__', sql)
    sql = _ALIAS_RE.sub('__alias__', sql)
    return re.sub(r'\s+', ' ', sql).strip()


def assert_sql_contains(sql: str, fragment: str) -> None:
    """Assert that *fragment* appears in *sql* after normalizing generated aliases.

    Both sides have numeric table aliases (t0, t1, a2, …) replaced with a
    placeholder so tests don't break when the compiler changes join order.
    """
    norm_sql = _normalize_sql(sql)
    norm_frag = _normalize_sql(fragment)
    assert norm_frag in norm_sql, (
        f"SQL fragment not found.\n"
        f"Fragment: {norm_frag}\n"
        f"SQL:      {norm_sql}"
    )


def assert_span_emitted(exporter, name_fragment: str) -> None:
    spans = exporter.get_finished_spans()
    names = [s.name for s in spans]
    assert any(name_fragment in n for n in names), (
        f"No span matching {name_fragment!r} found. Emitted: {names}"
    )


def assert_sql_matches(sql: str, pattern: str) -> None:
    """Assert that regex *pattern* matches *sql* after normalizing generated aliases."""
    norm_sql = _normalize_sql(sql)
    assert re.search(pattern, norm_sql, re.IGNORECASE), (
        f"Pattern not matched.\nPattern: {pattern}\nSQL: {norm_sql}"
    )
