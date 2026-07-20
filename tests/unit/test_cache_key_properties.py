# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for the response cache key (REQ-544, REQ-864, REQ-866).

cache_key derives the identity under which a governed result is cached. If two
DIFFERENT security contexts hash to the same key, one role is served another's
rows — a governance bypass through the cache. If two EQUIVALENT queries hash to
different keys, the cache never hits. So the key must reflect exactly the security
context and nothing cosmetic:

  * deterministic — identical inputs always yield the same key;
  * role-, RLS-, param-, and query-sensitive — changing any of them changes the key
    (no cached result crosses a security boundary);
  * SQL-normalizing — queries differing only in whitespace share a key (REQ-864).
"""

from __future__ import annotations

import re

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from provisa.cache.key import cache_key

_SQLS = [
    "SELECT id FROM t",
    "SELECT a, b FROM u WHERE a > 0",
    "SELECT id, amount FROM orders WHERE region = 'x'",
    "SELECT count(*) FROM events",
]
_ROLES = ["admin", "analyst", "viewer", "guest"]
_RLS_FILTERS = ["region = 'x'", "dept_id = 1", "tenant = current_user", ""]

_params = st.lists(st.one_of(st.integers(-50, 50), st.text(max_size=4)), max_size=3)
_rls = st.dictionaries(st.integers(1, 5), st.sampled_from(_RLS_FILTERS), max_size=3)


@st.composite
def _inputs(draw):
    return (
        draw(st.sampled_from(_SQLS)),
        draw(_params),
        draw(st.sampled_from(_ROLES)),
        draw(_rls),
    )


@settings(max_examples=300, deadline=None)
@given(x=_inputs())
def test_key_is_deterministic(x) -> None:
    assert cache_key(*x) == cache_key(*x)


@settings(max_examples=300, deadline=None)
@given(x=_inputs(), role2=st.sampled_from(_ROLES))
def test_role_change_changes_key(x, role2) -> None:
    """A different role must never reuse another role's cached result."""
    sql, params, role, rls = x
    assume(role2 != role)
    assert cache_key(sql, params, role, rls) != cache_key(sql, params, role2, rls)


@settings(max_examples=300, deadline=None)
@given(x=_inputs(), rls2=_rls)
def test_rls_change_changes_key(x, rls2) -> None:
    """Different resolved RLS filters (different visible rows) must key differently."""
    sql, params, role, rls = x
    assume(rls2 != rls)
    assert cache_key(sql, params, role, rls) != cache_key(sql, params, role, rls2)


@settings(max_examples=300, deadline=None)
@given(x=_inputs(), sql2=st.sampled_from(_SQLS), params2=_params)
def test_query_or_params_change_changes_key(x, sql2, params2) -> None:
    """A different query or different bound params must key differently."""
    sql, params, role, rls = x
    base = cache_key(sql, params, role, rls)
    if sql2 != sql:
        assert cache_key(sql2, params, role, rls) != base
    if params2 != params:
        assert cache_key(sql, params2, role, rls) != base


@settings(max_examples=300, deadline=None)
@given(x=_inputs())
def test_whitespace_is_normalized_away(x) -> None:
    """Queries that differ only in whitespace share a key — the cache hits despite
    formatting (the SQL pool carries no space-bearing string literals)."""
    sql, params, role, rls = x
    respaced = re.sub(r"\s+", "   ", sql)
    assert cache_key(respaced, params, role, rls) == cache_key(sql, params, role, rls)
