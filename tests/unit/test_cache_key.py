# Copyright (c) 2026 Kenneth Stott
# Canary: 882d87c0-053c-4924-a69e-4e1fd6224f0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The response cache's key (REQ-544, REQ-864, REQ-866, REQ-1897).

There is one key, ``raw_sql_cache_key``: the governed statement (normalized), its bound values
and the governed role. The governed statement carries the resolved row filters inline, so a
different filter is a different statement and a different key."""

from provisa.cache.key import is_cacheable, raw_sql_cache_key


def _key(sql: str, params: list, role: str, **kw) -> str:
    return raw_sql_cache_key(sql, params, role, wire_formats=kw.pop("wire_formats", None), **kw)


class TestCacheKey:
    def test_same_inputs_same_key(self):
        k1 = _key("SELECT 1 WHERE region = 'us'", [1, "us"], "analyst")
        k2 = _key("SELECT 1 WHERE region = 'us'", [1, "us"], "analyst")
        assert k1 == k2

    def test_different_role_different_key(self):
        assert _key("SELECT 1", [], "admin") != _key("SELECT 1", [], "analyst")

    def test_a_different_resolved_row_filter_is_a_different_key(self):
        # REQ-866: the row filter is resolved into the governed statement.
        k1 = _key("SELECT a FROM t WHERE region = 'us'", [], "analyst")
        k2 = _key("SELECT a FROM t WHERE region = 'eu'", [], "analyst")
        assert k1 != k2

    def test_different_params_different_key(self):
        assert _key("SELECT 1", [1], "admin") != _key("SELECT 1", [2], "admin")

    def test_different_sql_different_key(self):
        assert _key("SELECT 1", [], "admin") != _key("SELECT 2", [], "admin")

    def test_key_is_sha256_hex(self):
        k = _key("SELECT 1", [], "admin")
        assert len(k) == 64
        int(k, 16)  # should not raise

    def test_wire_formats_and_as_of_partition_the_key(self):
        base = _key("SELECT 1", [], "admin")
        assert _key("SELECT 1", [], "admin", wire_formats=[1]) != base
        assert _key("SELECT 1", [], "admin", as_of="TIMESTAMP '2026-01-01'") != base


class TestCacheKeyNormalization:  # REQ-864
    def test_whitespace_and_case_share_a_key(self):
        k1 = _key("SELECT a, b FROM t WHERE x = 1", [], "r")
        k2 = _key("select   A,\n  B\nfrom T\nwhere X = 1", [], "r")
        assert k1 == k2

    def test_commutable_predicate_order_shares_a_key(self):
        k1 = _key('SELECT "a" FROM "t" WHERE "x" = 1 AND "y" = 2', [], "r")
        k2 = _key('SELECT "a" FROM "t" WHERE "y" = 2 AND "x" = 1', [], "r")
        assert k1 == k2

    def test_distinct_literal_values_do_not_collapse(self):
        # Isolation-preserving: different predicate VALUES must stay distinct (REQ-866).
        k1 = _key("SELECT a FROM t WHERE tenant_id = 'acme'", [], "r")
        k2 = _key("SELECT a FROM t WHERE tenant_id = 'beta'", [], "r")
        assert k1 != k2

    def test_unparseable_sql_still_deterministic(self):
        # Falls back to raw text; distinct raw text → distinct key (miss, never wrong hit).
        weird = ">>> not sql <<<"
        assert _key(weird, [], "r") == _key(weird, [], "r")
        assert _key(weird, [], "r") != _key(weird + "!", [], "r")


class TestIsCacheable:  # REQ-866 fail-closed
    def test_plain_query_is_cacheable(self):
        ok, _ = is_cacheable("SELECT 1")
        assert ok is True

    def test_a_resolved_row_filter_is_cacheable(self):
        ok, _ = is_cacheable("SELECT a FROM t WHERE region = 'us'")
        assert ok is True

    def test_a_row_filter_on_unresolved_session_state_is_not_cacheable(self):
        ok, reason = is_cacheable(
            "SELECT a FROM t WHERE tenant_id = current_setting('provisa.tenant')"
        )
        assert ok is False and "session state" in reason

    def test_current_setting_in_sql_not_cacheable(self):
        ok, _ = is_cacheable("SELECT a FROM t WHERE u = CURRENT_SETTING('provisa.user_id')")
        assert ok is False
