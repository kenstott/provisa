# Copyright (c) 2026 Kenneth Stott
# Canary: 6e623c9d-f1f6-4f95-834d-2801066480ad
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for directive extraction — @noCache and @cached."""

from provisa.compiler.directives import (
    QueryDirectives,
    extract_directives_from_sql_comments,
    merge_directives,
)


class TestNoCacheDirective:
    def test_sql_comment_no_cache_true(self):
        sql = "-- @provisa no_cache=true\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.no_cache is True

    def test_sql_comment_no_cache_1(self):
        sql = "-- @provisa no_cache=1\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.no_cache is True

    def test_sql_comment_no_cache_yes(self):
        sql = "-- @provisa no_cache=yes\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.no_cache is True

    def test_sql_comment_no_cache_absent(self):
        sql = "-- @provisa route=direct\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.no_cache is False

    def test_merge_no_cache_or_semantics(self):
        a = QueryDirectives(no_cache=False)
        b = QueryDirectives(no_cache=True)
        merged = merge_directives(a, b)
        assert merged.no_cache is True

    def test_merge_no_cache_stays_false_when_both_false(self):
        a = QueryDirectives(no_cache=False)
        b = QueryDirectives(no_cache=False)
        merged = merge_directives(a, b)
        assert merged.no_cache is False

    def test_no_cache_default_false(self):
        d = QueryDirectives()
        assert d.no_cache is False


class TestCachedDirective:
    def test_sql_comment_cache_ttl(self):
        sql = "-- @provisa cache_ttl=300\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.cache_ttl == 300

    def test_sql_comment_cache_ttl_zero_disables(self):
        sql = "-- @provisa cache_ttl=0\nSELECT 1"
        d = extract_directives_from_sql_comments(sql)
        assert d.cache_ttl == 0

    def test_merge_cache_ttl_later_wins(self):
        a = QueryDirectives(cache_ttl=60)
        b = QueryDirectives(cache_ttl=120)
        merged = merge_directives(a, b)
        assert merged.cache_ttl == 120
