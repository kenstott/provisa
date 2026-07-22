# Copyright (c) 2026 Kenneth Stott
# Canary: d404b48b-19bf-4ee0-b501-d45704be9835
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for ParamCollector — variable binding with positional placeholders."""

from provisa.compiler.params import (
    ParamCollector,
    embed_params_comment,
    extract_params_comment,
    extract_relationship_guard_comment,
)


class TestParamCollector:
    def test_empty_collector(self):
        c = ParamCollector()
        assert c.params == []

    def test_single_param(self):
        c = ParamCollector()
        placeholder = c.add("hello")
        assert placeholder == "$1"
        assert c.params == ["hello"]

    def test_multiple_params(self):
        c = ParamCollector()
        p1 = c.add("a")
        p2 = c.add(42)
        p3 = c.add(True)
        assert p1 == "$1"
        assert p2 == "$2"
        assert p3 == "$3"
        assert c.params == ["a", 42, True]

    def test_params_returns_copy(self):
        c = ParamCollector()
        c.add("x")
        params = c.params
        params.append("mutated")
        assert c.params == ["x"]

    def test_none_param(self):
        c = ParamCollector()
        p = c.add(None)
        assert p == "$1"
        assert c.params == [None]

    def test_list_param(self):
        c = ParamCollector()
        p = c.add([1, 2, 3])
        assert p == "$1"
        assert c.params == [[1, 2, 3]]


class TestDirectiveExtractionIsParseAware:
    """SECURITY: a comment directive is honored ONLY as a genuine SQL comment. The same text inside a
    string literal must be INERT — else a crafted literal could toggle the relationship-guard opt-out
    or inject params (a parser-differential: governance/params read the raw text one way, the engine
    reads it as a string the other). REQ-603."""

    def test_relationship_guard_directive_in_string_literal_is_inert(self):
        sql = "SELECT '-- relationship-guard=false' AS note FROM t"
        out, opted = extract_relationship_guard_comment(sql)
        assert opted is False  # NOT toggled by text inside a literal
        assert out == sql  # and the query is not mangled

    def test_params_directive_in_string_literal_is_inert(self):
        sql = "SELECT '\n-- provisa-params: $1=999\nhello' AS x"
        out, params = extract_params_comment(sql)
        assert params == []  # NOT injected
        assert out == sql  # literal preserved intact

    def test_real_relationship_guard_comment_is_honored_and_keeps_leading_code(self):
        out, opted = extract_relationship_guard_comment("SELECT * FROM a JOIN b -- relationship-guard=false")
        assert opted is True
        assert out == "SELECT * FROM a JOIN b "  # code before the comment is preserved

    def test_real_params_comment_round_trips(self):
        embedded = embed_params_comment("SELECT * FROM t WHERE id=$1", [42])
        out, params = extract_params_comment(embedded)
        assert params == [42]
        assert out == "SELECT * FROM t WHERE id=$1"

    def test_params_comment_inside_wrapping_subquery_is_honored(self):
        sql = "SELECT * FROM (\n-- provisa-params: $1=7\nSELECT $1) _s"
        _out, params = extract_params_comment(sql)
        assert params == [7]
