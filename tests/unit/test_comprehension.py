# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for provisa.cypher.comprehension (REQ-345, REQ-347).

Pure string-in/string-out rewrites — no AST construction needed. Table-driven tests exercise every
branch of the hand-rolled recursive-descent parser: list comprehensions (map-only, filter-only,
filter+map, bare/invalid), any/all/none/single predicate comprehensions, reduce(), string/quote
handling, nested brackets, and non-comprehension passthrough text.
"""

import pytest

from provisa.cypher.comprehension import (
    _ComprehensionParser,
    rewrite_list_comprehensions,
    rewrite_reduce,
)


class TestRewriteReduce:
    @pytest.mark.parametrize(
        "text,expected",
        [
            (
                "reduce(acc = '', x IN names | acc || x)",
                "reduce(names, '', (acc, x) -> acc || x, acc -> acc)",
            ),
            (
                "reduce(total = 0, n IN nums | total + n)",
                "reduce(nums, 0, (total, n) -> total + n, total -> total)",
            ),
            (
                "RETURN reduce(acc = 0, x IN [1,2,3] | acc + x) AS s",
                "RETURN reduce([1,2,3], 0, (acc, x) -> acc + x, acc -> acc) AS s",
            ),
        ],
    )
    def test_rewrites_reduce_call(self, text, expected):
        assert rewrite_reduce(text) == expected

    def test_no_reduce_call_passthrough(self):
        text = "RETURN x + y AS z"
        assert rewrite_reduce(text) == text

    def test_case_insensitive_reduce_keyword_and_in(self):
        result = rewrite_reduce("REDUCE(acc = 0, x in nums | acc + x)")
        assert result == "reduce(nums, 0, (acc, x) -> acc + x, acc -> acc)"

    def test_multiple_reduce_calls(self):
        text = "reduce(a = 0, x IN xs | a + x) + reduce(b = 1, y IN ys | b * y)"
        result = rewrite_reduce(text)
        assert result == (
            "reduce(xs, 0, (a, x) -> a + x, a -> a) + reduce(ys, 1, (b, y) -> b * y, b -> b)"
        )


class TestListComprehensionMapOnly:
    def test_simple_map(self):
        result = rewrite_list_comprehensions("[x IN list | x + 1]")
        assert result == "transform(list, x -> x + 1)"

    def test_map_with_function_call_body(self):
        result = rewrite_list_comprehensions("[n IN nodes | toUpper(n.name)]")
        assert result == "transform(nodes, n -> toUpper(n.name))"

    def test_map_list_expr_is_property_access(self):
        result = rewrite_list_comprehensions("[x IN n.items | x * 2]")
        assert result == "transform(n.items, x -> x * 2)"


class TestListComprehensionFilterOnly:
    def test_simple_filter(self):
        result = rewrite_list_comprehensions("[x IN list WHERE x > 1]")
        assert result == "filter(list, x -> x > 1)"

    def test_filter_with_comparison_and_and(self):
        result = rewrite_list_comprehensions("[x IN nums WHERE x > 1 AND x < 10]")
        assert result == "filter(nums, x -> x > 1 AND x < 10)"


class TestListComprehensionFilterAndMap:
    def test_filter_then_map(self):
        result = rewrite_list_comprehensions("[x IN list WHERE x > 1 | x * 2]")
        assert result == "transform(filter(list, x -> x > 1), x -> x * 2)"

    def test_filter_then_map_nested_call(self):
        result = rewrite_list_comprehensions("[p IN people WHERE p.age > 18 | p.name]")
        assert result == ("transform(filter(people, p -> p.age > 18), p -> p.name)")


class TestListComprehensionInvalidOrBare:
    def test_bare_comprehension_no_pred_no_map_passthrough(self):
        # [x IN list] without WHERE or | is not a comprehension -> left untouched.
        result = rewrite_list_comprehensions("[x IN list]")
        assert result == "[x IN list]"

    def test_not_a_comprehension_plain_list_literal(self):
        result = rewrite_list_comprehensions("[1, 2, 3]")
        assert result == "[1, 2, 3]"

    def test_list_starting_with_non_ident_char(self):
        result = rewrite_list_comprehensions("[1 + 2]")
        assert result == "[1 + 2]"

    def test_missing_in_keyword_passthrough(self):
        result = rewrite_list_comprehensions("[x FOO list | x]")
        assert result == "[x FOO list | x]"

    def test_empty_list_expr_after_in_passthrough(self):
        # IN immediately followed by WHERE with nothing between -> list_expr empty -> bail.
        result = rewrite_list_comprehensions("[x IN WHERE x]")
        assert result == "[x IN WHERE x]"

    def test_unclosed_bracket_passthrough(self):
        result = rewrite_list_comprehensions("[x IN list | x + 1")
        assert result == "[x IN list | x + 1"


class TestPredicateComprehensions:
    @pytest.mark.parametrize(
        "func,expected_prefix",
        [
            ("any", "any_match"),
            ("all", "all_match"),
            ("none", "none_match"),
        ],
    )
    def test_any_all_none(self, func, expected_prefix):
        result = rewrite_list_comprehensions(f"{func}(x IN list WHERE x > 1)")
        assert result == f"{expected_prefix}(list, x -> x > 1)"

    def test_single(self):
        result = rewrite_list_comprehensions("single(x IN list WHERE x > 1)")
        assert result == "cardinality(filter(list, x -> x > 1)) = 1"

    def test_case_insensitive_keyword(self):
        result = rewrite_list_comprehensions("ANY(x IN list WHERE x = 1)")
        assert result == "any_match(list, x -> x = 1)"

    def test_predicate_word_without_paren_passthrough(self):
        # 'any' not followed by '(' is just treated as a plain identifier.
        result = rewrite_list_comprehensions("RETURN any AS x")
        assert result == "RETURN any AS x"

    def test_predicate_word_followed_by_ws_then_paren(self):
        result = rewrite_list_comprehensions("any (x IN list WHERE x > 1)")
        assert result == "any_match(list, x -> x > 1)"

    def test_predicate_missing_where_passthrough(self):
        # any(x IN list) without WHERE cannot be rewritten -> left as-is.
        result = rewrite_list_comprehensions("any(x IN list)")
        assert result == "any(x IN list)"

    def test_predicate_missing_in_passthrough(self):
        result = rewrite_list_comprehensions("all(x list WHERE x > 1)")
        assert result == "all(x list WHERE x > 1)"

    def test_predicate_unclosed_paren_passthrough(self):
        result = rewrite_list_comprehensions("none(x IN list WHERE x > 1")
        assert result == "none(x IN list WHERE x > 1"

    def test_predicate_empty_list_expr_passthrough(self):
        result = rewrite_list_comprehensions("all(x IN WHERE x > 1)")
        assert result == "all(x IN WHERE x > 1)"

    def test_non_predicate_call_passthrough(self):
        result = rewrite_list_comprehensions("count(x)")
        assert result == "count(x)"

    def test_paren_after_keyword_not_ident_start(self):
        # 'any' followed by '(' but the content inside doesn't start with an identifier char.
        result = rewrite_list_comprehensions("any(1 + 2)")
        assert result == "any(1 + 2)"


class TestStringHandling:
    def test_string_literal_untouched(self):
        result = rewrite_list_comprehensions("RETURN 'hello [world]' AS s")
        assert result == "RETURN 'hello [world]' AS s"

    def test_double_quoted_string_untouched(self):
        result = rewrite_list_comprehensions('RETURN "any(x)" AS s')
        assert result == 'RETURN "any(x)" AS s'

    def test_string_with_escaped_quote(self):
        result = rewrite_list_comprehensions(r"RETURN 'it\'s [x]' AS s")
        assert result == r"RETURN 'it\'s [x]' AS s"

    def test_comprehension_body_contains_string_literal(self):
        result = rewrite_list_comprehensions("[x IN list WHERE x = 'a|b']")
        assert result == "filter(list, x -> x = 'a|b')"

    def test_unterminated_string_literal(self):
        # Reader reaches end of text without closing quote; should not crash.
        result = rewrite_list_comprehensions("RETURN 'unterminated")
        assert result == "RETURN 'unterminated"


class TestNestedAndCombined:
    def test_nested_list_comprehension_in_map_expr(self):
        # The map-expr body is captured as raw text via _read_expr_until (bracket-depth tracked but
        # not recursively rewritten), so an inner comprehension inside the map body is left as-is.
        result = rewrite_list_comprehensions("[x IN outer | [y IN x.inner | y + 1]]")
        assert result == "transform(outer, x -> [y IN x.inner | y + 1])"

    def test_nested_function_call_parens_in_where(self):
        result = rewrite_list_comprehensions("[x IN list WHERE size(x.items) > 0]")
        assert result == "filter(list, x -> size(x.items) > 0)"

    def test_combined_reduce_and_list_comprehension(self):
        text = "reduce(acc = 0, x IN [y IN list WHERE y > 1] | acc + x)"
        result = rewrite_list_comprehensions(text)
        assert result == ("reduce(filter(list, y -> y > 1), 0, (acc, x) -> acc + x, acc -> acc)")

    def test_multiple_comprehensions_in_one_text(self):
        text = "RETURN [x IN a | x], [y IN b WHERE y > 0]"
        result = rewrite_list_comprehensions(text)
        assert result == "RETURN transform(a, x -> x), filter(b, y -> y > 0)"

    def test_plain_text_with_underscores_and_digits(self):
        text = "MATCH (n1:Label_1) RETURN n1"
        assert rewrite_list_comprehensions(text) == text

    def test_symbols_passthrough(self):
        text = "a + b - c * d / e"
        assert rewrite_list_comprehensions(text) == text


class TestParserInternals:
    def test_consume_keyword_prefix_match_but_longer_ident_fails(self):
        # 'INside' should not match keyword 'IN' since next char is alnum.
        parser = _ComprehensionParser("INside")
        assert parser._consume_keyword("IN") is False

    def test_consume_keyword_exact_match(self):
        parser = _ComprehensionParser("IN rest")
        assert parser._consume_keyword("IN") is True
        assert parser._text[parser._pos :] == " rest"

    def test_peek_keyword_does_not_advance(self):
        parser = _ComprehensionParser("WHERE x > 1")
        assert parser._peek_keyword("WHERE") is True
        assert parser._pos == 0

    def test_read_expr_until_stops_at_keyword(self):
        parser = _ComprehensionParser("x > 1 WHERE y")
        expr = parser._read_expr_until({"WHERE"})
        assert expr == "x > 1"

    def test_read_expr_until_respects_nested_depth(self):
        parser = _ComprehensionParser("foo(a, b) | rest")
        expr = parser._read_expr_until({"|"})
        assert expr == "foo(a, b)"

    def test_read_ident_stops_at_non_word_char(self):
        parser = _ComprehensionParser("abc123-def")
        assert parser._read_ident() == "abc123"
