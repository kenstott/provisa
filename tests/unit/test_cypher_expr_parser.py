# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Standalone tests for the Cypher expression parser (REQ-913).

These pin the parser's structure (the AST it emits) independent of SQL lowering, so the grammar can
evolve with a safety net before the visitor and integration land.
"""

import pytest

from provisa.cypher.expr_ast import (
    Binary,
    Case,
    FunctionCall,
    Index,
    IsNull,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    Literal,
    MapLiteral,
    MapProjection,
    Parameter,
    PatternComprehension,
    Property,
    Quantifier,
    Reduce,
    Slice,
    SubqueryExpr,
    Unary,
    Variable,
)
from provisa.cypher.expr_parser import CypherExprParseError, parse_expression as P


class TestLeaves:
    def test_number_int(self):
        assert P("42") == Literal(42, "number")

    def test_number_float(self):
        assert P("3.5") == Literal(3.5, "number")

    def test_single_quoted_string(self):
        assert P("'hi'") == Literal("hi", "string")

    def test_double_quoted_string(self):
        assert P('"hi"') == Literal("hi", "string")

    def test_boolean_and_null(self):
        assert P("true") == Literal(True, "boolean")
        assert P("NULL") == Literal(None, "null")

    def test_variable(self):
        assert P("n") == Variable("n")

    def test_parameter(self):
        assert P("$min") == Parameter("min")


class TestAccess:
    def test_property(self):
        assert P("n.age") == Property(Variable("n"), "age")

    def test_nested_property(self):
        assert P("a.b.c") == Property(Property(Variable("a"), "b"), "c")

    def test_index(self):
        assert P("n.tags[0]") == Index(Property(Variable("n"), "tags"), Literal(0, "number"))

    def test_slice_both_bounds(self):
        node = P("n.tags[1..3]")
        assert isinstance(node, Slice)
        assert node.start == Literal(1, "number") and node.stop == Literal(3, "number")

    def test_slice_open_start(self):
        node = P("n.tags[..2]")
        assert isinstance(node, Slice) and node.start is None
        assert node.stop == Literal(2, "number")

    def test_label_predicate(self):
        assert P("n:Person") == LabelPredicate(Variable("n"), ["Person"])

    def test_label_predicate_is_form(self):
        assert P("n IS :Person") == LabelPredicate(Variable("n"), ["Person"])

    def test_label_predicate_qualified(self):
        # Provisa labels can be domain:object_type — captured as a colon-delimited list
        assert P("n IS :Sales:Order") == LabelPredicate(Variable("n"), ["Sales", "Order"])

    def test_label_predicate_is_not_form(self):
        node = P("n IS NOT :Person")
        assert node == Unary("NOT", LabelPredicate(Variable("n"), ["Person"]))


class TestOperators:
    def test_comparison(self):
        assert P("n.age > 30") == Binary(">", Property(Variable("n"), "age"), Literal(30, "number"))

    def test_precedence_mul_over_add(self):
        node = P("1 + 2 * 3")
        assert node == Binary(
            "+", Literal(1, "number"), Binary("*", Literal(2, "number"), Literal(3, "number"))
        )

    def test_and_or_precedence(self):
        # AND binds tighter than OR
        node = P("a OR b AND c")
        assert isinstance(node, Binary) and node.op == "OR"
        assert isinstance(node.right, Binary) and node.right.op == "AND"

    def test_not_unary(self):
        assert P("NOT n.active") == Unary("NOT", Property(Variable("n"), "active"))

    def test_unary_minus(self):
        assert P("-n.x") == Unary("-", Property(Variable("n"), "x"))

    def test_power(self):
        node = P("n.x ^ 2")
        assert node == Binary("^", Property(Variable("n"), "x"), Literal(2, "number"))

    def test_power_binds_tighter_than_mult(self):
        # 2 * 3 ^ 2 == 2 * (3 ^ 2)
        node = P("2 * 3 ^ 2")
        assert node.op == "*" and isinstance(node.right, Binary) and node.right.op == "^"

    def test_trim_is_a_function(self):
        assert P("trim(n.name)") == FunctionCall("trim", [Property(Variable("n"), "name")])

    def test_string_predicates(self):
        for op, text in [
            ("STARTS WITH", "n.a STARTS WITH 'x'"),
            ("ENDS WITH", "n.a ENDS WITH 'x'"),
            ("CONTAINS", "n.a CONTAINS 'x'"),
            ("=~", "n.a =~ 'x'"),
        ]:
            node = P(text)
            assert isinstance(node, Binary) and node.op == op

    def test_in_list(self):
        node = P("n.x IN [1, 2, 3]")
        assert isinstance(node, Binary) and node.op == "IN"
        assert isinstance(node.right, ListLiteral) and len(node.right.items) == 3

    def test_is_null(self):
        assert P("n.x IS NULL") == IsNull(Property(Variable("n"), "x"), negated=False)

    def test_is_not_null(self):
        assert P("n.x IS NOT NULL") == IsNull(Property(Variable("n"), "x"), negated=True)


class TestCallsAndCase:
    def test_function_call(self):
        assert P("toLower(n.name)") == FunctionCall("toLower", [Property(Variable("n"), "name")])

    def test_count_star(self):
        node = P("count(*)")
        assert isinstance(node, FunctionCall) and node.star and node.name.lower() == "count"

    def test_distinct_arg(self):
        node = P("count(DISTINCT n.id)")
        assert isinstance(node, FunctionCall) and node.distinct

    def test_searched_case(self):
        node = P("CASE WHEN n.x > 0 THEN 'p' ELSE 'n' END")
        assert isinstance(node, Case) and node.subject is None
        assert len(node.whens) == 1 and node.default == Literal("n", "string")

    def test_simple_case(self):
        node = P("CASE n.c WHEN 'r' THEN 1 WHEN 'g' THEN 2 END")
        assert isinstance(node, Case) and node.subject == Property(Variable("n"), "c")
        assert len(node.whens) == 2 and node.default is None


class TestStructures:
    def test_list_literal(self):
        assert P("[1, 2, 3]") == ListLiteral([Literal(i, "number") for i in (1, 2, 3)])

    def test_map_literal(self):
        node = P("{a: 1, b: 'two'}")
        assert isinstance(node, MapLiteral)
        assert node.entries == [("a", Literal(1, "number")), ("b", Literal("two", "string"))]

    def test_list_comprehension(self):
        node = P("[x IN n.list WHERE x > 0 | x * 2]")
        assert isinstance(node, ListComprehension)
        assert node.var == "x" and node.predicate is not None and node.projection is not None

    def test_pattern_comprehension(self):
        node = P("[(a)-[:KNOWS]->(b) WHERE b.age > 20 | b.name]")
        assert isinstance(node, PatternComprehension)
        assert "(a)-[:KNOWS]->(b)" in node.pattern
        assert node.projection == Property(Variable("b"), "name")
        assert node.path_var is None

    def test_pattern_comprehension_path_binding(self):
        node = P("[p = (a)-[:KNOWS]->(b) | b.name]")
        assert isinstance(node, PatternComprehension) and node.path_var == "p"
        assert "(a)-[:KNOWS]->(b)" in node.pattern

    def test_map_projection(self):
        node = P("n{.name, .age, label: n.type, .*}")
        assert isinstance(node, MapProjection) and node.var == "n"
        assert node.properties == ["name", "age"] and node.all_props
        assert node.literal_entries[0][0] == "label"

    def test_subquery_expr(self):
        node = P("EXISTS { MATCH (n)-[:R]->(m) }")
        assert isinstance(node, SubqueryExpr) and node.kind == "EXISTS"
        assert "MATCH (n)-[:R]->(m)" in node.body

    def test_quantifier(self):
        for kind, text in [
            ("ALL", "all(x IN n.l WHERE x > 0)"),
            ("ANY", "any(x IN n.l WHERE x.ok)"),
            ("NONE", "none(x IN n.l WHERE x = 1)"),
            ("SINGLE", "single(x IN n.l WHERE x > 5)"),
        ]:
            node = P(text)
            assert isinstance(node, Quantifier) and node.kind == kind
            assert node.var == "x" and node.predicate is not None

    def test_reduce(self):
        node = P("reduce(s = 0, x IN n.nums | s + x)")
        assert isinstance(node, Reduce)
        assert node.accumulator == "s" and node.var == "x"
        assert node.init == Literal(0, "number") and node.step is not None


class TestErrors:
    def test_unparseable_raises(self):
        with pytest.raises(CypherExprParseError):
            P("n.age >< ")

    def test_map_projection_on_nonvariable_raises(self):
        with pytest.raises(CypherExprParseError):
            P("(a.b){.x}")
