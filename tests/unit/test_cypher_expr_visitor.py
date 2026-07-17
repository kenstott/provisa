# Copyright (c) 2026 Kenneth Stott
# Canary: 493e38a6-b2f0-4e9d-a5be-18ec3d804428
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Tests for the Cypher expression -> sqlglot lowering (REQ-913).

A minimal fake ``ExprContext`` resolves the scoped leaves (variables, properties, params, functions)
to plain columns so the context-free lowering can be asserted as concrete SQL.
"""

import sqlglot.expressions as exp

from provisa.cypher.expr_ast import MapProjection, PatternComprehension, SubqueryExpr
from provisa.cypher.expr_parser import parse_expression as P
from provisa.cypher.expr_visitor import ExprLowering


class _FakeCtx:
    """Resolves n.prop -> "n"."prop", a bare var -> "var", $p -> a placeholder, fns -> AS-IS."""

    def resolve_variable(self, name):
        return exp.column(name, quoted=True)

    def resolve_property(self, obj, name):
        table = obj.this if isinstance(obj, exp.Column) else obj
        return exp.column(name, table=getattr(table, "name", None), quoted=True)

    def resolve_parameter(self, name):
        return exp.Placeholder(this=name)

    def resolve_function(self, name, args, *, distinct):
        return exp.Anonymous(this=name, expressions=args)

    def resolve_label_predicate(self, operand, labels):
        return exp.Anonymous(
            this="has_label", expressions=[operand, exp.Literal.string(":".join(labels))]
        )

    def resolve_map_projection(self, node: MapProjection):
        return exp.Anonymous(this="map_projection", expressions=[exp.column(node.var)])

    def resolve_subquery(self, node: SubqueryExpr):
        return exp.Anonymous(this=node.kind.lower(), expressions=[])

    def resolve_pattern_comprehension(self, node: PatternComprehension):
        return exp.Anonymous(this="pattern_comp", expressions=[])


def _sql(text: str, dialect: str = "trino") -> str:
    node = ExprLowering(_FakeCtx()).lower(P(text))
    return node.sql(dialect=dialect)


class TestOperators:
    def test_comparison(self):
        assert _sql("n.age > 30") == '"n"."age" > 30'

    def test_arithmetic_precedence(self):
        assert _sql("1 + 2 * 3") == "1 + 2 * 3"

    def test_power(self):
        assert _sql("n.x ^ 2") == 'POWER("n"."x", 2)'

    def test_boolean(self):
        assert _sql("a AND b OR c") == '"a" AND "b" OR "c"'

    def test_not_and_isnull(self):
        assert _sql("n.x IS NULL") == '"n"."x" IS NULL'
        assert _sql("n.x IS NOT NULL") == 'NOT "n"."x" IS NULL'


class TestStringPredicates:
    def test_starts_with(self):
        assert _sql("n.name STARTS WITH 'A'") == 'STARTS_WITH("n"."name", \'A\')'

    def test_ends_with(self):
        assert _sql("n.name ENDS WITH 'z'") == "(\"n\".\"name\" LIKE CONCAT('%', 'z'))"

    def test_contains(self):
        assert _sql("n.name CONTAINS 'x'") == '(STRPOS("n"."name", \'x\') > 0)'

    def test_regex(self):
        assert _sql("n.name =~ '.*'") == 'REGEXP_LIKE("n"."name", \'.*\')'

    def test_in_list(self):
        assert _sql("n.x IN [1, 2, 3]") == '"n"."x" IN (1, 2, 3)'


class TestStructures:
    def test_case(self):
        out = _sql("CASE WHEN n.x > 0 THEN 'p' ELSE 'n' END")
        assert out.startswith("CASE WHEN") and "ELSE 'n'" in out and out.endswith("END")

    def test_list_literal(self):
        assert _sql("[1, 2, 3]") == "ARRAY[1, 2, 3]"

    def test_index_is_one_based(self):
        # Cypher 0-indexed -> engine element_at 1-indexed
        assert _sql("n.tags[0]") == 'ELEMENT_AT("n"."tags", 1)'


class TestListFunctions:
    def test_list_comprehension_filter_and_transform(self):
        out = _sql("[x IN n.list WHERE x > 0 | x * 2]")
        assert "FILTER(" in out.upper() and "TRANSFORM(" in out.upper()

    def test_quantifier_all(self):
        assert "ALL_MATCH(" in _sql("all(x IN n.l WHERE x > 0)").upper()

    def test_quantifier_single(self):
        out = _sql("single(x IN n.l WHERE x > 0)").upper()
        assert "CARDINALITY(FILTER(" in out and out.endswith("= 1")

    def test_reduce(self):
        out = _sql("reduce(s = 0, x IN n.nums | s + x)").upper()
        assert out.startswith("REDUCE(") and "->" in out
