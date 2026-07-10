# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Golden-IR regression for the AST expression path (REQ-913).

Started life as an A/B differential against the legacy regex ``_build_where``; once that path was
deleted the expected values were frozen as golden postgres IR (the dialect the Cypher compiler emits;
engine-specific forms are added later by ``transpile_physical``). This pins the full parse → lower →
fn-rewrite pipeline against a representative corpus, so any change in emitted IR is caught.
"""

import pytest
import sqlglot

from provisa.cypher.expr_context import TranslatorExprContext
from provisa.cypher.expr_parser import parse_expression
from provisa.cypher.expr_visitor import ExprLowering
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import _Translator
from provisa.cypher.translator_helpers import _rewrite_cypher_fn_node

_IR = "postgres"

# expr -> expected normalised postgres IR.
_GOLDEN = {
    "n.age > 30": 'n."age_col" > 30',
    "n.name = 'Alice'": "n.\"name\" = 'Alice'",
    "n.age > $min AND n.age < $max": 'n."age_col" > $1 AND n."age_col" < $2',
    "n.name STARTS WITH 'A'": "STARTS_WITH(n.\"name\", 'A')",
    "n.name CONTAINS 'x'": "(POSITION('x' IN n.\"name\") > 0)",
    "n.prop =~ '.*foo'": "n.\"prop\" ~ '.*foo'",
    "n.age IN [1, 2, 3]": 'n."age_col" IN (1, 2, 3)',
    "n.status IS NULL": 'n."_nf_status" IS NULL',
    "n.status IS NOT NULL": 'NOT n."_nf_status" IS NULL',
    "NOT n.age > 5": 'NOT n."age_col" > 5',
    "n.age >= 18 OR n.age <= 65": 'n."age_col" >= 18 OR n."age_col" <= 65',
    "n.age + 1 > 10": 'n."age_col" + 1 > 10',
    "n.age * 2 = 40": 'n."age_col" * 2 = 40',
    "toLower(n.name) = 'bob'": "LOWER(n.\"name\") = 'bob'",
    "coalesce(n.name, 'none') = 'x'": "COALESCE(n.\"name\", 'none') = 'x'",
    "n.age > 30 AND (n.name STARTS WITH 'A' OR n.age < 5)": 'n."age_col" > 30 AND (STARTS_WITH(n."name", \'A\') OR n."age_col" < 5)',
    "n.age > 30 OR (n.name = 'x' AND n.age < 5)": 'n."age_col" > 30 OR (n."name" = \'x\' AND n."age_col" < 5)',
    "NOT (n.age > 5 OR n.name = 'x')": 'NOT (n."age_col" > 5 OR n."name" = \'x\')',
    # ENDS WITH lowers to `x LIKE '%' || y` (|| preserves NULL — an elective IR change from the old
    # CONCAT form, which coerced null to a match-all; functionally equal for non-null post-transpile).
    "n.name ENDS WITH 'z'": "(n.\"name\" LIKE '%' || 'z')",
}


def _label_map() -> CypherLabelMap:
    person = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age_col"},
        native_filter_columns={"status": "varchar"},
    )
    knows = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        join_source_column="person_id",
        join_target_column="id",
        field_name="knows",
    )
    return CypherLabelMap(nodes={"Person": person}, relationships={"KNOWS": knows}, domains={})


def _fresh() -> _Translator:
    lm = _label_map()
    t = _Translator(parse_cypher("MATCH (n:Person) RETURN n"), lm, {})
    t._var_table = {"n": ("n", lm.nodes["Person"])}
    return t


def _norm(sql: str) -> str:
    tree = sqlglot.parse_one(sql, dialect=_IR)
    assert tree is not None
    return tree.sql(dialect=_IR)


def _ir(expr: str) -> str:
    t = _fresh()
    node = ExprLowering(TranslatorExprContext(t)).lower(parse_expression(expr))
    return _norm(node.transform(_rewrite_cypher_fn_node).sql(dialect=_IR))


@pytest.mark.parametrize("expr,expected", list(_GOLDEN.items()))
def test_golden_ir(expr, expected):
    assert _ir(expr) == _norm(expected)
