# Copyright (c) 2026 Kenneth Stott
# Canary: 0349e6e4-dd92-4e2a-9563-234e20c1379f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Phase-3 tests: expression lowering against real ``_Translator`` state (REQ-913).

Builds a translator with a hand-populated ``_var_table`` and asserts the ``TranslatorExprContext``
resolves the scoped leaves (property/param/function) exactly as the old regex path did (postgres IR:
quoted column name, unquoted table alias).
"""

from provisa.cypher.expr_context import TranslatorExprContext
from provisa.cypher.expr_parser import parse_expression as P
from provisa.cypher.expr_visitor import ExprLowering
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import _Translator
from provisa.cypher.translator_helpers import _rewrite_cypher_fn_node


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


def _translator() -> _Translator:
    lm = _label_map()
    ast = parse_cypher("MATCH (n:Person) RETURN n")
    t = _Translator(ast, lm, {})
    t._var_table = {"n": ("n", lm.nodes["Person"])}
    return t


def _sql(text: str, t: _Translator | None = None, dialect: str = "postgres") -> str:
    t = t or _translator()
    node = ExprLowering(TranslatorExprContext(t)).lower(P(text))
    return node.transform(_rewrite_cypher_fn_node).sql(dialect=dialect)


class TestPropertyResolution:
    def test_mapped_property_identity(self):
        assert _sql("n.name") == 'n."name"'

    def test_mapped_property_alias(self):
        assert _sql("n.age") == 'n."age_col"'

    def test_native_filter_column(self):
        assert _sql("n.status") == 'n."_nf_status"'

    def test_unmapped_property_quoted(self):
        assert _sql("n.unknown") == 'n."unknown"'

    def test_property_in_predicate(self):
        assert _sql("n.age > 30") == 'n."age_col" > 30'


class TestParameters:
    def test_single_param(self):
        assert _sql("$min") == "$1"

    def test_param_positional_order(self):
        t = _translator()
        assert _sql("n.age > $min AND n.name = $name", t) == 'n."age_col" > $1 AND n."name" = $2'
        assert t._param_order == ["min", "name"]


class TestFunctions:
    def test_rename_applied_post_pass(self):
        assert _sql("toLower(n.name)") == 'LOWER(n."name")'

    def test_unknown_function_preserved(self):
        assert _sql("my_udf(n.age)") == 'MY_UDF(n."age_col")'


class TestLabelPredicate:
    def test_matching_label_is_true(self):
        assert _sql("n:Person") == "TRUE"

    def test_nonmatching_label_is_false(self):
        assert _sql("n:Company") == "FALSE"

    def test_is_not_label(self):
        assert _sql("n IS NOT :Company").upper() == "NOT FALSE"


class TestMapProjection:
    def test_dotted_selectors(self):
        assert _sql("n{.name, .age}") == "MAP(ARRAY['name', 'age'], ARRAY[n.\"name\", n.\"age\"])"

    def test_all_props_expands_sorted(self):
        # .* expands the node's known cypher property names, sorted (raw names, not SQL aliases).
        assert _sql("n{.*}") == "MAP(ARRAY['age', 'name'], ARRAY[n.\"age\", n.\"name\"])"

    def test_literal_entry_lowers_value(self):
        assert _sql("n{label: n.name}") == "MAP(ARRAY['label'], ARRAY[n.\"name\"])"


class TestPatternComprehension:
    def test_correlated_array_subquery(self):
        out = _sql("[(n)-[:KNOWS]->(m) | m.name]")
        assert out == (
            'ARRAY(SELECT m."name" FROM "postgresql"."public"."persons" AS m '
            'WHERE n."person_id" = m."id")'
        )

    def test_with_predicate(self):
        out = _sql("[(n)-[:KNOWS]->(m) WHERE m.age > 20 | m.name]")
        assert out == (
            'ARRAY(SELECT m."name" FROM "postgresql"."public"."persons" AS m '
            'WHERE n."person_id" = m."id" AND m."age_col" > 20)'
        )


class TestSubqueryRecursion:
    def test_exists_becomes_correlated_subquery(self):
        out = _sql("EXISTS { MATCH (n)-[:KNOWS]->(m) }").upper()
        assert out.startswith("EXISTS(SELECT 1 FROM")
        assert "INNER JOIN" in out and 'ON N."PERSON_ID" = M."ID"' in out

    def test_count_becomes_scalar_subquery(self):
        out = _sql("COUNT { MATCH (n)-[:KNOWS]->(m) }").upper()
        assert "SELECT COUNT(*)" in out and out.strip().startswith("(SELECT COUNT(*)")
