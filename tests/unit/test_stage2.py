# Copyright (c) 2026 Kenneth Stott
# Canary: 922b19ba-a234-47bc-9f22-8475a0983746
# (run scripts/canary_stamp.py on this file after creating it)

"""Unit tests for Stage 2 SQL governance transformer."""

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.security.masking import MaskingRule, MaskType


def _gov(**kwargs) -> GovernanceContext:
    defaults = dict(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={},
        all_columns={},
        limit_ceiling=None,
        sample_size=None,
    )
    defaults.update(kwargs)
    return GovernanceContext(**defaults)


class TestRLSInjection:
    def test_rls_where_injected(self):
        gov = _gov(
            rls_rules={1: "region = 'us'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "WHERE" in result
        assert "region = 'us'" in result

    def test_rls_ands_with_existing_where(self):
        gov = _gov(
            rls_rules={1: "region = 'us'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders WHERE status = 'active'"
        result = apply_governance(sql, gov)
        assert "region = 'us'" in result
        assert "status = 'active'" in result
        assert "AND" in result

    def test_rls_injected_on_subquery(self):
        gov = _gov(
            rls_rules={1: "active = true"},
            table_map={"orders": 1},
        )
        sql = "SELECT * FROM (SELECT id FROM orders) AS sub"
        result = apply_governance(sql, gov)
        assert "active" in result.lower() and "true" in result.lower()

    def test_rls_injected_for_joined_table(self):
        gov = _gov(
            rls_rules={1: "region = 'us'", 2: "active = true"},
            table_map={"orders": 1, "customers": 2},
        )
        sql = "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id"
        result = apply_governance(sql, gov)
        assert "region = 'us'" in result
        assert "active" in result.lower() and "true" in result.lower()

    def test_no_rls_rule_unchanged(self):
        gov = _gov(
            rls_rules={99: "x = 1"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "WHERE" not in result


class TestMaskingInjection:
    def test_masked_column_wrapped(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        gov = _gov(
            masking_rules={(1, "email"): (rule, "varchar")},
            table_map={"users": 1},
        )
        sql = "SELECT email FROM users"
        result = apply_governance(sql, gov)
        assert "REGEXP_REPLACE" in result
        assert "email" in result

    def test_non_masked_column_unchanged(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        gov = _gov(
            masking_rules={(1, "email"): (rule, "varchar")},
            table_map={"users": 1},
        )
        sql = "SELECT id FROM users"
        result = apply_governance(sql, gov)
        assert "REGEXP_REPLACE" not in result
        assert "id" in result

    def test_constant_mask_applied(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        gov = _gov(
            masking_rules={(1, "secret"): (rule, "varchar")},
            table_map={"data": 1},
        )
        sql = "SELECT secret FROM data"
        result = apply_governance(sql, gov)
        assert "NULL" in result


class TestVisibilityFiltering:
    def test_invisible_column_removed(self):
        gov = _gov(
            visible_columns={1: frozenset({"id"})},
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("secret", "varchar")]},
        )
        sql = "SELECT id, secret FROM orders"
        result = apply_governance(sql, gov)
        # invisible column replaced with NULL
        assert "secret" not in result or "NULL" in result

    def test_visible_column_kept(self):
        gov = _gov(
            visible_columns={1: frozenset({"id", "amount"})},
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("amount", "numeric")]},
        )
        sql = "SELECT id, amount FROM orders"
        result = apply_governance(sql, gov)
        assert "id" in result
        assert "amount" in result


class TestLimitCeiling:
    def test_limit_injected_when_none(self):
        gov = _gov(limit_ceiling=100)
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "LIMIT 100" in result

    def test_limit_capped_when_over_ceiling(self):
        gov = _gov(limit_ceiling=50)
        sql = "SELECT id FROM orders LIMIT 1000"
        result = apply_governance(sql, gov)
        assert "LIMIT 50" in result
        assert "LIMIT 1000" not in result

    def test_limit_not_reduced_when_under_ceiling(self):
        gov = _gov(limit_ceiling=100)
        sql = "SELECT id FROM orders LIMIT 10"
        result = apply_governance(sql, gov)
        assert "LIMIT 10" in result

    def test_sample_size_used_when_no_ceiling(self):
        gov = _gov(sample_size=25)
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "LIMIT 25" in result


class TestStarExpansion:
    def test_star_expanded_to_columns(self):
        gov = _gov(
            visible_columns={1: frozenset({"id", "name"})},
            table_map={"users": 1},
            all_columns={1: [("id", "integer"), ("name", "varchar"), ("secret", "varchar")]},
        )
        sql = "SELECT * FROM users"
        result = apply_governance(sql, gov)
        assert "id" in result
        assert "name" in result
        assert "secret" not in result

    def test_star_with_masking(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        gov = _gov(
            visible_columns={1: None},
            masking_rules={(1, "email"): (rule, "varchar")},
            table_map={"users": 1},
            all_columns={1: [("id", "integer"), ("email", "varchar")]},
        )
        sql = "SELECT * FROM users"
        result = apply_governance(sql, gov)
        assert "NULL" in result


class TestUnionGovernance:
    def test_union_branches_governed_independently(self):
        gov = _gov(
            rls_rules={1: "region = 'us'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders UNION ALL SELECT id FROM orders"
        result = apply_governance(sql, gov)
        # Both branches should have RLS applied
        assert result.count("region = 'us'") >= 2
