# Copyright (c) 2026 Kenneth Stott
# Canary: 5dc0b5f1-b3a7-4d37-90af-e96444057ad7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for masking expression generation and validation."""

import pytest

from provisa.security.masking import (
    MaskType,
    MaskingRule,
    MaskingValidationError,
    build_mask_expression,
    validate_masking_rule,
)


class TestValidateMaskingRule:
    def test_regex_on_varchar_passes(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*$", replace="$1***")
        validate_masking_rule(rule, "email", "varchar", True)

    def test_regex_on_integer_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        with pytest.raises(MaskingValidationError, match="regex masking is only supported on string"):
            validate_masking_rule(rule, "amount", "integer", True)

    def test_regex_on_bigint_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        with pytest.raises(MaskingValidationError, match="regex masking is only supported on string"):
            validate_masking_rule(rule, "id", "bigint", True)

    def test_regex_missing_pattern_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=None, replace="X")
        with pytest.raises(MaskingValidationError, match="requires 'pattern' and 'replace'"):
            validate_masking_rule(rule, "email", "varchar", True)

    def test_regex_missing_replace_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace=None)
        with pytest.raises(MaskingValidationError, match="requires 'pattern' and 'replace'"):
            validate_masking_rule(rule, "email", "varchar", True)

    def test_truncate_on_timestamp_passes(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="month")
        validate_masking_rule(rule, "created_at", "timestamp", True)

    def test_truncate_on_date_passes(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="year")
        validate_masking_rule(rule, "birthday", "date", True)

    def test_truncate_on_integer_raises(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="month")
        with pytest.raises(MaskingValidationError, match="truncate masking is only supported on date"):
            validate_masking_rule(rule, "amount", "integer", True)

    def test_truncate_missing_precision_raises(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision=None)
        with pytest.raises(MaskingValidationError, match="requires 'precision'"):
            validate_masking_rule(rule, "created_at", "timestamp", True)

    def test_constant_null_on_nullable_passes(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        validate_masking_rule(rule, "email", "varchar", is_nullable=True)

    def test_constant_null_on_not_null_raises(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        with pytest.raises(MaskingValidationError, match="NOT NULL"):
            validate_masking_rule(rule, "email", "varchar", is_nullable=False)

    def test_constant_zero_on_integer_passes(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        validate_masking_rule(rule, "amount", "integer", True)

    def test_constant_string_on_varchar_passes(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="***@***.***")
        validate_masking_rule(rule, "email", "varchar", True)

    def test_regex_on_parameterized_varchar_passes(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        validate_masking_rule(rule, "name", "varchar(255)", True)


class TestBuildMaskExpression:
    def test_regex_expression(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2")
        expr = build_mask_expression(rule, '"t0"."email"', "varchar")
        assert expr == "REGEXP_REPLACE(\"t0\".\"email\", '^(.{2}).*(@.*)$', '$1***$2')"

    def test_constant_zero(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        expr = build_mask_expression(rule, '"t0"."amount"', "integer")
        assert expr == "0"

    def test_constant_null(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        expr = build_mask_expression(rule, '"t0"."email"', "varchar")
        assert expr == "NULL"

    def test_constant_string(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="***@***.***")
        expr = build_mask_expression(rule, '"email"', "varchar")
        assert expr == "'***@***.***'"

    def test_constant_string_with_quote(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="it's hidden")
        expr = build_mask_expression(rule, '"name"', "varchar")
        assert expr == "'it''s hidden'"

    def test_truncate_month(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="month")
        expr = build_mask_expression(rule, '"t0"."created_at"', "timestamp")
        assert expr == "DATE_TRUNC('month', \"t0\".\"created_at\")"

    def test_truncate_year(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="year")
        expr = build_mask_expression(rule, '"birthday"', "date")
        assert expr == "DATE_TRUNC('year', \"birthday\")"

    def test_constant_max_integer(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="MAX")
        expr = build_mask_expression(rule, '"amount"', "integer")
        assert expr == "2147483647"

    def test_constant_min_bigint(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="MIN")
        expr = build_mask_expression(rule, '"id"', "bigint")
        assert expr == "-9223372036854775808"

    def test_constant_max_non_integer_falls_through(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="MAX")
        expr = build_mask_expression(rule, '"val"', "varchar")
        assert expr == "'MAX'"

    def test_constant_boolean_true(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=True)
        expr = build_mask_expression(rule, '"flag"', "boolean")
        assert expr == "TRUE"

    def test_constant_boolean_false(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=False)
        expr = build_mask_expression(rule, '"flag"', "boolean")
        assert expr == "FALSE"
