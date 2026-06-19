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
    apply_mask_to_value,
    build_mask_expression,
    validate_masking_rule,
)

assert apply_mask_to_value  # used in TestApplyMaskToValue below


class TestValidateMaskingRule:
    def test_regex_on_varchar_passes(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*$", replace="$1***")
        validate_masking_rule(rule, "email", "varchar", True)

    def test_regex_on_integer_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        with pytest.raises(
            MaskingValidationError, match="regex masking is only supported on string"
        ):
            validate_masking_rule(rule, "amount", "integer", True)

    def test_regex_on_bigint_raises(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        with pytest.raises(
            MaskingValidationError, match="regex masking is only supported on string"
        ):
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
        with pytest.raises(
            MaskingValidationError, match="truncate masking is only supported on date"
        ):
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
        assert expr == 'DATE_TRUNC(\'month\', "t0"."created_at")'

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


class TestApplyMaskToValue:
    """REQ-336: Python-side masking for change-event subscription rows."""

    def test_regex_replaces_all_matches(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d", replace="X")
        assert apply_mask_to_value(rule, "ab12cd3", "varchar") == "abXXcdX"

    def test_regex_dollar_backreference_converted(self):
        # Trino $1 backref → Python \g<1>
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r"(\w+)@(\w+)", replace="$1@REDACTED")
        assert apply_mask_to_value(rule, "user@host", "varchar") == "user@REDACTED"

    def test_regex_literal_dollar_dollar(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d+", replace="$$")
        assert apply_mask_to_value(rule, "amt 500", "varchar") == "amt $"

    def test_regex_none_value_stays_none(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d", replace="X")
        assert apply_mask_to_value(rule, None, "varchar") is None

    def test_constant_string(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="REDACTED")
        assert apply_mask_to_value(rule, "secret", "varchar") == "REDACTED"

    def test_constant_null(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        assert apply_mask_to_value(rule, "secret", "varchar") is None

    def test_constant_int(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=0)
        assert apply_mask_to_value(rule, 12345, "integer") == 0

    def test_constant_max_resolved(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="MAX")
        assert apply_mask_to_value(rule, 5, "integer") == 2147483647

    def test_truncate_iso_string_to_month(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="month")
        out = apply_mask_to_value(rule, "2026-06-17T13:45:09", "timestamp")
        assert out.year == 2026 and out.month == 6 and out.day == 1
        assert out.hour == 0 and out.minute == 0 and out.second == 0

    def test_truncate_datetime_to_year(self):
        import datetime as dt

        rule = MaskingRule(mask_type=MaskType.truncate, precision="year")
        out = apply_mask_to_value(rule, dt.datetime(2026, 6, 17, 13, 45), "timestamp")
        assert out == dt.datetime(2026, 1, 1)

    def test_truncate_non_temporal_raises(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="day")
        with pytest.raises(MaskingValidationError):
            apply_mask_to_value(rule, 12345, "timestamp")

    def test_truncate_bad_iso_raises(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="day")
        with pytest.raises(ValueError):
            apply_mask_to_value(rule, "not-a-date", "timestamp")
