# Copyright (c) 2025 Kenneth Stott
# Canary: 646f6e47-50ec-442b-ad79-176c36b61068
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column-level data masking expression generation (REQ-087 through REQ-091).

Generates SQL expressions that replace column values per (column, role):
  - regex: REGEXP_REPLACE for string columns
  - constant: literal value (NULL, 0, custom)
  - truncate: DATE_TRUNC for date/timestamp columns

Type validation ensures invalid combinations are rejected at config load time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MaskType(str, Enum):
    regex = "regex"
    constant = "constant"
    truncate = "truncate"


# Trino base types that support regex masking
_STRING_TYPES = {"varchar", "char", "text", "varbinary"}

# Trino base types that support truncate masking
_TEMPORAL_TYPES = {"date", "time", "timestamp", "time with time zone", "timestamp with time zone"}

# Trino integer type bounds for MAX/MIN constant resolution
_INTEGER_BOUNDS: dict[str, tuple[int, int]] = {
    "tinyint": (-128, 127),
    "smallint": (-32768, 32767),
    "integer": (-2147483648, 2147483647),
    "int": (-2147483648, 2147483647),
    "bigint": (-9223372036854775808, 9223372036854775807),
}


def _base_type(trino_type: str) -> str:
    """Normalize parameterized types: varchar(100) → varchar."""
    return trino_type.lower().split("(")[0].strip()


@dataclass(frozen=True)
class MaskingRule:
    """A masking rule for a specific column and role."""

    mask_type: MaskType
    # regex fields
    pattern: str | None = None
    replace: str | None = None
    # constant fields
    value: object = None
    # truncate fields
    precision: str | None = None


class MaskingValidationError(Exception):
    """Raised when masking config is invalid for the column type."""


def validate_masking_rule(
    rule: MaskingRule,
    column_name: str,
    data_type: str,
    is_nullable: bool,
) -> None:
    """Validate a masking rule against the column's data type.

    Raises MaskingValidationError on invalid combinations.
    """
    base = _base_type(data_type)

    if rule.mask_type == MaskType.regex:
        if base not in _STRING_TYPES:
            raise MaskingValidationError(
                f"Column {column_name!r} has type {data_type!r}: "
                f"regex masking is only supported on string types "
                f"({', '.join(sorted(_STRING_TYPES))})"
            )
        if not rule.pattern or rule.replace is None:
            raise MaskingValidationError(
                f"Column {column_name!r}: regex masking requires 'pattern' and 'replace'"
            )

    elif rule.mask_type == MaskType.truncate:
        if base not in _TEMPORAL_TYPES:
            raise MaskingValidationError(
                f"Column {column_name!r} has type {data_type!r}: "
                f"truncate masking is only supported on date/timestamp types "
                f"({', '.join(sorted(_TEMPORAL_TYPES))})"
            )
        if not rule.precision:
            raise MaskingValidationError(
                f"Column {column_name!r}: truncate masking requires 'precision'"
            )

    elif rule.mask_type == MaskType.constant:
        if rule.value is None and not is_nullable:
            raise MaskingValidationError(
                f"Column {column_name!r} is NOT NULL: "
                f"cannot use NULL as constant mask value"
            )


def _resolve_constant(value: object, data_type: str) -> str:
    """Resolve a constant mask value to a SQL literal.

    Handles special values: NULL, MAX, MIN (resolved from column type bounds).
    """
    if value is None:
        return "NULL"

    if isinstance(value, str):
        upper = value.upper()
        base = _base_type(data_type)

        if upper == "MAX" and base in _INTEGER_BOUNDS:
            return str(_INTEGER_BOUNDS[base][1])
        if upper == "MIN" and base in _INTEGER_BOUNDS:
            return str(_INTEGER_BOUNDS[base][0])

        # String literal
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def build_mask_expression(
    rule: MaskingRule,
    column_ref: str,
    data_type: str,
) -> str:
    """Build the SQL expression that replaces a column reference.

    Args:
        rule: The masking rule to apply.
        column_ref: The fully qualified column reference (e.g., '"t0"."email"').
        data_type: The Trino data type of the column.

    Returns:
        SQL expression string.
    """
    if rule.mask_type == MaskType.regex:
        pattern = rule.pattern.replace("'", "''")
        replace = rule.replace.replace("'", "''")
        return f"REGEXP_REPLACE({column_ref}, '{pattern}', '{replace}')"

    if rule.mask_type == MaskType.constant:
        return _resolve_constant(rule.value, data_type)

    if rule.mask_type == MaskType.truncate:
        return f"DATE_TRUNC('{rule.precision}', {column_ref})"

    raise MaskingValidationError(f"Unknown mask type: {rule.mask_type!r}")
