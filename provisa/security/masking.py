# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-038, REQ-040, REQ-042, REQ-263


class MaskType(str, Enum):
    regex = "regex"
    constant = "constant"
    truncate = "truncate"


# the engine base types that support regex masking
_STRING_TYPES = {"varchar", "char", "text", "varbinary"}

# the engine base types that support truncate masking
_TEMPORAL_TYPES = {"date", "time", "timestamp", "time with time zone", "timestamp with time zone"}

# the engine integer type bounds for MAX/MIN constant resolution
_INTEGER_BOUNDS: dict[str, tuple[int, int]] = {
    "tinyint": (-128, 127),
    "smallint": (-32768, 32767),
    "integer": (-2147483648, 2147483647),
    "int": (-2147483648, 2147483647),
    "bigint": (-9223372036854775808, 9223372036854775807),
}


def _base_type(column_type: str) -> str:
    """Normalize parameterized types: varchar(100) → varchar."""
    return column_type.lower().split("(")[0].strip()


@dataclass(frozen=True)
class MaskingRule:  # REQ-038, REQ-040, REQ-263
    """A masking rule for a specific column and role."""

    mask_type: MaskType
    # regex fields
    pattern: str | None = None
    replace: str | None = None
    # constant fields
    value: int | float | str | None = None
    # truncate fields
    precision: str | None = None


class MaskingValidationError(Exception):
    """Raised when masking config is invalid for the column type."""


def validate_masking_rule(  # REQ-038, REQ-040, REQ-042
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
                f"Column {column_name!r} is NOT NULL: cannot use NULL as constant mask value"
            )


def _resolve_constant(value: int | float | str | None, data_type: str) -> str:
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


def build_mask_expression(  # REQ-040, REQ-263
    rule: MaskingRule,
    column_ref: str,
    data_type: str,
) -> str:
    """Build the SQL expression that replaces a column reference.

    Args:
        rule: The masking rule to apply.
        column_ref: The fully qualified column reference (e.g., '"t0"."email"').
        data_type: The engine data type of the column.

    Returns:
        SQL expression string.
    """
    if rule.mask_type == MaskType.regex:
        assert rule.pattern is not None
        assert rule.replace is not None
        pattern = rule.pattern.replace("'", "''")
        replace = rule.replace.replace("'", "''")
        return f"REGEXP_REPLACE({column_ref}, '{pattern}', '{replace}')"

    if rule.mask_type == MaskType.constant:
        return _resolve_constant(rule.value, data_type)

    if rule.mask_type == MaskType.truncate:
        return f"DATE_TRUNC('{rule.precision}', {column_ref})"

    raise MaskingValidationError(f"Unknown mask type: {rule.mask_type!r}")


def _resolve_constant_value(value: int | float | str | None, data_type: str):
    """Python equivalent of _resolve_constant: return the actual masked value, not a SQL literal."""
    if value is None:
        return None
    if isinstance(value, str):
        upper = value.upper()
        base = _base_type(data_type)
        if upper == "MAX" and base in _INTEGER_BOUNDS:
            return _INTEGER_BOUNDS[base][1]
        if upper == "MIN" and base in _INTEGER_BOUNDS:
            return _INTEGER_BOUNDS[base][0]
    return value


def _convert_regexp_replacement(replace: str) -> str:
    """Convert the engine REGEXP_REPLACE replacement syntax ($1, $$) to Python re.sub (\\g<1>, $)."""
    out: list[str] = []
    i = 0
    while i < len(replace):
        ch = replace[i]
        if ch == "$" and i + 1 < len(replace):
            nxt = replace[i + 1]
            if nxt == "$":
                out.append("$")
                i += 2
                continue
            if nxt.isdigit():
                j = i + 1
                while j < len(replace) and replace[j].isdigit():
                    j += 1
                out.append(f"\\g<{replace[i + 1 : j]}>")
                i = j
                continue
        if ch == "\\":
            out.append("\\\\")  # escape literal backslash for re.sub
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _truncate_temporal(value, precision: str):
    """Python equivalent of DATE_TRUNC for change-event rows (datetime/date or ISO string)."""
    import datetime as _dt

    if isinstance(value, str):
        parsed = _dt.datetime.fromisoformat(value)  # raises ValueError on a non-ISO string
    elif isinstance(value, _dt.datetime):
        parsed = value
    elif isinstance(value, _dt.date):
        parsed = _dt.datetime(value.year, value.month, value.day)
    else:
        raise MaskingValidationError(
            f"truncate mask cannot be applied to value of type {type(value).__name__}"
        )

    p = precision.lower()
    if p == "year":
        return parsed.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if p == "quarter":
        q_month = 3 * ((parsed.month - 1) // 3) + 1
        return parsed.replace(month=q_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if p == "month":
        return parsed.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if p == "week":  # DATE_TRUNC week → Monday
        monday = parsed - _dt.timedelta(days=parsed.weekday())
        return monday.replace(hour=0, minute=0, second=0, microsecond=0)
    if p == "day":
        return parsed.replace(hour=0, minute=0, second=0, microsecond=0)
    if p == "hour":
        return parsed.replace(minute=0, second=0, microsecond=0)
    if p == "minute":
        return parsed.replace(second=0, microsecond=0)
    if p == "second":
        return parsed.replace(microsecond=0)
    raise MaskingValidationError(f"Unknown truncate precision: {precision!r}")


def apply_mask_to_value(rule: MaskingRule, value, data_type: str):  # REQ-040, REQ-263, REQ-336
    """Apply a masking rule to a Python value (REQ-336).

    Mirrors build_mask_expression in Python for change-event subscription rows, which
    never pass through a SQL projection. Subscriptions thus enforce the same column
    masking as local-table queries.
    """
    if rule.mask_type == MaskType.constant:
        return _resolve_constant_value(rule.value, data_type)
    if value is None:
        return None
    if rule.mask_type == MaskType.regex:
        assert rule.pattern is not None
        assert rule.replace is not None
        import re

        return re.sub(rule.pattern, _convert_regexp_replacement(rule.replace), str(value))
    if rule.mask_type == MaskType.truncate:
        assert rule.precision is not None
        return _truncate_temporal(value, rule.precision)
    raise MaskingValidationError(f"Unknown mask type: {rule.mask_type!r}")
