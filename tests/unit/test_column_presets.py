# Copyright (c) 2026 Kenneth Stott
# Canary: e622509a-5330-4f65-8164-9ccdc811c1ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for apply_column_presets in provisa/compiler/mutation_gen.py.

Tests cover all three preset sources (now, header, literal), edge cases
for missing headers and unknown sources, and multi-preset combinations.

Presets are plain dicts matching the ColumnPreset schema:
  {"column": str, "source": str, "name": str|None, "value": str|None}
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from provisa.compiler.mutation_gen import apply_column_presets


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _preset(column: str, source: str, *, name: str | None = None, value: str | None = None) -> dict:
    """Build a preset dict matching the ColumnPreset field layout."""
    return {"column": column, "source": source, "name": name, "value": value}


# ---------------------------------------------------------------------------
# source="now"
# ---------------------------------------------------------------------------

class TestSourceNow:
    def test_now_sets_iso_datetime_string(self):
        """source=now injects a valid ISO-format UTC datetime string."""
        presets = [_preset("created_at", "now")]
        result = apply_column_presets({}, presets)

        assert "created_at" in result
        # Must parse as a datetime without raising
        parsed = datetime.fromisoformat(result["created_at"])
        # The value must include timezone offset (UTC)
        assert parsed.utcoffset() is not None

    def test_now_overrides_existing_user_value(self):
        """source=now replaces any value already present in input_data."""
        input_data = {"created_at": "2000-01-01T00:00:00"}
        presets = [_preset("created_at", "now")]
        result = apply_column_presets(input_data, presets)

        assert result["created_at"] != "2000-01-01T00:00:00"
        # Confirm it is still a parseable ISO datetime
        datetime.fromisoformat(result["created_at"])

    def test_now_value_is_close_to_current_time(self):
        """source=now value is within a few seconds of the actual current UTC time."""
        before = datetime.now(timezone.utc)
        presets = [_preset("ts", "now")]
        result = apply_column_presets({}, presets)
        after = datetime.now(timezone.utc)

        parsed = datetime.fromisoformat(result["ts"])
        assert before <= parsed <= after


# ---------------------------------------------------------------------------
# source="header"
# ---------------------------------------------------------------------------

class TestSourceHeader:
    def test_header_reads_correct_header_name(self):
        """source=header injects the value of the named request header."""
        presets = [_preset("tenant_id", "header", name="X-Tenant-ID")]
        headers = {"X-Tenant-ID": "acme-corp", "Authorization": "Bearer tok"}
        result = apply_column_presets({}, presets, headers=headers)

        assert result["tenant_id"] == "acme-corp"

    def test_header_reads_correct_header_among_multiple(self):
        """source=header uses the exact header name from the preset, not others."""
        presets = [_preset("user_id", "header", name="X-User-ID")]
        headers = {"X-User-ID": "u-42", "X-Tenant-ID": "acme"}
        result = apply_column_presets({}, presets, headers=headers)

        assert result["user_id"] == "u-42"

    def test_header_missing_key_leaves_column_unset(self):
        """source=header with a header name absent from headers does not set the column.

        The implementation guards with `if headers and header_name in headers`,
        so a missing key means the column is simply not written to the result.
        """
        presets = [_preset("tenant_id", "header", name="X-Tenant-ID")]
        headers = {"Authorization": "Bearer tok"}  # X-Tenant-ID absent
        result = apply_column_presets({"name": "alice"}, presets, headers=headers)

        assert "tenant_id" not in result
        assert result["name"] == "alice"  # other fields untouched

    def test_header_none_headers_dict_leaves_column_unset(self):
        """source=header when headers=None does not set the column."""
        presets = [_preset("tenant_id", "header", name="X-Tenant-ID")]
        result = apply_column_presets({}, presets, headers=None)

        assert "tenant_id" not in result

    def test_header_overrides_existing_value(self):
        """source=header replaces a pre-existing column value in input_data."""
        presets = [_preset("user_id", "header", name="X-User-ID")]
        headers = {"X-User-ID": "u-99"}
        result = apply_column_presets({"user_id": "old-value"}, presets, headers=headers)

        assert result["user_id"] == "u-99"


# ---------------------------------------------------------------------------
# source="literal"
# ---------------------------------------------------------------------------

class TestSourceLiteral:
    def test_literal_injects_fixed_value(self):
        """source=literal injects the preset's value field verbatim."""
        presets = [_preset("status", "literal", value="pending")]
        result = apply_column_presets({}, presets)

        assert result["status"] == "pending"

    def test_literal_overrides_existing_value(self):
        """source=literal replaces any user-supplied value for the column."""
        presets = [_preset("status", "literal", value="active")]
        result = apply_column_presets({"status": "draft"}, presets)

        assert result["status"] == "active"

    def test_literal_none_value_injects_none(self):
        """source=literal with value=None injects None (dict.get returns None when key exists)."""
        presets = [_preset("flag", "literal", value=None)]
        result = apply_column_presets({}, presets)

        assert result["flag"] is None

    def test_literal_empty_string_value(self):
        """source=literal with value='' injects an empty string."""
        presets = [_preset("notes", "literal", value="")]
        result = apply_column_presets({"notes": "original"}, presets)

        assert result["notes"] == ""


# ---------------------------------------------------------------------------
# Combined / multi-preset scenarios
# ---------------------------------------------------------------------------

class TestMultiplePresets:
    def test_multiple_presets_all_applied(self):
        """All presets in the list are applied to the result dict."""
        presets = [
            _preset("created_at", "now"),
            _preset("status", "literal", value="active"),
            _preset("tenant_id", "header", name="X-Tenant"),
        ]
        headers = {"X-Tenant": "tenant-7"}
        result = apply_column_presets({"amount": 100}, presets, headers=headers)

        assert "created_at" in result
        assert result["status"] == "active"
        assert result["tenant_id"] == "tenant-7"
        assert result["amount"] == 100  # untouched field preserved

    def test_presets_applied_in_order_last_wins(self):
        """When two presets target the same column the later one wins."""
        presets = [
            _preset("status", "literal", value="first"),
            _preset("status", "literal", value="second"),
        ]
        result = apply_column_presets({}, presets)

        assert result["status"] == "second"


# ---------------------------------------------------------------------------
# Empty presets list
# ---------------------------------------------------------------------------

class TestEmptyPresets:
    def test_empty_presets_returns_input_unchanged(self):
        """No presets → input_data returned as-is (a shallow copy)."""
        input_data = {"col_a": 1, "col_b": "hello"}
        result = apply_column_presets(input_data, [])

        assert result == input_data

    def test_empty_presets_does_not_mutate_input(self):
        """apply_column_presets must not mutate the original input_data dict."""
        input_data = {"col_a": 1}
        result = apply_column_presets(input_data, [_preset("col_a", "literal", value="changed")])

        assert input_data["col_a"] == 1  # original untouched
        assert result["col_a"] == "changed"


# ---------------------------------------------------------------------------
# Unknown / unsupported source type
# ---------------------------------------------------------------------------

class TestUnknownSource:
    def test_unknown_source_leaves_column_unset(self):
        """An unrecognised source value silently skips the preset (no if-branch matches).

        The implementation has no else-clause so unknown sources are ignored.
        """
        presets = [_preset("col", "magic_value")]
        result = apply_column_presets({"other": 42}, presets)

        assert "col" not in result
        assert result["other"] == 42


# ---------------------------------------------------------------------------
# Preset targeting a column not already in input_data
# ---------------------------------------------------------------------------

class TestColumnNotInInput:
    def test_literal_adds_new_column(self):
        """source=literal adds the column even when absent from input_data."""
        presets = [_preset("audit_user", "literal", value="system")]
        result = apply_column_presets({}, presets)

        assert result["audit_user"] == "system"

    def test_now_adds_new_column(self):
        """source=now adds the column even when absent from input_data."""
        presets = [_preset("updated_at", "now")]
        result = apply_column_presets({}, presets)

        assert "updated_at" in result
        datetime.fromisoformat(result["updated_at"])  # parseable ISO string

    def test_header_present_adds_new_column(self):
        """source=header adds the column when header is present but column was absent."""
        presets = [_preset("org_id", "header", name="X-Org")]
        result = apply_column_presets({}, presets, headers={"X-Org": "org-5"})

        assert result["org_id"] == "org-5"
