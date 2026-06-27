# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Table aggregate config fields (REQ-734)."""

# Requirements: REQ-734

from __future__ import annotations


from provisa.core.models import Column, Table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table(
    enable_aggregates: bool = False,
    enable_group_by: bool = False,
) -> Table:
    return Table(
        source_id="pg",
        domain_id="default",
        schema_name="public",
        table_name="orders",
        columns=[
            Column(name="id", visible_to=["analyst"]),
            Column(name="amount", visible_to=["analyst"]),
        ],
        enable_aggregates=enable_aggregates,
        enable_group_by=enable_group_by,
    )


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


class TestTableAggregateConfigDefaults:
    def test_enable_aggregates_defaults_false(self):
        """enable_aggregates must default to False (opt-in)."""
        t = _table()
        assert t.enable_aggregates is False

    def test_enable_group_by_defaults_false(self):
        """enable_group_by must default to False (opt-in)."""
        t = _table()
        assert t.enable_group_by is False

    def test_both_flags_independent_of_each_other(self):
        """enable_aggregates and enable_group_by are independent boolean flags."""
        t1 = _table(enable_aggregates=True, enable_group_by=False)
        t2 = _table(enable_aggregates=False, enable_group_by=True)
        assert t1.enable_aggregates is True
        assert t1.enable_group_by is False
        assert t2.enable_aggregates is False
        assert t2.enable_group_by is True


# ---------------------------------------------------------------------------
# Explicit values
# ---------------------------------------------------------------------------


class TestTableAggregateConfigExplicitValues:
    def test_enable_aggregates_true(self):
        """enable_aggregates=True is stored and readable."""
        t = _table(enable_aggregates=True)
        assert t.enable_aggregates is True

    def test_enable_group_by_true(self):
        """enable_group_by=True is stored and readable."""
        t = _table(enable_group_by=True)
        assert t.enable_group_by is True

    def test_enable_aggregates_false_explicit(self):
        """Explicit enable_aggregates=False is idempotent with default."""
        t = _table(enable_aggregates=False)
        assert t.enable_aggregates is False

    def test_enable_group_by_false_explicit(self):
        """Explicit enable_group_by=False is idempotent with default."""
        t = _table(enable_group_by=False)
        assert t.enable_group_by is False

    def test_both_flags_true(self):
        """Both flags can be True simultaneously."""
        t = _table(enable_aggregates=True, enable_group_by=True)
        assert t.enable_aggregates is True
        assert t.enable_group_by is True


# ---------------------------------------------------------------------------
# Persistence roundtrip (model_dump / model_validate)
# ---------------------------------------------------------------------------


class TestTableAggregateConfigPersistence:
    def test_roundtrip_defaults(self):
        """Default values survive model_dump → model_validate roundtrip."""
        original = _table()
        data = original.model_dump(by_alias=True)
        restored = Table.model_validate(data)
        assert restored.enable_aggregates is False
        assert restored.enable_group_by is False

    def test_roundtrip_aggregates_enabled(self):
        """enable_aggregates=True survives roundtrip."""
        original = _table(enable_aggregates=True)
        data = original.model_dump(by_alias=True)
        assert data["enable_aggregates"] is True
        restored = Table.model_validate(data)
        assert restored.enable_aggregates is True

    def test_roundtrip_group_by_enabled(self):
        """enable_group_by=True survives roundtrip."""
        original = _table(enable_group_by=True)
        data = original.model_dump(by_alias=True)
        assert data["enable_group_by"] is True
        restored = Table.model_validate(data)
        assert restored.enable_group_by is True

    def test_roundtrip_both_enabled(self):
        """Both flags True survive roundtrip."""
        original = _table(enable_aggregates=True, enable_group_by=True)
        data = original.model_dump(by_alias=True)
        restored = Table.model_validate(data)
        assert restored.enable_aggregates is True
        assert restored.enable_group_by is True

    def test_dump_contains_both_keys(self):
        """model_dump output always contains enable_aggregates and enable_group_by keys."""
        data = _table().model_dump(by_alias=True)
        assert "enable_aggregates" in data
        assert "enable_group_by" in data

    def test_model_json_schema_includes_fields(self):
        """JSON schema for Table includes both aggregate flag fields."""
        schema = Table.model_json_schema()
        props = schema.get("properties", {})
        assert "enable_aggregates" in props
        assert "enable_group_by" in props


# ---------------------------------------------------------------------------
# Aggregate routing disabled when flag is False
# ---------------------------------------------------------------------------


class TestTableAggregateRoutingGating:
    def test_aggregate_disabled_by_default(self):
        """Table with default flags does not enable aggregate routing."""
        t = _table()
        assert not t.enable_aggregates, "aggregate queries must be opt-in"

    def test_aggregate_enabled_explicitly(self):
        """Setting enable_aggregates=True opens aggregate routing."""
        t = _table(enable_aggregates=True)
        assert t.enable_aggregates is True

    def test_group_by_disabled_by_default(self):
        """Table with default flags does not enable group-by routing."""
        t = _table()
        assert not t.enable_group_by, "group-by queries must be opt-in"

    def test_group_by_enabled_explicitly(self):
        """Setting enable_group_by=True opens group-by routing."""
        t = _table(enable_group_by=True)
        assert t.enable_group_by is True

    def test_disabling_aggregate_after_enable(self):
        """A table reconstructed with enable_aggregates=False disables routing."""
        enabled = _table(enable_aggregates=True)
        assert enabled.enable_aggregates is True
        disabled = _table(enable_aggregates=False)
        assert disabled.enable_aggregates is False

    def test_disabling_group_by_after_enable(self):
        """A table reconstructed with enable_group_by=False disables group-by routing."""
        enabled = _table(enable_group_by=True)
        assert enabled.enable_group_by is True
        disabled = _table(enable_group_by=False)
        assert disabled.enable_group_by is False
