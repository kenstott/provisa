# Copyright (c) 2026 Kenneth Stott
# Canary: d3ec0d46-4397-4b9f-b200-b48cac65849a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-924/925: a watermark must name one of the table's own columns and be monotonic."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.config_loader import _validate_watermark_columns


def _col(name, data_type: str | None = "timestamp"):
    return SimpleNamespace(name=name, data_type=data_type)


def _table(*, watermark_column=None, live=None, columns=None):
    return SimpleNamespace(
        table_name="events",
        source_id="s1",
        watermark_column=watermark_column,
        live=live,
        columns=columns
        if columns is not None
        else [_col("id", "bigint"), _col("updated_at", "timestamp")],
    )


def _config(*tables):
    return SimpleNamespace(tables=list(tables))


class TestMonotonicType:  # REQ-925
    @pytest.mark.parametrize("dt", ["timestamp", "date", "time", "smallint", "integer", "bigint"])
    def test_monotonic_types_accepted(self, dt):
        t = _table(watermark_column="wm", columns=[_col("wm", dt)])
        _validate_watermark_columns(_config(t))  # no raise

    @pytest.mark.parametrize("dt", ["text", "float", "double", "boolean", "uuid", "numeric"])
    def test_non_monotonic_types_rejected(self, dt):
        t = _table(watermark_column="wm", columns=[_col("wm", dt)])
        with pytest.raises(ValueError, match="not monotonic"):
            _validate_watermark_columns(_config(t))

    def test_native_alias_maps_to_ir_before_check(self):
        # timestamptz → timestamp (accepted); varchar → text (rejected)
        ok = _table(watermark_column="wm", columns=[_col("wm", "timestamptz")])
        _validate_watermark_columns(_config(ok))
        bad = _table(watermark_column="wm", columns=[_col("wm", "varchar")])
        with pytest.raises(ValueError, match="not monotonic"):
            _validate_watermark_columns(_config(bad))

    def test_unmappable_type_rejected(self):
        t = _table(watermark_column="wm", columns=[_col("wm", "totally_unknown_type")])
        with pytest.raises(ValueError, match="not monotonic"):
            _validate_watermark_columns(_config(t))


class TestExistingColumn:  # REQ-924
    def test_watermark_must_be_a_table_column(self):
        t = _table(watermark_column="does_not_exist")
        with pytest.raises(ValueError, match="not a column of the table"):
            _validate_watermark_columns(_config(t))

    def test_existing_monotonic_column_passes(self):
        t = _table(watermark_column="updated_at")
        _validate_watermark_columns(_config(t))  # no raise


class TestDeferredAndAbsent:
    def test_no_watermark_is_a_noop(self):
        _validate_watermark_columns(_config(_table(watermark_column=None)))

    def test_empty_columns_deferred_to_introspection(self):
        # columns not yet reflected → can't validate existence/type; re-checked at selection
        _validate_watermark_columns(_config(_table(watermark_column="wm", columns=[])))

    def test_unknown_data_type_deferred(self):
        # column exists but type not yet resolved → existence ok, type check deferred
        t = _table(watermark_column="wm", columns=[_col("wm", None)])
        _validate_watermark_columns(_config(t))


class TestLegacyLiveWatermark:
    def test_legacy_live_watermark_validated(self):
        live = SimpleNamespace(watermark_column="updated_at")
        _validate_watermark_columns(_config(_table(watermark_column=None, live=live)))

    def test_legacy_live_non_monotonic_rejected(self):
        live = SimpleNamespace(watermark_column="label")
        t = _table(watermark_column=None, live=live, columns=[_col("label", "text")])
        with pytest.raises(ValueError, match="not monotonic"):
            _validate_watermark_columns(_config(t))
