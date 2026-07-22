# Copyright (c) 2026 Kenneth Stott
# Canary: fc852496-56de-41dc-ae1d-56c554a1d650
"""REQ-1169: the MV snapshot-schedule fields (mv_calendar / mv_grain / mv_allowed_lateness /
mv_expected_events / mv_business_day_grain) flow model→registry — a TableInput carrying a
calendar+grain binding maps into the core Table model without loss, so the periodic-snapshot
feature is reachable through the admin surface without code changes."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.admin._live_mappers import table_model_from_input


def _table_input(**overrides):
    base = dict(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="t",
        description=None,
        watermark_column=None,
        change_signal=None,
        probe_query=None,
        probe_type=None,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        view_sql=None,
        materialize=True,
        mv_refresh_interval=300,
        mv_debounce_quiet=0.0,
        mv_debounce_max_delay=5.0,
        mv_consistency="shared",
        mv_preprocess=None,
        mv_bitemporal_mode=None,
        mv_bitemporal_key=[],
        mv_persist="replace",
        mv_primary_key=[],
        mv_incremental=False,
        mv_calendar=None,
        mv_grain=None,
        mv_allowed_lateness=0.0,
        mv_expected_events=None,
        mv_business_day_grain=False,
        data_product=False,
        enable_aggregates=False,
        enable_group_by=False,
        live=None,
        unique_constraints=[],
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_snapshot_schedule_fields_survive_model_mapping():
    inp = _table_input(
        mv_calendar="fiscal-2026",
        mv_grain="RRULE:FREQ=MONTHLY;BYDAY=3WE",
        mv_allowed_lateness=3600.0,
        mv_expected_events=["orders", "returns"],
        mv_business_day_grain=True,
    )
    model = table_model_from_input(inp, columns=[], presets=[], alias="t")
    assert model.mv_calendar == "fiscal-2026"
    assert model.mv_grain == "RRULE:FREQ=MONTHLY;BYDAY=3WE"
    assert model.mv_allowed_lateness == 3600.0
    assert model.mv_expected_events == ["orders", "returns"]
    assert model.mv_business_day_grain is True


def test_snapshot_schedule_defaults_when_unbound():
    model = table_model_from_input(_table_input(), columns=[], presets=[], alias="t")
    assert model.mv_calendar is None
    assert model.mv_grain is None
    assert model.mv_allowed_lateness == 0.0
    assert model.mv_expected_events is None
    assert model.mv_business_day_grain is False
