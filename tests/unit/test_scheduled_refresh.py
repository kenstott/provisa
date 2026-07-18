# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2d1c90-7a44-4e18-9c05-3d6a0e4f1c84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1141: load-protected scheduled refresh — off-peak window + conjunction gate + read gate."""

from __future__ import annotations

import datetime
from zoneinfo import ZoneInfo

import pytest

from provisa.core.models import Column, Source, SourceType, Table
from provisa.federation.freshness_gate import FreshnessMode, evaluate_freshness
from provisa.federation.scheduled_refresh import (
    LoadProtectedWithoutGate,
    OffPeakWindow,
    parse_off_peak_window,
    refresh_gate_pre_probe,
    resolve_refresh_policy,
    should_scheduled_refresh,
    validate_refresh_policy,
    window_last_open_epoch,
    window_open,
)


def _ep(h: int, m: int = 0, *, tz: str = "UTC", day: int = 18) -> float:
    return datetime.datetime(2026, 7, day, h, m, tzinfo=ZoneInfo(tz)).timestamp()


def _src(sid: str = "pg", type_: SourceType = SourceType.postgresql, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


def _tbl(sid: str = "pg", **kw) -> Table:
    return Table(
        source_id=sid,
        domain_id="dom",
        table="t",
        schema="s",
        columns=[Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True)],
        **kw,
    )


# ---- off-peak window --------------------------------------------------------


def test_parse_window_hhmm():
    w = parse_off_peak_window("01:30-03:45", "UTC")
    assert (w.start_minute, w.end_minute, w.tz) == (90, 225, "UTC")


@pytest.mark.parametrize("bad", ["1-2", "25:00-03:00", "01:00", "01:60-02:00", "01:00-01:00"])
def test_parse_window_rejects_malformed(bad):
    with pytest.raises(ValueError):
        parse_off_peak_window(bad, "UTC")


def test_window_unknown_tz_raises():
    with pytest.raises(Exception):
        OffPeakWindow(60, 120, "Mars/Olympus")


def test_window_open_simple():
    w = parse_off_peak_window("01:00-03:00")
    assert window_open(w, _ep(1, 30)) is True
    assert window_open(w, _ep(1, 0)) is True  # half-open [start, end)
    assert window_open(w, _ep(3, 0)) is False
    assert window_open(w, _ep(4, 0)) is False


def test_window_open_wraps_midnight():
    w = parse_off_peak_window("22:00-02:00")
    assert window_open(w, _ep(23, 0)) is True
    assert window_open(w, _ep(1, 0)) is True
    assert window_open(w, _ep(12, 0)) is False


def test_window_respects_timezone():
    # 01:00 America/New_York == 05:00 UTC.
    w = parse_off_peak_window("01:00-03:00", "America/New_York")
    assert window_open(w, _ep(5, 0)) is True  # 05:00 UTC = 01:00 EDT
    assert window_open(w, _ep(1, 0)) is False  # 01:00 UTC = 21:00 EDT prev day


def test_window_last_open_epoch_identifies_instance():
    w = parse_off_peak_window("01:00-03:00")
    assert window_last_open_epoch(w, _ep(2, 0)) == _ep(1, 0)


# ---- the conjunction gate ---------------------------------------------------


def test_gate_none_configured_is_not_armed():
    assert (
        should_scheduled_refresh(
            now=_ep(4), window=None, last_refresh_at=None, cadence=None, probe_changed=None
        )
        is False
    )


def test_gate_window_only_once_per_open():
    w = parse_off_peak_window("01:00-03:00")
    # first entry into the window with no prior refresh → fire
    assert should_scheduled_refresh(
        now=_ep(1, 5), window=w, last_refresh_at=None, cadence=None, probe_changed=None
    )
    # already refreshed during this window instance → hold
    assert not should_scheduled_refresh(
        now=_ep(1, 40), window=w, last_refresh_at=_ep(1, 6), cadence=None, probe_changed=None
    )
    # closed window → never
    assert not should_scheduled_refresh(
        now=_ep(5), window=w, last_refresh_at=None, cadence=None, probe_changed=None
    )
    # next day's window instance → fire again (last refresh predates today's open)
    assert should_scheduled_refresh(
        now=_ep(1, 5, day=19), window=w, last_refresh_at=_ep(1, 6), cadence=None, probe_changed=None
    )


def test_gate_cadence_only():
    assert should_scheduled_refresh(
        now=_ep(4), window=None, last_refresh_at=_ep(1), cadence=3600, probe_changed=None
    )
    assert not should_scheduled_refresh(
        now=_ep(1, 30), window=None, last_refresh_at=_ep(1), cadence=3600, probe_changed=None
    )
    # never loaded → cadence vacuously elapsed
    assert should_scheduled_refresh(
        now=_ep(1), window=None, last_refresh_at=None, cadence=3600, probe_changed=None
    )


def test_gate_probe_only():
    assert should_scheduled_refresh(
        now=_ep(4), window=None, last_refresh_at=_ep(1), cadence=None, probe_changed=True
    )
    assert not should_scheduled_refresh(
        now=_ep(4), window=None, last_refresh_at=_ep(1), cadence=None, probe_changed=False
    )


def test_gate_window_and_cadence_conjunction():
    w = parse_off_peak_window("01:00-03:00")
    # cadence elapsed but window closed → no
    assert not should_scheduled_refresh(
        now=_ep(5), window=w, last_refresh_at=_ep(1, 0, day=17), cadence=3600, probe_changed=None
    )
    # in window and cadence elapsed → yes
    assert should_scheduled_refresh(
        now=_ep(2), window=w, last_refresh_at=_ep(1, 0, day=17), cadence=3600, probe_changed=None
    )


def test_gate_all_three():
    w = parse_off_peak_window("01:00-03:00")
    common = dict(now=_ep(2), window=w, last_refresh_at=_ep(1, 0, day=17), cadence=3600)
    assert should_scheduled_refresh(**common, probe_changed=True)
    assert not should_scheduled_refresh(**common, probe_changed=False)


def test_pre_probe_gate_excludes_probe():
    w = parse_off_peak_window("01:00-03:00")
    # off-window → pre-probe false so a caller never runs the probe
    assert not refresh_gate_pre_probe(now=_ep(5), window=w, last_refresh_at=None, cadence=None)
    assert refresh_gate_pre_probe(now=_ep(2), window=w, last_refresh_at=None, cadence=None)


# ---- SCHEDULED read gate (freshness_gate) -----------------------------------


def test_scheduled_read_never_loaded_is_not_fresh():
    # only the first read waits: nothing to serve yet.
    d = evaluate_freshness(
        FreshnessMode.SCHEDULED,
        now=_ep(4),
        last_refresh_at=None,
        ttl=60,
        stored_token=None,
        probe=None,
    )
    assert d.fresh is False


def test_scheduled_read_always_fresh_once_loaded_even_past_ttl():
    d = evaluate_freshness(
        FreshnessMode.SCHEDULED,
        now=_ep(23),
        last_refresh_at=_ep(1),  # 22h old, ttl 60s — still fresh to readers
        ttl=60,
        stored_token=None,
        probe=None,
    )
    assert d.fresh is True


def test_scheduled_read_never_calls_probe():
    called = False

    def probe():
        nonlocal called
        called = True
        return "x"

    evaluate_freshness(
        FreshnessMode.SCHEDULED,
        now=_ep(4),
        last_refresh_at=_ep(1),
        ttl=60,
        stored_token="y",
        probe=probe,
    )
    assert called is False  # readers never trigger a source probe (REQ-1141)


# ---- effective policy resolution + validation -------------------------------


def test_resolve_policy_source_window_and_cadence():
    s = _src(load_protected=True, off_peak_window="01:00-03:00", cache_ttl=300)
    p = resolve_refresh_policy(s, _tbl())
    assert p.load_protected and p.has_window and p.cadence == 300 and p.armed


def test_resolve_policy_table_overrides_source():
    s = _src(load_protected=True, off_peak_window="01:00-03:00")
    t = _tbl(load_protected=False, off_peak_window="04:00-05:00", off_peak_tz="America/New_York")
    p = resolve_refresh_policy(s, t)
    assert p.load_protected is False
    assert p.window == OffPeakWindow(240, 300, "America/New_York")


def test_resolve_policy_probe_capable_from_change_signal():
    s = _src(load_protected=True, change_signal="ttl_probe", cache_ttl=300)
    p = resolve_refresh_policy(s, _tbl())
    assert p.probe_capable is True


def test_validate_load_protected_without_gate_raises():
    s = _src(load_protected=True)  # no window, no ttl, ttl change_signal → no gate
    p = resolve_refresh_policy(s, _tbl())
    assert p.armed is False
    with pytest.raises(LoadProtectedWithoutGate):
        validate_refresh_policy(p, "pg", "t")


def test_validate_passes_with_one_gate():
    s = _src(load_protected=True, cache_ttl=300)
    validate_refresh_policy(resolve_refresh_policy(s, _tbl()), "pg", "t")  # no raise
