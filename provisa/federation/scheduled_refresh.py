# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8b1c60-5a44-4d92-9e07-3b6a0d4f1c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load-protected scheduled refresh — the out-of-band trigger for SCHEDULED sources (REQ-1141).

A load-protected source (``prefer_materialized`` + ``FreshnessMode.SCHEDULED``) is refreshed ONLY
here, never on the query path (freshness_gate.py returns fresh-to-readers for SCHEDULED). This
module is the pure decision: given wall-clock, the source's optional gates, and its last refresh,
decide whether the scheduler should PUBLISH a new snapshot now.

Three gates, each OPTIONAL and independently configured; the decision is the AND of ONLY the gates
that are set — an unset gate is vacuously true (ignored):

- WINDOW  — an off-peak/maintenance window (tenant-local). Refresh only while it is open.
- CADENCE — a minimum interval between snapshots (the source's ``cache_ttl``).
- PROBE   — the REQ-855 freshness probe. Refresh only when the source reports CHANGED; unchanged →
            reset the clock, publish nothing (one cheap probe, zero data pull).

The trigger clock depends on cadence:
- CADENCE set: re-evaluate each tick; a tick past the cadence (or a never-loaded entry) fires.
- NO cadence, WINDOW set: the window-open EDGE is the clock — refresh once per window instance
  (guarded by whether the last refresh predates this window's most-recent open boundary).
- PROBE only (no window, no cadence): fire on any tick the probe reports changed.

At least one gate MUST be configured for a scheduled refresh to be armed. None configured is not a
REQ-1141 mode (see the requirement's boundary): a reachable source is served live, an unreachable
one is a frozen one-shot snapshot — neither routes through here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from provisa.core.models import Source, Table

_MINUTES_PER_DAY = 24 * 60


@dataclass(frozen=True)
class OffPeakWindow:  # REQ-1141
    """A daily maintenance window, tenant-local. ``start_minute``/``end_minute`` are minutes-of-day
    in [0, 1440). A window wraps midnight when ``end_minute <= start_minute`` (e.g. 22:00–02:00).
    ``tz`` is an IANA zone name; the window is evaluated in that zone, so it tracks DST."""

    start_minute: int
    end_minute: int
    tz: str = "UTC"

    def __post_init__(self) -> None:
        for v in (self.start_minute, self.end_minute):
            if not 0 <= v < _MINUTES_PER_DAY:
                raise ValueError(f"off-peak window minute out of range [0,1440): {v}")
        if self.start_minute == self.end_minute:
            raise ValueError("off-peak window start and end must differ (a zero-length window)")
        ZoneInfo(self.tz)  # raises if the zone is unknown — fail loud at parse, no silent UTC


def parse_off_peak_window(spec: str, tz: str = "UTC") -> OffPeakWindow:
    """Parse an ``"HH:MM-HH:MM"`` window spec into an OffPeakWindow (REQ-1141).

    Both endpoints are 24h clock times in the given ``tz``. Malformed input raises — never a
    silent default window."""
    try:
        start_s, end_s = spec.split("-")
        start = _hhmm_to_minutes(start_s.strip())
        end = _hhmm_to_minutes(end_s.strip())
    except ValueError as e:
        raise ValueError(f"invalid off-peak window {spec!r}, expected 'HH:MM-HH:MM': {e}") from e
    return OffPeakWindow(start, end, tz)


def _hhmm_to_minutes(hhmm: str) -> int:
    h_s, m_s = hhmm.split(":")
    h, m = int(h_s), int(m_s)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError(f"time out of range: {hhmm!r}")
    return h * 60 + m


def _local_minute_of_day(window: OffPeakWindow, now: float) -> int:
    local = datetime.fromtimestamp(now, tz=ZoneInfo(window.tz))
    return local.hour * 60 + local.minute


def window_open(window: OffPeakWindow, now: float) -> bool:
    """True iff ``now`` falls inside ``window`` (half-open [start, end), tenant-local, REQ-1141)."""
    minute = _local_minute_of_day(window, now)
    if window.start_minute < window.end_minute:
        return window.start_minute <= minute < window.end_minute
    # wraps midnight: open from start through end-of-day, then start-of-day through end.
    return minute >= window.start_minute or minute < window.end_minute


def window_last_open_epoch(window: OffPeakWindow, now: float) -> float:
    """Epoch of the most recent window-OPEN boundary at or before ``now`` (REQ-1141).

    Only meaningful while the window is open; it identifies the current window instance so a
    no-cadence source refreshes exactly once per opening (the window-open edge is the clock)."""
    tz = ZoneInfo(window.tz)
    local = datetime.fromtimestamp(now, tz=tz)
    start_today = local.replace(
        hour=window.start_minute // 60,
        minute=window.start_minute % 60,
        second=0,
        microsecond=0,
    )
    # If today's start is still in the future relative to ``now`` (only possible for a wrapping
    # window whose current open instance began yesterday), step back one day.
    if start_today.timestamp() > now:
        start_today = start_today.astimezone(timezone.utc) - _one_day()
        start_today = start_today.astimezone(tz)
    return start_today.timestamp()


def _one_day():
    from datetime import timedelta

    return timedelta(days=1)


def refresh_gate_pre_probe(
    *,
    now: float,
    window: OffPeakWindow | None,
    last_refresh_at: float | None,
    cadence: float | None,
) -> bool:
    """The window + cadence gates only — the CHEAP pre-probe check (REQ-1141).

    A caller runs the (potentially networked) freshness probe ONLY after this returns True, so an
    off-window or too-soon source is never probed. Equivalent to ``should_scheduled_refresh`` with
    the probe gate omitted. Returns True when neither window nor cadence is configured (the probe,
    if any, then decides); ``should_scheduled_refresh`` still rejects the all-unset case."""
    if window is not None and not window_open(window, now):
        return False
    if cadence is not None:
        if last_refresh_at is not None and (now - last_refresh_at) < cadence:
            return False
    elif window is not None:
        if last_refresh_at is not None and last_refresh_at >= window_last_open_epoch(window, now):
            return False
    return True


def should_scheduled_refresh(
    *,
    now: float,
    window: OffPeakWindow | None,
    last_refresh_at: float | None,
    cadence: float | None,
    probe_changed: bool | None,
) -> bool:
    """Decide whether the scheduler should publish a new snapshot now (REQ-1141).

    ``window`` — the off-peak window, or None if unset. ``cadence`` — minimum seconds between
    snapshots (the source's cache_ttl), or None if unset. ``probe_changed`` — the freshness-probe
    verdict the caller already resolved (True=changed, False=unchanged), or None when no probe is
    configured. ``last_refresh_at`` — epoch of the last published snapshot, or None if never loaded.

    AND of only the configured gates; an unset gate is vacuously true. Returns False when NO gate is
    configured (not a scheduled source — see the module docstring boundary)."""
    if window is None and cadence is None and probe_changed is None:
        return False  # nothing armed — not a load-protected scheduled source

    if not refresh_gate_pre_probe(
        now=now, window=window, last_refresh_at=last_refresh_at, cadence=cadence
    ):
        return False

    if probe_changed is False:
        return False  # probe configured and source unchanged — publish nothing (reset clock only)

    return True


class LoadProtectedWithoutGate(ValueError):  # REQ-1141
    """A source marked load_protected but with no armed gate (no off-peak window, no cache_ttl
    cadence, no probing change_signal). Not a REQ-1141 mode — fail loud at registration."""

    def __init__(self, source_id: str, table_name: str | None = None) -> None:
        where = f"{source_id}.{table_name}" if table_name else source_id
        super().__init__(
            f"{where} is load_protected but arms no refresh gate — set an off_peak_window, a "
            "cache_ttl cadence, or a probing change_signal (probe/ttl_probe) (REQ-1141)"
        )


@dataclass(frozen=True)
class RefreshPolicy:  # REQ-1141
    """The effective, per-(source,table) resolution of the load-protection refresh policy.

    ``load_protected`` — the effective flag (table override → source). ``window`` — the effective
    off-peak window, or None. ``cadence`` — the effective cache_ttl in seconds, or None.
    ``probe_capable`` — the effective change_signal is a probing signal (probe/ttl_probe). The three
    gate flags are the ANDed inputs to ``should_scheduled_refresh``; ``armed`` is True iff at least
    one gate is set (required when load_protected)."""

    load_protected: bool
    window: OffPeakWindow | None
    cadence: int | None
    probe_capable: bool

    @property
    def has_window(self) -> bool:
        return self.window is not None

    @property
    def has_cadence(self) -> bool:
        return self.cadence is not None

    @property
    def armed(self) -> bool:
        return self.has_window or self.has_cadence or self.probe_capable


def _resolve_bool(table_flag: bool | None, source_flag: bool) -> bool:
    return source_flag if table_flag is None else table_flag


def resolve_refresh_policy(source: Source, table: Table) -> RefreshPolicy:
    """Resolve the effective load-protection refresh policy for one (source, table) (REQ-1141).

    Precedence is the standard table-override → source-inherit used across the config. The window
    is parsed from the effective ``off_peak_window`` spec in the effective ``off_peak_tz``; cadence
    is the effective ``cache_ttl``; probe capability is the effective ``change_signal`` resolved via
    the REQ-932 change-signal axis. Never validates the ≥1-gate rule here — that is
    ``validate_refresh_policy`` (called at registration), so read paths stay total."""
    from provisa.core.change_signal import resolve_effective

    load_protected = _resolve_bool(table.load_protected, source.load_protected)

    window_spec = table.off_peak_window or source.off_peak_window
    window: OffPeakWindow | None = None
    if window_spec is not None:
        tz = table.off_peak_tz or source.off_peak_tz
        window = parse_off_peak_window(window_spec, tz)

    cadence = table.cache_ttl if table.cache_ttl is not None else source.cache_ttl

    live = table.live
    sig = resolve_effective(
        table.change_signal,
        source.change_signal,
        live.strategy if live is not None else None,
    )
    probe_capable = sig in ("probe", "ttl_probe")

    return RefreshPolicy(
        load_protected=load_protected, window=window, cadence=cadence, probe_capable=probe_capable
    )


def validate_refresh_policy(policy: RefreshPolicy, source_id: str, table_name: str) -> None:
    """Enforce REQ-1141's ≥1-gate rule: a load_protected policy MUST arm at least one gate."""
    if policy.load_protected and not policy.armed:
        raise LoadProtectedWithoutGate(source_id, table_name)
