# Copyright (c) 2026 Kenneth Stott
# Canary: bdb2341e-2b46-4da1-bc20-532eefa78d80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Named, shared, versioned CALENDARS — the temporal-window boundary source (REQ-962).

A temporal MV declares ``(calendar, grain)`` and the calendar deterministically yields a half-open
``[start, end)`` window, a calendar-addressable ``window_id`` (e.g. ``2026-Q1``), and the next
boundary — driving the as-of peg (``window.end``), the claim deadline (``boundary + lateness``), the
scheduler wake, and forced-regen addressing. Boundaries are NEVER a fixed offset:

- Timezone/DST-aware — a day is 23h or 25h across a DST transition, computed by converting the
  calendar's LOCAL civil midnight to UTC through ``zoneinfo`` (never ``start + 24h``).
- Base systems — Gregorian, fiscal (an anchor month), and retail 4-4-5 (13-week quarters).
- Grains NEST (day ⊂ week ⊂ month ⊂ quarter ⊂ year), so a coarse roll-up's expected set is its
  constituent sealed sub-windows and a shared calendar keeps the nesting consistent across the DAG.

The calendar also GATES WINDOW EXISTENCE: a BUSINESS-day grain has no window on a holiday/weekend
(``window_for`` returns ``None`` → the MV deterministically does not generate and raises no deadline
alarm); a CALENDAR-day grain always opens a window. Holiday/business-day data is versioned and
immutable-per-window for replay fidelity. Unknown calendar or grain fails LOUD — never a silent
default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

from dateutil.rrule import rrule, rrulestr, rruleset

_QUARTER_MONTHS = 3
_YEAR_MONTHS = 12
_RETAIL_QUARTER_WEEKS = 13  # 4-4-5 = 13 weeks per quarter
_RETAIL_YEAR_WEEKS = 52
_DAYS_PER_WEEK = 7
# 4-4-5: each quarter is a 4-week, 4-week, 5-week period triple → 12 periods, cumulative week offsets.
_RETAIL_PERIOD_WEEKS = (4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5)


class Grain(str, Enum):
    """The temporal grains an MV may declare. Grains NEST coarser left→right (REQ-962)."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class BaseSystem(str, Enum):
    GREGORIAN = "gregorian"
    FISCAL = "fiscal"
    RETAIL_445 = "retail_445"


def parse_grain(grain: str | Grain) -> Grain:
    """Coerce a declared grain to :class:`Grain`, failing LOUD on an unknown value (REQ-962)."""
    if isinstance(grain, Grain):
        return grain
    try:
        return Grain(grain)
    except ValueError as exc:
        raise ValueError(
            f"unknown grain {grain!r}; expected one of {[g.value for g in Grain]}"
        ) from exc


@dataclass(frozen=True)
class Window:
    """A sealed, calendar-addressable processing window: half-open ``[start, end)`` in UTC plus its
    stable ``window_id`` (the as-of peg is ``end``, the forced-regen address is ``window_id``)."""

    start: datetime
    end: datetime
    window_id: str


@dataclass(frozen=True)
class Calendar:
    """A registered, versioned calendar definition (REQ-962).

    ``version`` is the immutable-per-window stamp: the holiday/business-day set is captured at a
    version so a replay reproduces the same window existence. ``fiscal_anchor`` is the (month, day)
    the fiscal/retail year starts. ``retail_anchor`` is the explicit start DATE of a reference retail
    4-4-5 year (must fall on ``week_start``). ``holidays`` + ``weekend`` gate business-day existence.
    """

    name: str
    version: str
    base_system: BaseSystem = BaseSystem.GREGORIAN
    tz: str = "UTC"
    fiscal_anchor: tuple[int, int] = (1, 1)
    retail_anchor: date | None = None
    week_start: int = 0  # 0 = Monday (date.weekday convention)
    holidays: frozenset[date] = field(default_factory=frozenset)
    weekend: frozenset[int] = frozenset({5, 6})  # Sat, Sun (date.weekday convention)

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)

    def is_business_day(self, d: date) -> bool:
        return d.weekday() not in self.weekend and d not in self.holidays


# -- month arithmetic (calendar-correct, never day-count) ----------------------
def _add_months(d: date, months: int) -> date:
    total = (d.year * _YEAR_MONTHS + (d.month - 1)) + months
    return date(total // _YEAR_MONTHS, total % _YEAR_MONTHS + 1, 1)


def _utc(local_naive: datetime, cal: Calendar) -> datetime:
    """A LOCAL civil wall-clock instant in the calendar's zone → its UTC instant. DST-aware: the same
    civil midnight maps to a different UTC offset across a transition, so consecutive daily boundaries
    span 23h/25h — never a fixed 24h (REQ-962)."""
    return local_naive.replace(tzinfo=cal.zone).astimezone(timezone.utc)


def _window(cal: Calendar, start_d: date, end_d: date, window_id: str) -> Window:
    return Window(
        start=_utc(datetime(start_d.year, start_d.month, start_d.day), cal),
        end=_utc(datetime(end_d.year, end_d.month, end_d.day), cal),
        window_id=window_id,
    )


def _local_date(cal: Calendar, instant: datetime) -> date:
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(cal.zone).date()


# -- fiscal year ---------------------------------------------------------------
def _fiscal_year_start(cal: Calendar, d: date) -> date:
    am, ad = cal.fiscal_anchor
    candidate = date(d.year, am, ad)
    return candidate if d >= candidate else date(d.year - 1, am, ad)


def _fiscal_label(cal: Calendar, fy_start: date) -> int:
    """The fiscal-year label: the calendar year the fiscal year ENDS in (US convention) when the
    anchor is mid-year; the start year when the anchor is Jan (fiscal == calendar)."""
    return fy_start.year if cal.fiscal_anchor[0] == 1 else fy_start.year + 1


# -- per-grain gregorian/fiscal boundaries -------------------------------------
def _daily(cal: Calendar, d: date, *, business_day: bool) -> Window | None:
    if business_day and not cal.is_business_day(d):
        return None  # holiday/weekend on a business-day grain → NO window (REQ-962 existence gate)
    return _window(cal, d, d + timedelta(days=1), d.isoformat())


def _weekly(cal: Calendar, d: date) -> Window:
    start = d - timedelta(days=(d.weekday() - cal.week_start) % _DAYS_PER_WEEK)
    end = start + timedelta(days=_DAYS_PER_WEEK)
    iso_year, iso_week, _ = start.isocalendar()
    return _window(cal, start, end, f"{iso_year}-W{iso_week:02d}")


def _monthly(cal: Calendar, d: date) -> Window:
    start = date(d.year, d.month, 1)
    return _window(cal, start, _add_months(start, 1), f"{d.year}-{d.month:02d}")


def _quarterly(cal: Calendar, d: date) -> Window:
    if cal.base_system is BaseSystem.FISCAL:
        fy = _fiscal_year_start(cal, d)
        idx = ((d.year - fy.year) * _YEAR_MONTHS + (d.month - fy.month)) // _QUARTER_MONTHS
        start = _add_months(fy, idx * _QUARTER_MONTHS)
        label = _fiscal_label(cal, fy)
        return _window(cal, start, _add_months(start, _QUARTER_MONTHS), f"{label}-Q{idx + 1}")
    idx = (d.month - 1) // _QUARTER_MONTHS
    start = date(d.year, idx * _QUARTER_MONTHS + 1, 1)
    return _window(cal, start, _add_months(start, _QUARTER_MONTHS), f"{d.year}-Q{idx + 1}")


def _annual(cal: Calendar, d: date) -> Window:
    if cal.base_system is BaseSystem.FISCAL:
        fy = _fiscal_year_start(cal, d)
        return _window(cal, fy, _add_months(fy, _YEAR_MONTHS), str(_fiscal_label(cal, fy)))
    start = date(d.year, 1, 1)
    return _window(cal, start, date(d.year + 1, 1, 1), str(d.year))


# -- retail 4-4-5 --------------------------------------------------------------
def _retail_week_index(cal: Calendar, d: date) -> int:
    if cal.retail_anchor is None:
        raise ValueError(f"calendar {cal.name!r}: retail_445 base_system requires a retail_anchor")
    delta = (d - cal.retail_anchor).days
    if delta < 0:
        raise ValueError(
            f"calendar {cal.name!r}: {d} precedes retail_anchor {cal.retail_anchor} "
            f"(register an anchor at or before the queried date)"
        )
    return delta // _DAYS_PER_WEEK


def _retail_window(cal: Calendar, grain: Grain, d: date) -> Window:
    week = _retail_week_index(cal, d)  # fails loud when retail_anchor is unset
    anchor = cal.retail_anchor
    assert anchor is not None  # guaranteed by _retail_week_index
    label = anchor.year
    if grain is Grain.WEEKLY:
        start = anchor + timedelta(weeks=week)
        return _window(cal, start, start + timedelta(weeks=1), f"{label}-W{week + 1:02d}")
    if grain is Grain.QUARTERLY:
        q = week // _RETAIL_QUARTER_WEEKS
        start = anchor + timedelta(weeks=q * _RETAIL_QUARTER_WEEKS)
        end = start + timedelta(weeks=_RETAIL_QUARTER_WEEKS)
        return _window(cal, start, end, f"{label}-Q{q + 1}")
    if grain is Grain.ANNUAL:
        start = anchor + timedelta(weeks=(week // _RETAIL_YEAR_WEEKS) * _RETAIL_YEAR_WEEKS)
        return _window(cal, start, start + timedelta(weeks=_RETAIL_YEAR_WEEKS), str(label))
    if grain is Grain.MONTHLY:  # retail PERIOD (4-4-5)
        cum = 0
        for period, span in enumerate(_RETAIL_PERIOD_WEEKS):
            if week < cum + span:
                start = anchor + timedelta(weeks=cum)
                return _window(cal, start, start + timedelta(weeks=span), f"{label}-P{period + 1}")
            cum += span
        raise ValueError(f"calendar {cal.name!r}: week {week} is beyond the 52-week retail year")
    raise ValueError(f"calendar {cal.name!r}: retail_445 has no daily grain (weeks are the atom)")


_GREGORIAN_GRAIN = {
    Grain.WEEKLY: _weekly,
    Grain.MONTHLY: _monthly,
    Grain.QUARTERLY: _quarterly,
    Grain.ANNUAL: _annual,
}


# -- nth-weekday-of-month recurrence (REQ-1168) --------------------------------
# The fixed NESTING grains (day⊂week⊂month⊂…) cannot express "the 3rd Wednesday of each month" or
# "the last Friday": those are ANCHORED recurrences, not a period roll-up. A recurrence yields the
# same calendar-addressable ``[start, end)`` tiling as a grain — consecutive occurrences tile the
# timeline — so it drops into ``window_for``/``next_boundary``/``PeriodicCalendar`` unchanged.
_LAST = -1  # sentinel n: the LAST matching weekday of the month
_SCAN_MONTHS = 24  # bound the occurrence scan (a rule always recurs within ~2 months; guards runaway)
_WD_ABBR = ("MO", "TU", "WE", "TH", "FR", "SA", "SU")  # date.weekday() index → label


@dataclass(frozen=True)
class NthWeekday:
    """An anchored monthly recurrence (REQ-1168): the ``n``-th ``weekday`` of each month.

    ``weekday`` is the ``date.weekday()`` index (0=Mon … 6=Sun). ``n`` is 1–5 (the n-th occurrence)
    or ``-1`` (the LAST occurrence). A month lacking the n-th occurrence (e.g. a 5th Wednesday) has
    NO occurrence — consecutive occurrences still tile the timeline, so such a tile simply spans more
    than one month. Fails LOUD on an out-of-range weekday/n."""

    weekday: int
    n: int = 1

    def __post_init__(self) -> None:
        if not 0 <= self.weekday <= 6:
            raise ValueError(f"NthWeekday.weekday out of range [0,6]: {self.weekday}")
        if not (1 <= self.n <= 5 or self.n == _LAST):
            raise ValueError(f"NthWeekday.n must be 1..5 or -1 (last): {self.n}")

    @property
    def label(self) -> str:
        ord_ = "L" if self.n == _LAST else str(self.n)
        return f"{ord_}{_WD_ABBR[self.weekday]}"


def _occurrence_in_month(rule: NthWeekday, year: int, month: int) -> date | None:
    """The date of ``rule`` in ``(year, month)``, or None when that month has no such occurrence
    (e.g. a 5th weekday that does not exist). ``n == -1`` is the last matching weekday."""
    first = date(year, month, 1)
    first_offset = (rule.weekday - first.weekday()) % _DAYS_PER_WEEK
    if rule.n == _LAST:
        last_day = _add_months(first, 1) - timedelta(days=1)
        last_offset = (last_day.weekday() - rule.weekday) % _DAYS_PER_WEEK
        return last_day - timedelta(days=last_offset)
    day = 1 + first_offset + (rule.n - 1) * _DAYS_PER_WEEK
    candidate = first + timedelta(days=day - 1)
    return candidate if candidate.month == month else None  # e.g. no 5th weekday this month


def _occurrence_on_or_before(rule: NthWeekday, d: date) -> date:
    probe = date(d.year, d.month, 1)
    for _ in range(_SCAN_MONTHS):
        occ = _occurrence_in_month(rule, probe.year, probe.month)
        if occ is not None and occ <= d:
            return occ
        probe = _add_months(probe, -1)
    raise ValueError(f"no {rule.label} occurrence within {_SCAN_MONTHS} months on/before {d}")


def _occurrence_strictly_after(rule: NthWeekday, d: date) -> date:
    probe = date(d.year, d.month, 1)
    for _ in range(_SCAN_MONTHS):
        occ = _occurrence_in_month(rule, probe.year, probe.month)
        if occ is not None and occ > d:
            return occ
        probe = _add_months(probe, 1)
    raise ValueError(f"no {rule.label} occurrence within {_SCAN_MONTHS} months after {d}")


def _recurrence_window(cal: Calendar, rule: NthWeekday, d: date) -> Window:
    """The half-open ``[occurrence, next_occurrence)`` tile containing local date ``d`` (REQ-1168),
    addressed by its opening occurrence (``window_id`` = ``2026-03-18-3WE``)."""
    start = _occurrence_on_or_before(rule, d)
    end = _occurrence_strictly_after(rule, start)
    return _window(cal, start, end, f"{start.isoformat()}-{rule.label}")


_WD_INDEX = {abbr: i for i, abbr in enumerate(_WD_ABBR)}


# -- RFC 5545 RRULE recurrence (REQ-1169) --------------------------------------
# The Outlook / iCalendar recurrence model, modeled on the ``rrule`` builder the UI exposes: an
# arbitrary ``FREQ``/``INTERVAL``/``BYDAY``/``BYMONTHDAY``/``BYSETPOS``/``BYMONTH`` rule. The fixed
# NESTING grains and the ``NthWeekday`` shorthand cover the common cases; an RRULE covers everything
# Outlook can express (every-N-weeks, last weekday of a quarter, a specific month-day, …). Like
# ``NthWeekday`` it TILES the timeline — consecutive occurrences yield the same calendar-addressable
# ``[start, end)`` tiling, so it drops into ``window_for``/``next_boundary`` unchanged.
#
# INTERVAL phase (e.g. "every 2 weeks") is anchored to a FIXED civil epoch so the tiling is
# deterministic and independent of when a query is made — RFC 5545 anchors recurrence phase to
# DTSTART, and a grain has no DTSTART of its own.
_RRULE_EPOCH = datetime(2000, 1, 1)  # phase anchor for INTERVAL; precedes any real snapshot data
_RRULE_FREQ_CODE = {"YEARLY": "Y", "MONTHLY": "M", "WEEKLY": "W", "DAILY": "D"}


@dataclass(frozen=True)
class RRuleRecurrence:
    """An RFC 5545 recurrence grain (REQ-1169): the general Outlook/iCalendar recurrence form. Holds
    the NORMALIZED rule string (no ``RRULE:`` prefix, no ``COUNT``/``UNTIL`` — a grain tiles forever).
    Validated at construction; fails LOUD on an unparseable or bounded rule."""

    rule: str

    def __post_init__(self) -> None:
        try:
            self._rrule()  # parse eagerly so an invalid rule fails at declaration, not at boundary time
        except (ValueError, TypeError) as exc:
            raise ValueError(f"invalid RRULE grain {self.rule!r}: {exc}") from exc

    def _rrule(self) -> rrule | rruleset:
        return rrulestr(self.rule, dtstart=_RRULE_EPOCH)

    @property
    def label(self) -> str:
        """A short, stable token for the ``window_id`` suffix, derived from the rule (e.g. a monthly
        3rd-Wednesday rule → ``M3WE``; every-2-weeks-on-Mon → ``W2MO``)."""
        parts = dict(kv.split("=", 1) for kv in self.rule.split(";"))
        tok = _RRULE_FREQ_CODE.get(parts.get("FREQ", ""), "?")
        interval = parts.get("INTERVAL")
        if interval and interval != "1":
            tok += interval
        for key, prefix in (("BYMONTH", "MO"), ("BYMONTHDAY", "D"), ("BYDAY", ""), ("BYSETPOS", "P")):
            val = parts.get(key)
            if val:
                tok += prefix + val.replace(",", "").replace("-", "L")
        return tok


def _normalize_rrule(s: str) -> str:
    """Strip an optional ``RRULE:`` prefix and uppercase keys; fail LOUD on a bounded rule (a grain
    tiles forever, so ``COUNT``/``UNTIL`` are rejected rather than silently dropped)."""
    body = s.strip()
    if body.upper().startswith("RRULE:"):
        body = body[len("RRULE:") :]
    pairs = [kv.strip() for kv in body.split(";") if kv.strip()]
    upper = {kv.split("=", 1)[0].upper() for kv in pairs}
    if upper & {"COUNT", "UNTIL"}:
        raise ValueError(f"RRULE grain {s!r} must be unbounded (remove COUNT/UNTIL) — a grain tiles forever")
    return ";".join(f"{kv.split('=', 1)[0].upper()}={kv.split('=', 1)[1]}" for kv in pairs)


def _rrule_occurrence_on_or_before(rec: RRuleRecurrence, d: date) -> date:
    occ = rec._rrule().before(datetime(d.year, d.month, d.day), inc=True)
    if occ is None:
        raise ValueError(f"RRULE grain {rec.rule!r}: no occurrence on/before {d} (after epoch {_RRULE_EPOCH:%Y-%m-%d})")
    return occ.date()


def _rrule_occurrence_after(rec: RRuleRecurrence, d: date) -> date:
    occ = rec._rrule().after(datetime(d.year, d.month, d.day), inc=False)
    if occ is None:
        raise ValueError(f"RRULE grain {rec.rule!r}: no occurrence after {d}")
    return occ.date()


def _rrule_window(cal: Calendar, rec: RRuleRecurrence, d: date) -> Window:
    """The half-open ``[occurrence, next_occurrence)`` tile containing local date ``d`` (REQ-1169),
    addressed by its opening occurrence (``window_id`` = ``2026-03-18-M3WE``)."""
    start = _rrule_occurrence_on_or_before(rec, d)
    end = _rrule_occurrence_after(rec, start)
    return _window(cal, start, end, f"{start.isoformat()}-{rec.label}")


def parse_grain_spec(spec: str | Grain | NthWeekday | RRuleRecurrence) -> Grain | NthWeekday | RRuleRecurrence:
    """Parse a declared calendar grain into a nesting :class:`Grain`, an anchored :class:`NthWeekday`
    shorthand, or a full :class:`RRuleRecurrence` (REQ-962/1168/1169). Resolution order:

    - a full RFC 5545 rule (``RRULE:FREQ=…`` or a bare ``FREQ=…``) → :class:`RRuleRecurrence`;
    - the ``<ordinal><weekday>`` shorthand — ordinal 1–5 or ``L`` (last), weekday MO/TU/…/SU (e.g.
      ``3WE`` = 3rd Wednesday, ``LFR`` = last Friday) → :class:`NthWeekday`;
    - anything else → a nesting grain, or fail LOUD on an unknown value."""
    if isinstance(spec, (Grain, NthWeekday, RRuleRecurrence)):
        return spec
    s = spec.strip()
    if s.upper().startswith("RRULE:") or s.upper().startswith("FREQ="):
        return RRuleRecurrence(_normalize_rrule(s))
    ordinal, wd = s[:-2].upper(), s[-2:].upper()
    if wd in _WD_INDEX and (ordinal == "L" or (ordinal.isdigit() and 1 <= int(ordinal) <= 5)):
        return NthWeekday(weekday=_WD_INDEX[wd], n=_LAST if ordinal == "L" else int(ordinal))
    return parse_grain(s)  # a nesting grain, or fail loud on an unknown value


def window_for(
    cal: Calendar,
    grain: str | Grain | NthWeekday | RRuleRecurrence,
    instant: datetime,
    *,
    business_day: bool = False,
) -> Window | None:
    """The ``[start, end)`` window (+ ``window_id``) covering ``instant`` for ``(cal, grain)``, or
    ``None`` when the calendar gates the window out of existence (a business-day grain on a
    holiday/weekend). Fails LOUD on an unknown grain (REQ-962).

    ``grain`` is a nesting :class:`Grain`, an anchored :class:`NthWeekday` shorthand (REQ-1168), or a
    full :class:`RRuleRecurrence` (REQ-1169) — each recurrence yields the same ``[start, end)`` tiling,
    addressed by its opening occurrence. ``business_day`` picks the existence rule: True = business-day
    grain (no window on a non-business day); False = calendar-day grain (always a window). ``instant``
    is resolved to the calendar's LOCAL date before boundary derivation, so DST and zone offset are
    honored."""
    d = _local_date(cal, instant)
    spec = parse_grain_spec(grain)  # a nesting grain, an nth-weekday shorthand, or a full RRULE
    if isinstance(spec, NthWeekday):
        return _recurrence_window(cal, spec, d)  # an occurrence is a specific day; no grain gating
    if isinstance(spec, RRuleRecurrence):
        return _rrule_window(cal, spec, d)  # a recurrence occurrence is a specific day; no gating
    if cal.base_system is BaseSystem.RETAIL_445:
        return _retail_window(cal, spec, d)
    if spec is Grain.DAILY:
        return _daily(cal, d, business_day=business_day)
    return _GREGORIAN_GRAIN[spec](cal, d)


def next_boundary(
    cal: Calendar,
    grain: str | Grain | NthWeekday | RRuleRecurrence,
    instant: datetime,
    *,
    business_day: bool = False,
) -> datetime:
    """The next boundary at/after ``instant`` — the scheduler wake and the close of the current
    window. When the current instant's window is gated out (holiday), advance day-by-day to the next
    existing window's end (a business-day grain skips non-business days). Fails loud on unknown grain."""
    win = window_for(cal, grain, instant, business_day=business_day)  # window_for parses the grain
    if win is not None:
        return win.end
    probe = instant
    for _ in range(_YEAR_MONTHS * 31):  # bounded scan; a year of days is an ample backstop
        probe = probe + timedelta(days=1)
        win = window_for(cal, grain, probe, business_day=business_day)
        if win is not None:
            return win.end
    raise ValueError(f"calendar {cal.name!r}: no window found within a year of {instant}")


class CalendarRegistry:
    """The named, shared calendar registry (REQ-962). ``get`` fails LOUD on an unknown name — never
    a silent default calendar."""

    def __init__(self) -> None:
        self._by_name: dict[str, Calendar] = {}

    def register(self, cal: Calendar) -> None:
        self._by_name[cal.name] = cal

    def get(self, name: str) -> Calendar:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise ValueError(
                f"unknown calendar {name!r}; registered: {sorted(self._by_name)}"
            ) from exc

    def window_for(
        self,
        name: str,
        grain: str | Grain | NthWeekday | RRuleRecurrence,
        instant: datetime,
        *,
        business_day: bool = False,
    ) -> Window | None:
        return window_for(self.get(name), grain, instant, business_day=business_day)
