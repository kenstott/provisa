# Copyright (c) 2026 Kenneth Stott
# Canary: 8d6a163c-b950-4017-8306-721c318f4aa7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Standard crontab schedules for APScheduler.

APScheduler's ``CronTrigger.from_crontab`` numbers the weekday field 0 = Monday; crontab numbers it
0 or 7 = Sunday, 1 = Monday ... 6 = Saturday. Every Provisa cron expression is crontab, so every
trigger is built here, the one place that translates the weekday field.
"""

from __future__ import annotations

from datetime import tzinfo

from apscheduler.triggers.cron import CronTrigger

_NAMES = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}


def _day(token: str, expr: str) -> int:
    text = token.strip().lower()
    if text in _NAMES:
        return _NAMES[text]
    if text.isdigit() and 0 <= int(text) <= 7:
        return int(text)
    raise ValueError(f"cron {expr!r}: invalid weekday {token!r}")


def _weekday_field(field: str, expr: str) -> str:
    """The crontab weekday ``field`` in APScheduler's numbering (0 = Monday)."""
    if field == "*":
        return "*"
    days: set[int] = set()
    for part in field.split(","):
        base, slash, step_text = part.partition("/")
        step = 1
        if slash:
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError(f"cron {expr!r}: invalid weekday step {part!r}")
            step = int(step_text)
        if base == "*":
            first, last = 0, 6
        elif "-" in base:
            low, _, high = base.partition("-")
            first, last = _day(low, expr), _day(high, expr)
            if last == 0 and first > 0:
                last = 7  # FRI-SUN: Sunday closes the range
            if first > last:
                raise ValueError(f"cron {expr!r}: weekday range {base!r} is reversed")
        else:
            first = _day(base, expr)
            last = 7 if slash else first
        days.update(range(first, last + 1, step))
    return ",".join(str(n) for n in sorted({(d - 1) % 7 for d in days}))


def crontab_to_apscheduler(expr: str) -> str:
    """``expr``, a five-field crontab, with its weekday field renumbered for APScheduler."""
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"Wrong number of fields; got {len(fields)}, expected 5")
    fields[4] = _weekday_field(fields[4], expr)
    return " ".join(fields)


def cron_trigger(expr: str, timezone: tzinfo | None = None) -> CronTrigger:
    """The trigger for crontab ``expr``; ValueError when it is not a five-field crontab."""
    translated = crontab_to_apscheduler(expr)
    if timezone is None:
        return CronTrigger.from_crontab(translated)
    return CronTrigger.from_crontab(translated, timezone=timezone)
