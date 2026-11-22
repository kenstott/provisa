# Copyright (c) 2026 Kenneth Stott
# Canary: 7d5a7325-00f3-4bb6-87aa-5a5b2e116f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of dates, times and intervals (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms, pg_forms_types
from provisa.transpiler.rows.base import (
    _CLICKHOUSE,
    _DUCKDB,
    _EVERY,
    _NO_TIME,
    MAPPED,
    Forms,
    Macro,
    Pg,
    _macro,
    _pg,
    strict,
)

_BIGQUERY_HAS_IT: Forms = {frozenset({"bigquery"}): MAPPED}
_MAKE_INTERVAL_PARAMS = "years, months, weeks, days, hours, mins, secs"
_DATES: tuple[Macro, ...] = (
    Macro("statement_timestamp", (), {_EVERY: Pg("CURRENT_TIMESTAMP")}),
    _macro("clock_timestamp", "", pg_forms.CLOCK_TIMESTAMP),
    _macro("timeofday", "", pg_forms.TIMEOFDAY),
    *(
        _macro("justify_" + of, "value", strict("value", "INTERVAL", form), own=_BIGQUERY_HAS_IT)
        for of, form in (
            ("hours", pg_forms.JUSTIFY_HOURS),
            ("days", pg_forms.JUSTIFY_DAYS),
            ("interval", pg_forms.JUSTIFY_INTERVAL),
        )
    ),
    _macro(
        "make_interval_fields",
        _MAKE_INTERVAL_PARAMS,
        strict(_MAKE_INTERVAL_PARAMS, "INTERVAL", pg_forms.MAKE_INTERVAL),
    ),
    _pg("overlaps_timestamp", "s1, e1, s2, e2", pg_forms.OVERLAPS),
    _macro(
        "datetime_pl",
        "d, t",
        strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL),
        {_EVERY: Pg(strict("d, t", "TIMESTAMP", pg_forms.DATE_AND_TIME))},
    ),
    _macro(
        "timedate_pl",
        "t, d",
        strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL),
        {_EVERY: Pg(strict("d, t", "TIMESTAMP", pg_forms.DATE_AND_TIME))},
    ),
    _macro(
        "isfinite",
        "value",
        "isfinite(value)",
        {_EVERY: Pg("CASE WHEN value IS NOT NULL THEN TRUE END")},
    ),
    _pg("timestamp", "value", "CAST(value AS TIMESTAMP)"),
    _pg("timestamptz_timestamp", "value", "CAST(value AT TIME ZONE 'UTC' AS TIMESTAMP)"),
    *(
        Macro(
            name,
            ("part", "value"),
            {_DUCKDB: Pg("DATE_PART(part, value)"), _EVERY: Pg(pg_forms.date_part_form(name))},
            own={_CLICKHOUSE: pg_forms.clickhouse_date_part()},
        )
        for name in ("date_part", "extract")
    ),
    _macro("datetimetz_pl", "d, t", strict("d, t", "TIMESTAMPTZ", pg_forms.DATETIMETZ_PL)),
    _macro("timetzdate_pl", "t, d", strict("d, t", "TIMESTAMPTZ", pg_forms.DATETIMETZ_PL)),
    _macro(
        "timestamp_scale", "value, n", strict("value, n", "TIMESTAMP", pg_forms.TIMESTAMP_SCALE)
    ),
    _macro(
        "timestamptz_scale",
        "value, n",
        strict("value, n", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_SCALE),
    ),
    _macro("time_scale", "value, n", strict("value, n", "TIME", pg_forms.TIME_SCALE)),
    _macro("timetz", "value, n", strict("value, n", "TIMETZ", pg_forms.TIMETZ_SCALE)),
    _macro("interval_scale", "value, n", strict("value, n", "INTERVAL", pg_forms.INTERVAL_SCALE)),
    _macro("interval_time", "value", strict("value", "TIME", pg_forms.INTERVAL_TIME)),
    _macro("time_interval", "value", strict("value", "INTERVAL", pg_forms.TIME_INTERVAL)),
    _macro(
        "timestamp_time",
        "value",
        strict("value", "TIME", pg_forms.TIMESTAMP_TIME),
        {_EVERY: Pg("CAST(value AS TIME)")},
        differs_on=_CLICKHOUSE,
    ),
    _macro(
        "timestamptz_time",
        "value",
        strict("value", "TIME", pg_forms.TIMESTAMPTZ_TIME),
        {_EVERY: Pg("CAST(value AT TIME ZONE 'UTC' AS TIME)")},
        differs_on=_CLICKHOUSE,
    ),
    _pg("timetz_time", "value", "CAST(value AS TIME)"),
    _pg("time", "value", "CAST(value AS TIME)", _NO_TIME),
    _macro(
        "timestamptz",
        "value",
        strict("value", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_OF),
        {_EVERY: Pg("CAST(value AS TIMESTAMPTZ)")},
        differs_on=_CLICKHOUSE,  # it has no zoned value
    ),
    _macro(
        "timestamptz",
        "d, t",
        strict("d, t", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_OF_DATE_AND_TIME),
    ),
    _macro("timestamptz_timetz", "value", strict("value", "TIMETZ", pg_forms.TIMESTAMPTZ_TIMETZ)),
    _macro("time_timetz", "value", strict("value", "TIMETZ", pg_forms.TIME_TIMETZ)),
    _pg("timetz", "value", "CAST(value AS TIMETZ)", _NO_TIME),
)

ROWS: tuple[Macro, ...] = (
    *(
        Macro("mul_d_interval", ("b", "a"), operators={_DUCKDB: Pg(form.sql)})
        for form in pg_forms_types.OPERATORS
        if form.name == "interval_mul"
    ),
    *_DATES,
)
