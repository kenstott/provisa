# Copyright (c) 2026 Kenneth Stott
# Canary: a4ee3867-fafd-4bcc-bf87-511d42a283f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of network addresses (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms
from provisa.transpiler.rows.base import (
    _EVERY,
    _EVERY_BUT_DUCKDB,
    _TRINO,
    MAPPED,
    Macro,
    Pg,
    _macro,
    strict,
)

_ABBREVS: tuple[Macro, ...] = (
    _macro("inet_abbrev", "value", pg_forms.INET_ABBREV, {_TRINO: pg_forms.INET_ABBREV}),
    _macro("cidr_abbrev", "value", pg_forms.CIDR_ABBREV, {_TRINO: pg_forms.TRINO_CIDR_ABBREV}),
    # A value whose type the statement does not say is the engine's inet, or a cidr's text.
    _macro(
        "abbrev",
        "value",
        f"CASE typeof(value) WHEN 'INET' THEN {pg_forms.INET_ABBREV} "
        f"ELSE {pg_forms.CIDR_ABBREV} END",
    ),
)
#: inet's text always says its mask; the engine's value says it only where it is not the widest.
_INET_TEXT = (
    "CASE WHEN STRPOS(value, '/') > 0 THEN value WHEN STRPOS(value, ':') > 0 "
    "THEN value || '/128' ELSE value || '/32' END"
)

ROWS: tuple[Macro, ...] = (
    Macro("inet_text", ("value",), {_EVERY: Pg(strict("value", "TEXT", _INET_TEXT))}),
    _macro("netmask", "value", "host(netmask(value))"),
    # DuckDB's broadcast of an IPv4 address of the high bit holds it as a negative integer, which
    # is no address; its text is the address, read again.
    _macro(
        "broadcast",
        "value",
        "CAST(CAST(broadcast(value) AS VARCHAR) AS INET)",
        own={_EVERY_BUT_DUCKDB: MAPPED},
    ),
    *_ABBREVS,
)
