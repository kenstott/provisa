# Copyright (c) 2026 Kenneth Stott
# Canary: 9772d88f-e0b2-4f80-835a-16ab1d388665
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Date/timestamp token substitution for scheduled SQL (REQ-1004).

A scheduled SQL statement may embed ``{{token}}`` placeholders that are
replaced with the run's execution date/time immediately before execution.
Substitution is pure and deterministic given ``run_at``.

Supported tokens:

    {{yyyymmdd}}    -> run_at as YYYYMMDD           (e.g. 20260713)
    {{YYYY-MM-DD}}  -> run_at as YYYY-MM-DD          (e.g. 2026-07-13)
    {{iso8601}}     -> run_at.isoformat()            (e.g. 2026-07-13T14:30:00+00:00)
    {{timestamp}}   -> integer Unix epoch seconds    (e.g. 1784056200)

An unrecognized ``{{...}}`` token raises ValueError (fail loud — no silent
pass-through of a possibly-mistyped token).
"""
# Requirements: REQ-1004

from __future__ import annotations

import re
from datetime import datetime

_TOKEN_RE = re.compile(r"\{\{\s*([^}]*?)\s*\}\}")


def _render_token(name: str, run_at: datetime) -> str:
    if name == "yyyymmdd":
        return run_at.strftime("%Y%m%d")
    if name == "YYYY-MM-DD":
        return run_at.strftime("%Y-%m-%d")
    if name == "iso8601":
        return run_at.isoformat()
    if name == "timestamp":
        return str(int(run_at.timestamp()))
    raise ValueError(f"Unrecognized scheduled-SQL date token: {{{{{name}}}}}")


def substitute_date_tokens(sql: str, run_at: datetime) -> str:
    """Replace ``{{token}}`` date placeholders in ``sql`` with values derived
    from ``run_at``. Pure and deterministic. Raises ValueError on an unknown
    token (REQ-1004)."""
    return _TOKEN_RE.sub(lambda m: _render_token(m.group(1), run_at), sql)
