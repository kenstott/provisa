# Copyright (c) 2026 Kenneth Stott
# Canary: 7d24a9e1-58c3-4b0f-a6e2-c91f3b5d8e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the Trino and ClickHouse function replays still run (REQ-2167, REQ-2171; REQ-1989 cadence,
maintainer 2026-11-20): each family's statements less those conformance-functions already measures
on the engine, as scripts/conformance/replay_coverage.py recorded them in
function_replay_covered.json. A statement the file does not name is run."""

from __future__ import annotations

import json
from collections.abc import Iterable
from functools import cache
from pathlib import Path

_DATA = Path(__file__).with_name("function_replay_covered.json")
ENGINES = ("trino", "clickhouse")


@cache
def _covered() -> dict[str, dict[str, list[str]]]:
    return json.loads(_DATA.read_text())


def covered(family: str, engine: str) -> frozenset[str]:
    """The statements of ``family`` conformance-functions measures on ``engine``."""
    if engine not in ENGINES:
        raise ValueError(f"no replay coverage is recorded for {engine}")
    return frozenset(sql for sql, engines in _covered()[family].items() if engine in engines)


def remaining(family: str, engine: str, statements: Iterable[str]) -> list[str]:
    """``statements`` in their order, less those conformance-functions measures on ``engine``."""
    measured = covered(family, engine)
    return [sql for sql in statements if sql not in measured]
