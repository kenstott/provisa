# Copyright (c) 2026 Kenneth Stott
# Canary: f03a8e65-c1d7-4b29-a6e4-19d7b5c2e086
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The text search functions on Trino and ClickHouse against what postgres:17.11 answered (REQ-2167,
REQ-2171): each statement of the corpus, as Provisa writes it for the engine, run there. DuckDB's
replay is tests/unit/pgwire/test_pg_text_search_forms.py. A statement conformance-functions measures
on the engine is not replayed here (tests/integration/replay_coverage.py)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

import pytest

from tests.integration.every_case import failures_of_every_case
from tests.integration.replay_coverage import ENGINES, remaining
from tests.unit.pgwire.text_search_replay import BY_FUNCTION, outcome

pytestmark = [pytest.mark.integration]

#: Each engine's statements of each function that conformance-functions does not measure there.
_REMAINING: dict[str, dict[str, list[str]]] = {
    engine: {
        function: left
        for function, statements in BY_FUNCTION.items()
        if (left := remaining("text_search", engine, statements))
    }
    for engine in ENGINES
}


def _check(
    function: str, dialect: str, run: Callable[[str], list[Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    on_engine = [0.0, 0.0, 0]  # seconds on the engine, its slowest statement, characters sent

    def timed(sql: str) -> list[Any]:
        started = time.perf_counter()
        try:
            return run(sql)
        finally:
            took = time.perf_counter() - started
            on_engine[0] += took
            on_engine[1] = max(on_engine[1], took)
            on_engine[2] = max(on_engine[2], len(sql))

    statements = _REMAINING[dialect][function]
    started = time.perf_counter()
    said = [outcome(sql, dialect, timed) for sql in statements]
    wrong, refused = [s for s in said if s not in ("ok", "refused")], said.count("refused")
    took = time.perf_counter() - started
    with capsys.disabled():  # the run's record, in the log whether the test passes or not
        print(  # noqa: T201
            f"\nTIMING {dialect} {function} statements={len(statements)} "
            f"refused={refused} total={took:.1f}s engine={on_engine[0]:.1f}s "
            f"slowest={on_engine[1]:.2f}s largest_sql={on_engine[2]}"
        )
    assert not wrong, f"{len(wrong)} differ on {dialect}: " + "\n".join(wrong[:30])


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(_REMAINING["trino"]))
def test_text_search_on_trino(trino_conn, function: str, capsys):
    def run(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    _check(function, "trino", run, capsys)


@pytest.mark.requires_clickhouse
def test_text_search_on_clickhouse(capsys):
    """One test over every function: requires_clickhouse boots the engine for each test, and one
    test to a function booted it 48 times (tests/integration/every_case.py)."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )

    def run(sql: str) -> list[Any]:
        return list(client.query(sql).result_rows)

    assert (
        failures_of_every_case(
            sorted(_REMAINING["clickhouse"]), lambda f: _check(f, "clickhouse", run, capsys)
        )
        == {}
    )
