# Copyright (c) 2026 Kenneth Stott
# Canary: e35a71c9-08b4-4d6e-8f23-b16d4c09a7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Helpers the per-engine geometric replays share (REQ-2171)."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import duckdb

from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.geometric_replay import BY_FUNCTION, column_statement, problems, same

#: The DuckDB forms replay is cut into this many files (every Nth function, in sorted order) so each
#: fits a core shard: one file of all of them ran 1866 s (CI run 45720298507).
DUCKDB_FORM_SLICES = 4


def duckdb_form_slice(index: int) -> list[str]:
    return sorted(BY_FUNCTION)[index::DUCKDB_FORM_SLICES]


def duckdb_forms(function: str) -> None:
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    assert check(function, "duckdb", run) == []


def check(function: str, dialect: str, run: Callable[[str], Any]) -> list[str]:
    wrong, refused = problems(function, dialect, run)
    print("REFUSED", dialect, function, len(refused))  # noqa: T201 -- the run's record
    assert not wrong, f"{len(wrong)} differ on {dialect}: " + "\n".join(wrong[:30])
    return refused


def clickhouse_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def on_columns(function: str, dialect: str, rows: Callable[[str], list[Any]]) -> None:
    """A form over a column: two rows, each PostgreSQL's answer for its own values."""
    statement = column_statement(function)
    if statement is None:  # every recorded statement of the function is an error or holds a NULL
        return
    sql, wants = statement
    got = rows(transpile(sql, dialect, generated=False))
    assert len(got) == len(wants) and all(map(same, got, wants)), f"{sql}: {got}, {wants}"
