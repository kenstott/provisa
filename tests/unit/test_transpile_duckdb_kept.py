# Copyright (c) 2026 Kenneth Stott
# Canary: a2eff3ee-9db9-445e-9a63-e3c00f2f2b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-066: a statement's DuckDB form is derived once, whatever repeats it."""

# Requirements: REQ-066

from __future__ import annotations

from collections.abc import Iterator

import pytest

from provisa.transpiler import pg_money, transpile
from provisa.transpiler.transpile import transpile_to_duckdb


@pytest.fixture(autouse=True)
def nothing_kept() -> Iterator[None]:
    transpile._duckdb_form.cache_clear()
    yield
    transpile._duckdb_form.cache_clear()


def _counted(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    lowered = pg_money.lowered

    def counted(sql: str, dialect: str) -> str:
        seen.append(sql)
        return lowered(sql, dialect)

    monkeypatch.setattr(transpile.pg_money, "lowered", counted)
    return seen


def test_a_repeated_statement_is_derived_once(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _counted(monkeypatch)
    first = transpile_to_duckdb("SELECT 1 AS a")
    derived = len(seen)
    assert transpile_to_duckdb("SELECT 1 AS a") == first
    assert len(seen) == derived


def test_another_statement_is_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _counted(monkeypatch)
    assert transpile_to_duckdb("SELECT 1 AS a") != transpile_to_duckdb("SELECT 2 AS a")
    assert set(seen) == {"SELECT 1 AS a", "SELECT 2 AS a"}


def test_a_refusal_is_not_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    lowered = pg_money.lowered

    def refused_once(sql: str, dialect: str) -> str:
        calls.append(sql)
        if len(calls) == 1:
            raise ValueError("refused")
        return lowered(sql, dialect)

    monkeypatch.setattr(transpile.pg_money, "lowered", refused_once)
    with pytest.raises(ValueError, match="refused"):
        transpile_to_duckdb("SELECT 3 AS a")
    assert "3" in transpile_to_duckdb("SELECT 3 AS a")


def test_the_stage_is_traced_on_a_kept_statement_too(monkeypatch: pytest.MonkeyPatch) -> None:
    from provisa.observability import stage_trace

    traced: list[tuple[str, str]] = []
    monkeypatch.setattr(stage_trace, "trace_stage", lambda stage, sql: traced.append((stage, sql)))
    kept = transpile_to_duckdb("SELECT 4 AS a")
    traced.clear()
    assert transpile_to_duckdb("SELECT 4 AS a") == kept
    assert traced == [("transpile.duckdb", kept)]


_CANONICAL_UUID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"


@pytest.mark.parametrize(
    "written",
    [
        "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
        "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}",
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        "a0eebc999c0b4ef8bb6d6bb9bd380a11",
        "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
    ],
)
def test_uuid_literal_cast_is_canonical_and_executes(written):
    import duckdb

    from provisa.transpiler.transpile import transpile_to_duckdb

    sql = transpile_to_duckdb(f"SELECT '{written}'::uuid")
    assert f"'{_CANONICAL_UUID}'" in sql
    assert str(duckdb.connect().execute(sql).fetchone()[0]) == _CANONICAL_UUID


def test_invalid_uuid_literal_cast_is_left_to_the_engine():
    from provisa.transpiler.transpile import transpile_to_duckdb

    assert "'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1}'" in transpile_to_duckdb(
        "SELECT '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1}'::uuid"
    )
