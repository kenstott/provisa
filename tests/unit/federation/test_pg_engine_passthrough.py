# Copyright (c) 2026 Kenneth Stott
# Canary: c674b4d7-32ab-49df-bdd9-70d5b9466b4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``EngineRuntime.execute_pg_engine_passthrough`` — the REQ-1863 counterpart for the ENGINE route
when the bound federation engine is itself Postgres (REQ-904, PROVISA_ENGINE=pg): pgwire and the
engine both speak real Postgres wire protocol end to end, so the same raw-DataRow passthrough
mechanism (provisa/pgwire/pg_passthrough.py) applies to the ENGINE route too, not just DIRECT."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.runtime import EngineRuntime, _strip_driver_suffix
from provisa.pgwire.pg_passthrough import PassthroughError
from tests.unit.pgwire.test_pg_passthrough import (
    _BIND_OK,
    _PARAMS_NONE,
    _PARSE_OK,
    _READY,
    _data_row,
    _FakePostgres,
    _msg,
    _row_description,
)


class _Backend:
    """An engine backend double: lends whatever ``borrow`` returns, recording who asked."""

    def __init__(self, borrow) -> None:
        self._borrow = borrow
        self.asked: list[object] = []

    def borrow_raw_pg_connection(self, state):
        self.asked.append(state)
        return self._borrow()


def test_the_engine_passthrough_reads_on_a_connection_from_the_engines_own_pool():
    """REQ-1863 (amended 2026-10-01): no URL is read and no connection opened — the engine's
    runtime lends one pooled connection, which goes back to its pool when the stream drains."""
    row = _data_row(1, [b"7"])
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("n", 20)])),
            ("BES", _BIND_OK + row + _msg(b"C", b"SELECT 1\x00") + _READY),
        ]
    )
    backend = _Backend(pg.borrow)
    state = object()
    rt = EngineRuntime(SimpleNamespace(backend=backend), state=state)
    try:
        stream = rt.execute_pg_engine_passthrough("SELECT n FROM t", None, [1], described_oids=[20])
        assert stream.column_names == ["n"]
        assert [bytes(r) for batch in stream.batches() for r in batch] == [row]
    finally:
        pg.finish()
    assert backend.asked == [state]
    assert pg.released == [False]


def test_a_pool_that_cannot_lend_a_connection_fails_the_request():
    """The passthrough applies but cannot get its connection: the request fails with that error;
    it is never read as "does not apply" (which would silently decode instead)."""

    def _exhausted():
        raise RuntimeError("no engine connection freed within 120.0s (all 10 checked out)")

    rt = EngineRuntime(SimpleNamespace(backend=_Backend(_exhausted)), state=object())
    with pytest.raises(RuntimeError, match="no engine connection freed") as exc:
        rt.execute_pg_engine_passthrough("SELECT 1", None, [0], described_oids=None)
    assert not isinstance(exc.value, PassthroughError)


def test_an_engine_without_a_postgres_pool_does_not_apply():
    from provisa.federation.backend import EngineBackend

    not_postgres = SimpleNamespace(engine=SimpleNamespace(name="duckdb"))
    with pytest.raises(PassthroughError, match="duckdb"):
        EngineBackend.borrow_raw_pg_connection(not_postgres, state=object())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("postgresql+psycopg2://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgresql://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgres+asyncpg://u:p@host/db", "postgres://u:p@host/db"),
    ],
)
def test_strip_driver_suffix(raw: str, expected: str) -> None:
    assert _strip_driver_suffix(raw) == expected
