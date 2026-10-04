# Copyright (c) 2026 Kenneth Stott
# Canary: fececd18-f9da-4777-99ef-c6391d5ac072
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1905: the catalog's best-effort reltuples fetch must not consume the request deadline.

``_fetch_row_counts`` issues one ``SHOW STATS FOR`` engine round-trip per visible table. A catalog
reflection (SQLAlchemy ``get_columns``, the hstore oid lookup) runs on the pgwire connection thread
inside the held request deadline, so an estate with many tables -- or a slow stats call -- could let
the row-count phase eat the whole deadline and fail the reflection with "the request's deadline
passed". The phase is capped by ``_ROW_COUNT_BUDGET_S`` so it stops issuing round-trips once the
budget is spent; the remaining tables keep reltuples 0 and the partial result is still returned (and
cached by the caller), so a reflection is never failed by the estimate."""

from __future__ import annotations

import time
from types import SimpleNamespace

from provisa.pgwire import catalog_populate


class _SlowCursor:
    def __init__(self, delay: float) -> None:
        self._delay = delay

    def execute(self, sql: str) -> None:
        time.sleep(self._delay)

    def fetchall(self):
        # Shape _fetch_row_counts reads: the summary row has col 0 NULL and col 4 the row count.
        return [(None, None, None, None, 1000.0)]


class _SlowEngineConn:
    def __init__(self, delay: float) -> None:
        self._delay = delay
        self.calls = 0

    def cursor(self):
        self.calls += 1
        return _SlowCursor(self._delay)


def _ctx_and_idx(n: int):
    tables = {
        i: SimpleNamespace(table_id=i, catalog_name="c", schema_name="s", table_name=f"t{i}")
        for i in range(n)
    }
    ctx = SimpleNamespace(tables=tables)
    idx = catalog_populate.CatalogIndex()
    idx.tables = [("c", "s", f"t{i}", i, 1000 + i) for i in range(n)]
    return ctx, idx


def test_row_count_fetch_is_bounded_and_does_not_do_a_round_trip_per_table(monkeypatch):
    # 200 tables at 50 ms each would be 10 s unbounded -- far past a reflection's patience. With a
    # 0.2 s budget the fetch must return in well under a second, having queried only a few tables.
    monkeypatch.setattr(catalog_populate, "_ROW_COUNT_BUDGET_S", 0.2)
    ctx, idx = _ctx_and_idx(200)
    engine = _SlowEngineConn(delay=0.05)

    start = time.monotonic()
    result = catalog_populate._fetch_row_counts(ctx, idx, engine)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"row-count fetch ran {elapsed:.2f}s; it must stop at the budget"
    assert engine.calls < 200, (
        f"queried {engine.calls}/200 tables; must stop once the budget is spent"
    )
    # Best-effort: whatever it did fetch is returned (and the caller caches it), so a reflection
    # proceeds with partial reltuples rather than failing.
    assert isinstance(result, dict)


def test_no_engine_connection_returns_empty():
    ctx, idx = _ctx_and_idx(3)
    assert catalog_populate._fetch_row_counts(ctx, idx, None) == {}
