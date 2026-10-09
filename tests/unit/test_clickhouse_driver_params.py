# Copyright (c) 2026 Kenneth Stott
# Canary: d77b53bb-d930-484f-b021-4450c6cee1e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouseDriver.execute() must substitute the governed pipeline's `@N`/`$N` positional
placeholders before sending SQL to ClickHouse — confirmed live (federated_join via GraphQL,
perf-bench) that ClickHouse rejects `@N` outright as a syntax error when it reaches the wire
unsubstituted; the driver previously discarded `params` entirely on the assumption the SQL
"arrives fully formed," which only held for the single-source path it was first written
against. Mirrors tests/unit/test_executor_trino.py's TestExecuteTrinoParameterSubstitution
pattern for the equivalent Trino bug class.
"""

from __future__ import annotations

import asyncio

from provisa.executor.drivers.clickhouse import ClickHouseDriver


class _FakeResult:
    def __init__(self, rows, cols):
        self.result_rows = rows
        self.column_names = cols


class _FakeClient:
    def __init__(self, rows=None, cols=None):
        self._rows = rows or []
        self._cols = cols or []
        self.last_sql: str | None = None
        self.last_parameters = None

    def query(self, sql, parameters=None, settings=None):
        self.last_sql = sql
        self.last_parameters = parameters
        return _FakeResult(self._rows, self._cols)

    def close(self):
        pass


def _make_driver(rows=None, cols=None) -> tuple[ClickHouseDriver, _FakeClient]:
    from provisa.core.sync_pool import BlockingPool

    driver = ClickHouseDriver()
    client = _FakeClient(rows=rows, cols=cols)
    # the driver's pool, holding this one client
    driver._pool = BlockingPool(
        lambda: client,
        lambda c: c.close(),
        minsize=1,
        maxsize=1,
        wait_s=5.0,
        name="clickhouse:test",
    )
    return driver, client


class TestClickHouseDriverParameterSubstitution:
    def test_no_params_passed_as_is(self):
        driver, client = _make_driver(rows=[(42,)], cols=["n"])
        result = asyncio.run(driver.execute("SELECT 42 AS n"))
        assert result.rows == [(42,)]
        assert result.column_names == ["n"]
        assert client.last_sql == "SELECT 42 AS n"
        assert client.last_parameters is None

    def test_at_param_replaced_and_bound(self):
        driver, client = _make_driver(rows=[("x",)], cols=["v"])
        asyncio.run(driver.execute("SELECT @1 AS v", params=["x"]))
        assert client.last_sql is not None
        assert "@1" not in client.last_sql
        assert client.last_parameters == {"p1": "x"}

    def test_dollar_param_replaced_and_bound(self):
        driver, client = _make_driver(rows=[], cols=["v"])
        asyncio.run(driver.execute("SELECT $1 AS v", params=["x"]))
        assert client.last_sql is not None
        assert "$1" not in client.last_sql
        assert client.last_parameters == {"p1": "x"}

    def test_multiple_params_all_replaced(self):
        driver, client = _make_driver(rows=[], cols=["a", "b"])
        asyncio.run(driver.execute("SELECT @1, @2", params=["lo", "hi"]))
        assert client.last_sql is not None
        assert "@1" not in client.last_sql
        assert "@2" not in client.last_sql
        assert client.last_parameters == {"p1": "lo", "p2": "hi"}

    def test_replacement_order_no_prefix_collision(self):
        """@10 must not be corrupted by a naive forward-order @1 replacement first."""
        driver, client = _make_driver(rows=[], cols=["a"])
        params = list(range(10))  # 10 values -> placeholders @1..@10
        asyncio.run(driver.execute("SELECT @1, @10", params=params))
        assert client.last_sql is not None
        assert client.last_sql.count("%(p1)s") == 1
        assert "@10" not in client.last_sql
        assert client.last_parameters is not None
        assert client.last_parameters["p1"] == 0
        assert client.last_parameters["p10"] == 9


class TestClickHouseDriverSessions:
    """One clickhouse-connect client is one session and a session runs one query at a time
    (REQ-1882): each in-flight request checks out its own client, and the pool bounds them."""

    @staticmethod
    def _driver(monkeypatch, max_pool: int):
        import threading

        import clickhouse_connect

        opened: list = []
        lock = threading.Lock()

        class _Session:
            def __init__(self) -> None:
                self.in_flight = 0
                self.overlapped = False
                self.closed = False

            def query(self, sql, parameters=None, settings=None):
                import time

                with lock:
                    self.in_flight += 1
                    self.overlapped = self.overlapped or self.in_flight > 1
                time.sleep(0.05)
                with lock:
                    self.in_flight -= 1
                return _FakeResult([(1,)], ["n"])

            def close(self):
                self.closed = True

        def _get_client(**kwargs):
            session = _Session()
            with lock:
                opened.append(session)
            return session

        monkeypatch.setattr(clickhouse_connect, "get_client", _get_client)
        driver = ClickHouseDriver()
        asyncio.run(driver.connect("h", 8123, "default", "u", "p", min_pool=1, max_pool=max_pool))
        return driver, opened

    def test_concurrent_requests_never_share_a_session(self, monkeypatch):
        import threading

        driver, opened = self._driver(monkeypatch, max_pool=3)
        start = threading.Barrier(9)
        errors: list[BaseException] = []

        def _request() -> None:
            try:
                start.wait(timeout=10)
                assert asyncio.run(driver.execute("SELECT 1 AS n")).rows == [(1,)]
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_request) for _ in range(9)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, errors[:1]
        assert not any(s.overlapped for s in opened), "two requests ran on one session at once"
        assert 1 <= len(opened) <= 3, f"the pool opened {len(opened)} sessions for max_pool=3"

    def test_close_closes_every_session(self, monkeypatch):
        driver, opened = self._driver(monkeypatch, max_pool=2)
        asyncio.run(driver.execute("SELECT 1 AS n"))
        asyncio.run(driver.close())
        assert opened and all(s.closed for s in opened) and not driver.is_connected


def test_a_pooled_client_names_no_server_session(monkeypatch):
    """A ClickHouse session admits one statement at a time and is released a moment after its
    answer is read; a pooled client goes to the next request at once. Every client the driver
    opens -- pooled, and the cancel's own -- names no session (#134, #189)."""
    import asyncio

    import clickhouse_connect

    from provisa.executor.drivers.clickhouse import ClickHouseDriver

    asked: list[dict] = []

    class _Client:
        def command(self, *_a, **_k):
            return None

        def close(self):
            return None

    def _get_client(**kwargs):
        asked.append(kwargs)
        return _Client()

    monkeypatch.setattr(clickhouse_connect, "get_client", _get_client)
    driver = ClickHouseDriver()
    asyncio.run(driver.connect("h", 8123, "default", "u", "p", min_pool=2, max_pool=2))
    driver._kill_query("abc")  # noqa: SLF001

    assert len(asked) == 3
    for kwargs in asked:
        assert kwargs["autogenerate_session_id"] is False
        assert "session_id" not in kwargs
