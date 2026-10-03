# Copyright (c) 2026 Kenneth Stott
# Canary: 53b9dbbc-a2e9-4faa-a1a4-6ee7ea711fcb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A keyed fetch's key values reach the source as data, whatever they hold.

The ClickHouse keyed fetch names the rows it needs by their key values. ClickHouse reads a
backslash inside a string literal as an escape, so a key value written into the statement with
quote-doubling alone can end the literal early. Here the statement the fetch sends runs on a
real ClickHouse (chdb, in this process): a key that tries to close the literal and add a predicate
matches no row, and a key holding a backslash or a quote matches its own row."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

HOSTILE = "x\\') OR 1=1 --"
ROWS = [("a", "1"), ("b", "2"), ("it's", "3"), ("back\\slash", "4")]


class _ChdbDriver:
    """Stands in for the ClickHouse driver: binds the statement's parameters the way
    clickhouse-connect does, and runs it on an in-process ClickHouse holding one ``orders``
    table."""

    session = None

    def configure(self, hints):
        del hints

    async def connect(self, *args, **kwargs):
        del args, kwargs

    async def execute_arrow(self, sql, params=None):
        import io

        import pyarrow as pa
        from clickhouse_connect.driver.binding import bind_query

        # The driver's own binding, as clickhouse-connect applies it before sending.
        sql, server_params = bind_query(sql, params)
        raw = self.session.query(sql, "ArrowStream", params=server_params or None).bytes()
        return pa.ipc.open_stream(io.BytesIO(raw)).read_all()

    async def close(self):
        pass


@pytest.fixture
def orders(monkeypatch):
    from chdb import session

    s = session.Session()
    s.query("CREATE TABLE orders (id String, v String) ENGINE = Memory")
    for key, value in ROWS:
        s.query("INSERT INTO orders VALUES ({k:String}, {v:String})", params={"k": key, "v": value})
    _ChdbDriver.session = s
    monkeypatch.setattr("provisa.executor.drivers.clickhouse.ClickHouseDriver", _ChdbDriver)
    yield
    s.close()


def _fetch(keys):
    import asyncio

    from provisa.events.source_loader import make_clickhouse_keyed_arrow_loader

    source = SimpleNamespace(
        id="ch",
        host="h",
        port=8123,
        database="default",
        username="u",
        password="",
        federation_hints={},
    )
    table = SimpleNamespace(
        table_name="orders",
        schema_name="default",
        columns=[
            SimpleNamespace(name="id", data_type="varchar", native_filter_type=None),
            SimpleNamespace(name="v", data_type="varchar", native_filter_type=None),
        ],
    )
    fetched = asyncio.run(make_clickhouse_keyed_arrow_loader()(source, table, ["id"], keys))
    return sorted(fetched.column("id").to_pylist())


def test_a_key_that_tries_to_end_its_literal_matches_no_row(orders):
    assert _fetch([(HOSTILE,)]) == []


def test_keys_holding_a_quote_or_a_backslash_match_their_own_rows(orders):
    assert _fetch([("it's",), ("back\\slash",), ("a",)]) == ["a", "back\\slash", "it's"]
