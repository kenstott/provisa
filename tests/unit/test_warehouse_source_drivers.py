# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-986/987/988: warehouses (Databricks/Snowflake/ClickHouse) are first-class NAMED SOURCES —
read directly then landed on ANY engine, reusing the same connection their federation engine uses.
Driver-free unit coverage: registry wiring, reachability on every engine, the federation_hints →
``configure`` → ``connect`` channel for the params the standard args can't carry. Live reads are
exercised in integration (Databricks) / skipped (Snowflake — no creds)."""

from __future__ import annotations

import asyncio

import pytest

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.registry import create_driver, has_driver
from provisa.federation.engine import reachable_source_types


@pytest.mark.parametrize("stype", ["databricks", "snowflake", "clickhouse"])
def test_warehouse_has_driver_and_factory(stype):
    assert has_driver(stype)
    assert isinstance(create_driver(stype), DirectDriver)


@pytest.mark.parametrize("stype", ["databricks", "snowflake", "clickhouse"])
def test_warehouse_reachable_as_source_on_every_engine(stype):
    # Direct-read-then-land makes them reachable regardless of the selected engine (no Trino needed).
    for engine in ("duckdb", "trino", "pg", "clickhouse"):
        assert stype in reachable_source_types(engine), (stype, engine)


def test_databricks_connect_requires_http_path():
    # http_path can't ride host/port/user/password; it must arrive via federation_hints/configure.
    d = create_driver("databricks")
    d.configure({})  # no http_path
    with pytest.raises(ValueError, match="http_path"):
        asyncio.run(d.connect("host", 443, "cat", "token", "tok"))


def test_databricks_configure_stashes_http_path():
    d = create_driver("databricks")
    d.configure({"http_path": "/sql/1.0/warehouses/abc"})
    assert d._http_path == "/sql/1.0/warehouses/abc"  # noqa: SLF001


def test_snowflake_connect_requires_account():
    d = create_driver("snowflake")
    d.configure({})  # no account, host empty → account unresolved
    with pytest.raises(ValueError, match="account"):
        asyncio.run(d.connect("", 443, "db", "user", "pw"))


def test_rdbms_driver_configure_is_noop():
    # The base no-op keeps the RDBMS drivers untouched by the new channel.
    d = create_driver("postgresql")
    d.configure({"anything": "ignored"})  # must not raise


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
