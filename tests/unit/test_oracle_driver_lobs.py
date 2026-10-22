# Copyright (c) 2026 Kenneth Stott
# Canary: ca9bb842-5b53-4a9a-b7ff-601f98209d53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An Oracle CLOB/BLOB is read as its value, not as a LOB locator a result cannot carry."""

from __future__ import annotations

from contextlib import contextmanager

from provisa.executor.drivers.oracle import OracleDriver


class _Cursor:
    description = None

    def __init__(self, seen: list) -> None:
        self._seen = seen

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def parse(self, _sql):  # a statement is described before it runs; this one answers no rows
        pass

    def execute(self, sql, params, **kwargs):
        self._seen.append(kwargs)


class _Conn:
    def __init__(self, seen: list) -> None:
        self._seen = seen

    def cursor(self):
        return _Cursor(self._seen)

    def cancel(self):
        pass

    def commit(self):
        pass


class _Pool:
    def __init__(self, seen: list) -> None:
        self._seen = seen

    @contextmanager
    def connection(self, is_broken):
        yield _Conn(self._seen)


async def test_a_lob_is_fetched_as_its_value():
    seen: list = []
    driver = OracleDriver()
    driver._pool = _Pool(seen)  # pyright: ignore[reportAttributeAccessIssue]
    await driver.execute("SELECT c FROM t WHERE id = $1", [1])
    assert seen == [{"fetch_lobs": False}]
