# Copyright (c) 2026 Kenneth Stott
# Canary: ca9bb842-5b53-4a9a-b7ff-601f98209d53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An Oracle CLOB/NCLOB/BLOB is read as its value, not as a LOB locator a result cannot carry:
python-oracledb's data-frame fetch reads a LOB as its text or bytes (REQ-2194)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, NamedTuple

import oracledb  # pyright: ignore[reportMissingImports]
import pyarrow as pa

from provisa.executor.drivers.oracle import OracleDriver


class _Col(NamedTuple):
    name: str
    type_code: Any


_LOBS = [
    _Col("C", oracledb.DB_TYPE_CLOB),
    _Col("NC", oracledb.DB_TYPE_NCLOB),
    _Col("B", oracledb.DB_TYPE_BLOB),
]


class _Cursor:
    description: Any = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def parse(self, _sql):  # a statement is described before it runs
        self.description = _LOBS

    def execute(self, sql, params, **kwargs):
        raise AssertionError("a result is read through the data-frame fetch, not a cursor")


class _Conn:
    def __init__(self, seen: list) -> None:
        self._seen = seen

    def cursor(self):
        return _Cursor()

    def fetch_df_all(self, statement, parameters, arraysize=None, *, fetch_decimals=None):
        self._seen.append(statement)
        return pa.table(
            {
                "C": pa.array(["text"], pa.large_string()),
                "NC": pa.array(["texte"], pa.large_string()),
                "B": pa.array([b"\x00\x01"], pa.large_binary()),
            }
        )

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
    result = await driver.execute("SELECT c, nc, b FROM t WHERE id = $1", [1])
    assert seen == ["SELECT c, nc, b FROM t WHERE id = :1"]
    assert result.rows == [("text", "texte", b"\x00\x01")]
