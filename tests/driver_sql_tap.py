# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7a5e91-6d3f-4b08-8e14-9a0b7f5d1c62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Test-only tap on the SQL a real server hands to its source drivers and its engine.

Installed by ``tests.integration.driver_sql_tap_app`` after the app is imported. It changes no
behaviour: every wrapper calls straight through. It appends one JSON line per statement to
``$PROVISA_TEST_DRIVER_SQL_TAP``:

- ``source``: the statement a source's own driver receives — ``SourcePool.execute`` (the buffered
  DIRECT read) and ``SourcePool.open_stream`` (the streamed DIRECT read);
- ``engine``: the physical statement the federation engine receives —
  ``EngineRuntime.execute_engine`` / ``execute_engine_sync``;
- ``sqlglot``: one record per SQL tokenization (every parse starts with one) and per expression
  tree copy, with the provisa frame that asked for it — what a request spends re-deriving a
  statement it has already compiled.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from typing import Any

_out_fd: int | None = None


def _write(terminal: str, sql: Any, source_id: Any = None, params: Any = None) -> None:
    assert _out_fd is not None
    record = {
        "terminal": terminal,
        "source_id": source_id,
        "sql": str(sql),
        "params": [repr(p) for p in (params or [])],  # the values bound to the statement
    }
    os.write(_out_fd, (json.dumps(record) + "\n").encode())  # O_APPEND: one atomic write


# The provisa package directory (this file is <repo>/tests/driver_sql_tap.py).
_PACKAGE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "provisa")


def _provisa_caller() -> str:
    """``file:function`` of the nearest provisa-package frame on the stack (the sqlglot call's
    requester)."""
    frame = sys._getframe(3)  # past this function, _write_sqlglot and the wrapper
    while frame is not None:
        filename = frame.f_code.co_filename
        if filename.startswith(_PACKAGE + os.sep):
            return f"provisa/{os.path.relpath(filename, _PACKAGE)}:{frame.f_code.co_name}"
        frame = frame.f_back
    return "?"


def _write_sqlglot(op: str) -> None:
    assert _out_fd is not None
    record = {
        "terminal": "sqlglot",
        "op": op,
        "at": _provisa_caller(),
        "thread": threading.current_thread().name,
    }
    os.write(_out_fd, (json.dumps(record) + "\n").encode())


def _install_sqlglot_tap() -> None:
    from sqlglot.expressions import Expression
    from sqlglot.tokens import Tokenizer

    real_tokenize = Tokenizer.tokenize
    real_deepcopy = Expression.__deepcopy__

    def tokenize(self, sql):  # type: ignore[no-untyped-def]
        _write_sqlglot("tokenize")
        return real_tokenize(self, sql)

    def deepcopy(self, memo):  # type: ignore[no-untyped-def]
        _write_sqlglot("copy")
        return real_deepcopy(self, memo)

    Tokenizer.tokenize = tokenize  # type: ignore[method-assign]
    Expression.__deepcopy__ = deepcopy  # type: ignore[method-assign]


def install(path: str) -> None:
    """Open the tap file and wrap the source-driver and engine terminals."""
    global _out_fd
    _out_fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    from provisa.executor.pool import SourcePool
    from provisa.federation.runtime import EngineRuntime

    real_execute = SourcePool.execute
    real_open_stream = SourcePool.open_stream
    real_engine = EngineRuntime.execute_engine
    real_engine_sync = EngineRuntime.execute_engine_sync

    async def execute(self, source_id, sql, params=None):  # type: ignore[no-untyped-def]
        _write("source", sql, source_id, params)
        return await real_execute(self, source_id, sql, params)

    async def open_stream(self, source_id, sql, params=None):  # type: ignore[no-untyped-def]
        _write("source", sql, source_id, params)
        return await real_open_stream(self, source_id, sql, params)

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, params=None, **kwargs):  # type: ignore[no-untyped-def]
        _write("engine", sql, None, params)
        return await real_engine(self, sql, params, **kwargs)

    def execute_engine_sync(self, sql, params=None, **kwargs):  # type: ignore[no-untyped-def]
        _write("engine", sql, None, params)
        return real_engine_sync(self, sql, params, **kwargs)

    SourcePool.execute = execute  # type: ignore[method-assign]
    SourcePool.open_stream = open_stream  # type: ignore[method-assign]
    EngineRuntime.execute_engine = execute_engine  # type: ignore[method-assign]
    EngineRuntime.execute_engine_sync = execute_engine_sync  # type: ignore[method-assign]
    _install_sqlglot_tap()
