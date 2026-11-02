# Copyright (c) 2026 Kenneth Stott
# Canary: 40933671-f592-4164-b795-879fb6974e54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stand-in for the ADBC connection to the Zaychik proxy that Trino reads go through
(REQ-2194): it records what the proxy would receive -- each statement and the Trino session
(``x-provisa-trino-context``) it was sent with, in order -- and answers with a table typed as
the proxy types it."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pyarrow as pa

_HEADER_OPTION = "adbc.flight.sql.rpc.call_header.x-provisa-trino-context"


def proxy_table(columns: dict[str, tuple[str, list[Any]]]) -> pa.Table:
    """A table as the proxy sends it: ``{name: (trino type, values)}`` for bigint/varchar."""
    arrow = {"bigint": pa.int64(), "integer": pa.int32(), "varchar": pa.string()}
    fields, arrays = [], []
    for name, (trino_type, values) in columns.items():
        fields.append(
            pa.field(
                name,
                arrow[trino_type],
                metadata={
                    "trino.type": trino_type,
                    "trino.type_signature": json.dumps({"rawType": trino_type, "arguments": []}),
                    "trino.encoding": "arrow",
                },
            )
        )
        arrays.append(pa.array(values, arrow[trino_type]))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


class _Cursor:
    def __init__(self, proxy: ProxyConnection) -> None:
        self._proxy = proxy
        self.adbc_statement = SimpleNamespace(set_options=self._set_options)

    def _set_options(self, **options: str) -> None:
        session = json.loads(base64.b64decode(options[_HEADER_OPTION]))
        self._proxy.log.append(("session", session))
        self._proxy.sessions.append(session)

    def execute(self, sql: str, params: Any = None) -> None:
        assert params is None, "values are bound in the statement, never as driver params"
        self._proxy.log.append(("statement", sql))
        self._proxy.statements.append(sql)
        if self._proxy.on_execute is not None:
            self._proxy.on_execute(sql, self._proxy.sessions[-1] if self._proxy.sessions else None)

    def fetch_arrow_table(self) -> pa.Table:
        return self._proxy.table

    def fetch_record_batch(self) -> pa.RecordBatchReader:
        return self._proxy.table.to_reader()

    def adbc_cancel(self) -> None:
        pass

    def close(self) -> None:
        pass


class ProxyConnection:
    """Records ``statements``, ``sessions`` and both in ``log`` (in the order sent).
    ``on_execute(sql, session)`` may raise, as the proxy answers a failed statement."""

    def __init__(
        self,
        table: pa.Table | None = None,
        *,
        on_execute: Callable[[str, dict | None], None] | None = None,
    ) -> None:
        self.table = table if table is not None else proxy_table({"n": ("bigint", [1])})
        self.on_execute = on_execute
        self.statements: list[str] = []
        self.sessions: list[dict] = []
        self.log: list[tuple[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)


def backend_state(conn: ProxyConnection, **overrides: Any) -> SimpleNamespace:
    """What TrinoBackend reads off the app state for a shared-coordinator terminal."""
    state = SimpleNamespace(
        engine_conn=None,
        engine_conn_kwargs={"source": "provisa/org-1", "catalog": "system", "schema": "org_1"},
        engine_session_hints={},
        active_engine_endpoint=None,
        active_isolated_org=None,
        flight_client=conn,
    )
    for name, value in overrides.items():
        setattr(state, name, value)
    return state


def trino_backend():
    from provisa.federation.backend import TrinoBackend
    from provisa.federation.engine import build_trino_engine

    return TrinoBackend(build_trino_engine())
