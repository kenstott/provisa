# Copyright (c) 2026 Kenneth Stott
# Canary: 460798a4-b9d2-40a6-b160-3cb0d8452e3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ADBC interface backed by Provisa Arrow Flight (REQ-AK5)."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import httpx
import pyarrow as pa
import pyarrow.flight as fl


def _auth_login(base_url: str, user: str, password: str) -> str | None:
    """POST /auth/login and return token, or None on failure."""
    try:
        r = httpx.post(
            f"{base_url}/auth/login",
            json={"username": user, "password": password},
        )
        if r.status_code == 200:
            return r.json().get("token")
    except httpx.HTTPError:
        pass
    return None


def adbc_connect(
    url: str = "http://localhost:8001",
    *,
    user: str = "",
    password: str = "",
    mode: str = "approved",
) -> "AdbcConnection":
    """Create an ADBC-compatible connection backed by Arrow Flight."""
    base_url = url.rstrip("/")
    token = _auth_login(base_url, user, password)
    role = user if token is None else "admin"

    parsed = urlparse(base_url)
    host = parsed.hostname or "localhost"
    flight_client = fl.connect(f"grpc://{host}:8815")

    return AdbcConnection(
        flight_client=flight_client,
        role=role,
        token=token,
        base_url=base_url,
    )


class AdbcConnection:
    """ADBC connection interface backed by Arrow Flight."""

    def __init__(
        self,
        flight_client: fl.FlightClient,
        role: str,
        token: str | None,
        base_url: str,
    ) -> None:
        self._flight_client = flight_client
        self._role = role
        self._token = token
        self._base_url = base_url
        self._closed = False

    def cursor(self) -> "AdbcCursor":
        if self._closed:
            raise RuntimeError("Connection is closed")
        return AdbcCursor(connection=self)

    def close(self) -> None:
        self._closed = True
        try:
            self._flight_client.close()
        except Exception:
            pass

    def __enter__(self) -> "AdbcConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class AdbcCursor:
    """ADBC cursor that streams Arrow RecordBatches via Flight doGet."""

    def __init__(self, *, connection: AdbcConnection) -> None:
        self._conn = connection
        self._stream: fl.FlightStreamReader | None = None
        self._table: pa.Table | None = None
        self._rows: list[tuple] | None = None
        self._pos: int = 0
        self._closed = False

    def _build_ticket(self, query: str) -> fl.Ticket:
        data = {"query": query, "role": self._conn._role}
        if self._conn._token:
            data["token"] = self._conn._token
        return fl.Ticket(json.dumps(data).encode())

    def execute(self, query: str, parameters: Any = None) -> None:
        if self._closed:
            raise RuntimeError("Cursor is closed")
        if self._conn._closed:
            raise RuntimeError("Connection is closed")
        ticket = self._build_ticket(query)
        self._stream = self._conn._flight_client.do_get(ticket)
        self._table = None
        self._rows = None
        self._pos = 0

    def _ensure_table(self) -> pa.Table:
        if self._table is None:
            if self._stream is None:
                raise RuntimeError("No query has been executed")
            self._table = self._stream.read_all()
        return self._table

    def fetch_arrow_table(self) -> pa.Table:
        """Read all RecordBatches from stream and return as a pyarrow Table."""
        return self._ensure_table()

    def _ensure_rows(self) -> list[tuple]:
        if self._rows is None:
            tbl = self._ensure_table()
            pydict = tbl.to_pydict()
            columns = list(pydict.keys())
            n = tbl.num_rows
            self._rows = [
                tuple(pydict[col][i] for col in columns) for i in range(n)
            ]
        return self._rows

    def fetchone(self) -> tuple | None:
        rows = self._ensure_rows()
        if self._pos >= len(rows):
            return None
        row = rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self) -> list[tuple]:
        rows = self._ensure_rows()
        result = rows[self._pos:]
        self._pos = len(rows)
        return result

    @property
    def description(self) -> list[tuple] | None:
        if self._table is None and self._stream is None:
            return None
        tbl = self._ensure_table()
        schema = tbl.schema
        return [
            (field.name, None, None, None, None, None, None)
            for field in schema
        ]

    def close(self) -> None:
        self._closed = True
        self._stream = None

    def __enter__(self) -> "AdbcCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
