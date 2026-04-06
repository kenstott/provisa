# Copyright (c) 2026 Kenneth Stott
# Canary: 740cab28-f527-46c0-8cde-c262d701fe49
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PEP 249 DB-API 2.0 interface for Provisa (REQ-AK3)."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

apilevel = "2.0"
threadsafety = 1
paramstyle = "named"


# ── Exceptions ────────────────────────────────────────────────────────────────

class Error(Exception):
    """Base DB-API exception."""


class DatabaseError(Error):
    """Database-related error."""


class OperationalError(DatabaseError):
    """Operational database error (connection, auth, etc.)."""


class ProgrammingError(DatabaseError):
    """Programming error (bad query syntax, etc.)."""


# ── Helpers ───────────────────────────────────────────────────────────────────

_GQL_RE = re.compile(r"^\s*(\{|query\b|mutation\b)", re.IGNORECASE)


def _is_graphql(query: str) -> bool:
    return bool(_GQL_RE.match(query))


def _auth_login(base_url: str, username: str, password: str) -> str | None:
    """POST /auth/login and return token, or None on failure."""
    try:
        r = httpx.post(
            f"{base_url}/auth/login",
            json={"username": username, "password": password},
        )
        if r.status_code == 200:
            return r.json().get("token")
    except httpx.HTTPError:
        pass
    return None


def _apply_parameters(query: str, parameters: dict | None) -> str:
    """Substitute :name placeholders with values (named paramstyle)."""
    if not parameters:
        return query
    for key, value in parameters.items():
        placeholder = f":{key}"
        if isinstance(value, str):
            replacement = f"'{value}'"
        else:
            replacement = str(value)
        query = query.replace(placeholder, replacement)
    return query


# ── Connection ────────────────────────────────────────────────────────────────

def connect(
    url: str,
    *,
    username: str,
    password: str,
    role: str = "admin",
    mode: str = "approved",
) -> "Connection":
    """Create a DB-API 2.0 connection to a Provisa server."""
    token = _auth_login(url, username, password)
    if token is None:
        # Fall back to username as role (test mode)
        effective_role = username if username else role
    else:
        effective_role = role
    return Connection(
        base_url=url.rstrip("/"),
        token=token,
        role=effective_role,
        mode=mode,
    )


class Connection:
    """PEP 249 Connection."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str | None,
        role: str,
        mode: str,
    ) -> None:
        self._base_url = base_url
        self._token = token
        self._role = role
        self._mode = mode
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise OperationalError("Connection is closed")

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "X-Role": self._role}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        return h

    def cursor(self) -> "Cursor":
        self._check_open()
        return Cursor(connection=self)

    def close(self) -> None:
        self._closed = True

    def commit(self) -> None:
        # read-only — no-op
        pass

    def rollback(self) -> None:
        # read-only — no-op
        pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ── Cursor ────────────────────────────────────────────────────────────────────

class Cursor:
    """PEP 249 Cursor."""

    arraysize: int = 1

    def __init__(self, *, connection: Connection) -> None:
        self._conn = connection
        self._rows: list[tuple] = []
        self._pos: int = 0
        self.description: list[tuple] | None = None
        self.rowcount: int = -1
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise OperationalError("Cursor is closed")

    def execute(self, query: str, parameters: dict | None = None) -> None:
        self._check_open()
        self._conn._check_open()
        query = _apply_parameters(query, parameters)
        if _is_graphql(query):
            self._execute_graphql(query)
        else:
            self._execute_sql(query)

    def _execute_graphql(self, query: str) -> None:
        try:
            r = httpx.post(
                f"{self._conn._base_url}/data/graphql",
                json={"query": query, "role": self._conn._role},
                headers=self._conn._headers(),
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise OperationalError(str(exc)) from exc
        body = r.json()
        if "errors" in body:
            raise ProgrammingError(str(body["errors"]))
        data = body.get("data", {})
        root_field = next(iter(data), None)
        if root_field is None:
            self._rows = []
            self.description = []
            self.rowcount = 0
            return
        rows_raw = data[root_field]
        if not isinstance(rows_raw, list):
            rows_raw = [rows_raw]
        self._set_rows(rows_raw)

    def _execute_sql(self, query: str) -> None:
        try:
            r = httpx.post(
                f"{self._conn._base_url}/data/sql",
                json={"sql": query, "role": self._conn._role},
                headers=self._conn._headers(),
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise OperationalError(str(exc)) from exc
        body = r.json()
        # Handle {"data": {"_sql": [...]}} or just a list
        if isinstance(body, list):
            rows_raw = body
        elif isinstance(body, dict):
            data = body.get("data", body)
            if isinstance(data, list):
                rows_raw = data
            elif isinstance(data, dict):
                inner = next(iter(data.values()), [])
                rows_raw = inner if isinstance(inner, list) else [inner]
            else:
                rows_raw = []
        else:
            rows_raw = []
        self._set_rows(rows_raw)

    def _set_rows(self, rows_raw: list[dict]) -> None:
        if not rows_raw:
            self._rows = []
            self.description = []
            self.rowcount = 0
            self._pos = 0
            return
        if isinstance(rows_raw[0], dict):
            columns = list(rows_raw[0].keys())
            self.description = [
                (col, None, None, None, None, None, None) for col in columns
            ]
            self._rows = [tuple(row.get(c) for c in columns) for row in rows_raw]
        else:
            # Already tuples/lists
            self.description = None
            self._rows = [tuple(r) for r in rows_raw]
        self.rowcount = len(self._rows)
        self._pos = 0

    def executemany(self, query: str, seq_of_params: list) -> None:
        self._check_open()
        for params in seq_of_params:
            self.execute(query, params)

    def fetchone(self) -> tuple | None:
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        self._check_open()
        n = size if size is not None else self.arraysize
        rows = self._rows[self._pos: self._pos + n]
        self._pos += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        self._check_open()
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
