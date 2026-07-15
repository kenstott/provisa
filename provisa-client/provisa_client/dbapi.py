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

import re
from typing import Any

import httpx

from provisa_client.encryption import ClientEncryptionService, build_client_encryption

apilevel = "2.0"
threadsafety = 1
paramstyle = "named"


# ── Exceptions (PEP 249) ──────────────────────────────────────────────────────


class Warning(Exception):  # noqa: A001
    """Exception raised for important warnings."""


class Error(Exception):
    """Base DB-API exception."""


class InterfaceError(Error):
    """Error related to the database interface."""


class DatabaseError(Error):
    """Database-related error."""


class DataError(DatabaseError):
    """Error due to problems with the processed data."""


class OperationalError(DatabaseError):
    """Operational database error (connection, auth, etc.)."""


class IntegrityError(DatabaseError):
    """Error raised when the relational integrity of the database is affected."""


class InternalError(DatabaseError):
    """Internal database error."""


class ProgrammingError(DatabaseError):
    """Programming error (bad query syntax, etc.)."""


class NotSupportedError(DatabaseError):
    """Method or database API not supported."""


# ── Helpers ───────────────────────────────────────────────────────────────────

_GQL_RE = re.compile(r"^\s*(\{|query\b|mutation\b)", re.IGNORECASE)


def _is_graphql(query: str) -> bool:
    return bool(_GQL_RE.match(query))


def _encrypted_columns(body: Any) -> list[str]:
    """Read the server's encrypted-column metadata flag from a response body (REQ-691)."""
    if isinstance(body, dict):
        cols = body.get("encrypted_columns")
        if isinstance(cols, list):
            return [c for c in cols if isinstance(c, str)]
    return []


_TIMEOUT = 10.0


def _auth_login(base_url: str, username: str, password: str) -> tuple[str | None, str | None]:
    """POST /auth/login and return (token, role), or (None, None) on failure."""
    try:
        r = httpx.post(
            f"{base_url}/auth/login",
            json={"username": username, "password": password},
            timeout=_TIMEOUT,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("token"), body.get("role")
    except (httpx.HTTPError, httpx.TimeoutException):
        pass
    return None, None


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
    role: str | None = None,
    kms_provider: str | None = None,
    kms_key_arn: str | None = None,
    dek_cache_ttl: float = 300.0,
    _kms_client: Any = None,
) -> "Connection":
    """Create a DB-API 2.0 connection to a Provisa server.

    REQ-691: when ``kms_provider`` and ``kms_key_arn`` are supplied, result columns
    the server flags encrypted are decrypted client-side with a DEK cached for
    ``dek_cache_ttl`` seconds. ``_kms_client`` injects a KMS SDK client (tests).
    """
    token, auth_role = _auth_login(url, username, password)
    if token:
        resolved_role: str | None = role or auth_role
    else:
        resolved_role = role or (
            username if username else None
        )  # design: username is role for unauthed access (REQ-AK5)
    encryption = build_client_encryption(
        kms_provider=kms_provider,
        kms_key_arn=kms_key_arn,
        dek_cache_ttl=dek_cache_ttl,
        _client=_kms_client,
    )
    return Connection(
        base_url=url.rstrip("/"),
        token=token,
        role=resolved_role,
        encryption=encryption,
        kms_key_arn=kms_key_arn,
    )


class Connection:
    """PEP 249 Connection."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str | None,
        role: str | None = None,
        encryption: ClientEncryptionService | None = None,
        kms_key_arn: str | None = None,
    ) -> None:
        self._base_url = base_url
        self._token = token
        self._role = role
        self._encryption = encryption  # REQ-691: client-side column decrypt (or None)
        self._kms_key_arn = kms_key_arn  # REQ-693: proof-of-client-decrypt for high-security gate
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise OperationalError("Connection is closed")

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        # REQ-273: server-validated requested role; sent only when explicitly chosen.
        if self._role:
            h["X-Provisa-Role"] = self._role
        # REQ-693: signal client-side decryption so high-security mode admits this connection.
        if self._kms_key_arn:
            h["X-Provisa-KMS-Key"] = self._kms_key_arn
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

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
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
                timeout=_TIMEOUT,
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
        self._set_rows(rows_raw, _encrypted_columns(body))

    def _execute_sql(self, query: str) -> None:
        try:
            r = httpx.post(
                f"{self._conn._base_url}/data/sql",
                json={"sql": query, "role": self._conn._role},
                headers=self._conn._headers(),
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
        except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
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
        enc_cols = _encrypted_columns(body) if isinstance(body, dict) else []
        self._set_rows(rows_raw, enc_cols)

    def _set_rows(self, rows_raw: list[dict], encrypted_columns: list[str] | None = None) -> None:
        encrypted_columns = encrypted_columns or []
        if not rows_raw:
            self._rows = []
            self.description = []
            self.rowcount = 0
            self._pos = 0
            return
        if isinstance(rows_raw[0], dict):
            if encrypted_columns:
                svc = self._conn._encryption
                if svc is None:
                    raise DataError(
                        "server flagged encrypted columns but no kms_provider/kms_key_arn "
                        "was configured on this connection (REQ-691)"
                    )
                from provisa_client.encryption import decrypt_rows

                decrypt_rows(rows_raw, encrypted_columns, svc)
            columns = list(rows_raw[0].keys())
            self.description = [(col, None, None, None, None, None, None) for col in columns]
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
        rows = self._rows[self._pos : self._pos + n]
        self._pos += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        self._check_open()
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
