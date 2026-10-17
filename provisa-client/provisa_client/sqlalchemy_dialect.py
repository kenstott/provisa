# Copyright (c) 2026 Kenneth Stott
# Canary: a0be43a1-5ab0-429a-88b9-5b6f94e0be82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLAlchemy dialect for Provisa (REQ-AK4).

URL schemes: provisa+http:// and provisa+https://

Example:
    engine = create_engine("provisa+http://user:pass@localhost:8001")
"""

from __future__ import annotations

import base64
from typing import Any

import httpx
import pyarrow as pa
import sqlalchemy.types as sqltypes
from sqlalchemy import util
from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError


class ProvisaDialect(DefaultDialect):
    """SQLAlchemy dialect backed by the Provisa DB-API 2.0 driver."""

    name = "provisa"
    driver = "provisa_client"
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        from provisa_client import dbapi as _dbapi

        return _dbapi

    @classmethod
    def import_dbapi(cls):  # pyright: ignore[reportIncompatibleMethodOverride]
        from provisa_client import dbapi as _dbapi

        return _dbapi

    def create_connect_args(self, url: Any) -> tuple[list, dict]:
        scheme = url.drivername.split("+")[1] if "+" in url.drivername else "http"
        port = url.port or 8001
        query = url.query or {}
        opts: dict[str, Any] = {
            "url": f"{scheme}://{url.host}:{port}",
            "username": url.username or "",
            "password": url.password or "",
        }
        if "role" in query:
            opts["role"] = query["role"]
        # REQ-1235: ?org=<id> names the org the connection's requests are for.
        if "org" in query:
            opts["org"] = query["org"]
        # REQ-691: client-side decryption params passed through to the DB-API connect().
        if "kms_provider" in query:
            opts["kms_provider"] = query["kms_provider"]
        if "kms_key_arn" in query:
            opts["kms_key_arn"] = query["kms_key_arn"]
        if "dek_cache_ttl" in query:
            opts["dek_cache_ttl"] = float(query["dek_cache_ttl"])
        return [], opts

    _TIMEOUT = 10.0  # seconds

    @staticmethod
    def _dbapi_connection(connection: Any) -> Any:
        """The Provisa DB-API connection under a SQLAlchemy connection: reflection reads the
        catalog as its user, role and org."""
        from provisa_client.dbapi import Connection

        raw = connection.connection.dbapi_connection
        if not isinstance(raw, Connection):
            raise TypeError(f"{type(raw).__name__} is not a Provisa DB-API connection")
        return raw

    @reflection.cache
    def _catalog(self, connection: Any, **_kw: Any) -> dict[str, dict[str, pa.Schema]]:
        """The catalog the connection's role is served (``GET /data/catalog``), as
        {schema: {table: Arrow schema}}. A table's schema is its domain, as SQL addresses it
        (``<domain>.<table>``). Cached for one inspection (SQLAlchemy's ``info_cache``)."""
        raw = self._dbapi_connection(connection)
        r = httpx.get(
            f"{raw._base_url}/data/catalog", headers=raw._headers(), timeout=self._TIMEOUT
        )
        r.raise_for_status()
        out: dict[str, dict[str, pa.Schema]] = {}
        for table in r.json()["tables"]:
            domain, name = table["path"]
            schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(table["schema"])))
            out.setdefault(domain, {})[name] = schema
        return out

    def get_schema_names(self, connection: Any, **kw: Any) -> list[str]:
        return sorted(self._catalog(connection, **kw))

    def get_table_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        """The tables of ``schema``. Every Provisa table lives in a domain's schema and there is
        no default one, so naming none lists none."""
        if schema is None:
            return []
        return sorted(self._catalog(connection, **kw).get(schema, {}))

    def get_columns(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[dict]:
        arrow = self._catalog(connection, **kw).get(schema or "", {}).get(table_name)
        if arrow is None:
            raise NoSuchTableError(f"{schema}.{table_name}")
        return [
            {
                "name": field.name,
                "type": _sql_type(field),
                "nullable": field.nullable,
                "comment": (field.metadata or {}).get(b"description", b"").decode() or None,
            }
            for field in arrow
        ]

    def has_table(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **_kw: Any,
    ) -> bool:
        return table_name in self.get_table_names(connection, schema=schema)

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any = None,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        cursor.execute(statement, parameters or None)

    def _check_unicode_returns(self, _connection: Any, _additional_tests: Any = None) -> bool:
        return True

    def _check_unicode_description(self, _connection: Any) -> bool:
        return True

    def get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list:
        return []

    def get_indexes(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list:
        return []

    def get_pk_constraint(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> dict:
        return {"constrained_columns": [], "name": None}

    def get_unique_constraints(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list:
        return []


def _sql_type(field: pa.Field) -> Any:
    """The SQLAlchemy type of a catalog column's Arrow type."""
    t = field.type
    if pa.types.is_boolean(t):
        return sqltypes.Boolean()
    if pa.types.is_int8(t) or pa.types.is_int16(t):
        return sqltypes.SmallInteger()
    if pa.types.is_int32(t):
        return sqltypes.Integer()
    if pa.types.is_int64(t):
        return sqltypes.BigInteger()
    if pa.types.is_floating(t):
        return sqltypes.Float()
    if pa.types.is_decimal(t):
        return sqltypes.Numeric(precision=t.precision, scale=t.scale)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return sqltypes.String()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return sqltypes.LargeBinary()
    if pa.types.is_date(t):
        return sqltypes.Date()
    if pa.types.is_timestamp(t):
        return sqltypes.DateTime(timezone=t.tz is not None)
    if pa.types.is_time(t):
        return sqltypes.Time()
    if pa.types.is_duration(t):
        return sqltypes.Interval()
    # SQLAlchemy's own type for a reflected column it has no type for; said, as its dialects do.
    util.warn(f"column {field.name!r}: no SQLAlchemy type for Arrow type {t}")
    return sqltypes.NullType()
