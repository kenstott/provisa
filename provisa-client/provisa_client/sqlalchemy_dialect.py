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

from typing import Any

import httpx
import sqlalchemy.types as sqltypes
from sqlalchemy.engine.default import DefaultDialect


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

    @classmethod
    def dbapi(cls):
        from provisa_client import dbapi as _dbapi
        return _dbapi

    @classmethod
    def import_dbapi(cls):
        from provisa_client import dbapi as _dbapi
        return _dbapi

    def create_connect_args(self, url: Any) -> tuple[list, dict]:
        scheme = url.drivername.split("+")[1] if "+" in url.drivername else "http"
        port = url.port or 8001
        opts: dict[str, Any] = {
            "url": f"{scheme}://{url.host}:{port}",
            "username": url.username or "",
            "password": url.password or "",
            "role": (url.query or {}).get("role", "admin"),
            "mode": (url.query or {}).get("mode", "approved"),
        }
        return [], opts

    def _get_base_url_and_role(self, connection: Any) -> tuple[str, str]:
        """Extract base_url and role from a live DBAPI connection."""
        if not hasattr(connection, "connection"):
            return "http://localhost:8001", "admin"
        raw = connection.connection
        # raw may be a Connection or wrapped by SQLAlchemy
        if hasattr(raw, "_role"):
            return raw._base_url, raw._role
        if hasattr(raw, "connection") and hasattr(raw.connection, "_role"):
            return raw.connection._base_url, raw.connection._role
        return "http://localhost:8001", "admin"

    _TIMEOUT = 10.0  # seconds

    def get_table_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        base_url, role = self._get_base_url_and_role(connection)
        try:
            r = httpx.post(
                f"{base_url}/admin/graphql",
                json={"query": "{ persistedQueries { stableId status } }"},
                headers={"Content-Type": "application/json", "X-Role": role},
                timeout=self._TIMEOUT,
            )
            r.raise_for_status()
            body = r.json()
            queries = body.get("data", {}).get("persistedQueries", [])
            return [q["stableId"] for q in queries if isinstance(q, dict) and q.get("stableId")]
        except (httpx.HTTPError, httpx.TimeoutException, KeyError):
            return []

    def get_columns(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[dict]:
        base_url, role = self._get_base_url_and_role(connection)
        query = """
        {
          semanticModel {
            tables {
              name
              columns {
                name
                dataType
              }
            }
          }
        }
        """
        try:
            r = httpx.post(
                f"{base_url}/admin/graphql",
                json={"query": query},
                headers={"Content-Type": "application/json", "X-Role": role},
                timeout=self._TIMEOUT,
            )
            r.raise_for_status()
            body = r.json()
            tables = body.get("data", {}).get("semanticModel", {}).get("tables", [])
            for table in tables:
                if table.get("name") == table_name:
                    return [
                        {
                            "name": col["name"],
                            "type": sqltypes.String(),
                            "nullable": True,
                        }
                        for col in table.get("columns", [])
                    ]
        except (httpx.HTTPError, httpx.TimeoutException, KeyError):
            pass
        return []

    def has_table(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> bool:
        return table_name in self.get_table_names(connection, schema=schema)

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any = None,
    ) -> None:
        cursor.execute(statement, parameters or None)

    def _check_unicode_returns(self, connection: Any, additional_tests: Any = None) -> bool:
        return True

    def _check_unicode_description(self, connection: Any) -> bool:
        return True

    def get_schema_names(self, connection: Any, **kw: Any) -> list[str]:
        return ["default"]

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

    def get_pk_constraint(
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
