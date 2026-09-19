# Copyright (c) 2026 Kenneth Stott
# Canary: 4f9c2e17-8a63-4d05-b1e7-9c3a6f2d81b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Apache Druid over its broker's plain SQL-over-HTTP endpoint, engine-independently (REQ-1730).

Trino's own connector (TrinoDruidConnector, trino_connectors.py) reads Druid via its broker's
Avatica JDBC endpoint. This module is the native reader used when the active engine has no live
Druid ATTACH connector of its own (every engine but Trino today) — the SAME broker HTTP port
(``Source.port``, Trino's own ``jdbc_url()`` target) also serves a plain SQL endpoint
(``POST /druid/v2/sql``) and Druid's own ANSI-ish ``INFORMATION_SCHEMA``, so — unlike Pinot — no
separate controller/broker address split is needed here: one host:port answers everything."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

_TIMEOUT = 30.0

# Druid's SQL type names (INFORMATION_SCHEMA.COLUMNS.DATA_TYPE) are already close to a coarse
# SQL-ish vocabulary — passed through as-is (uppercased) rather than remapped, unlike Pinot's own
# non-SQL type names.


@dataclass(frozen=True)
class DruidConnection:
    base_url: str

    @classmethod
    def build(cls, host: str | None, port: int | None) -> "DruidConnection":
        return cls(base_url=f"http://{host or 'localhost'}:{port or 8082}".rstrip("/"))


def _sql(conn: DruidConnection, query: str) -> list[dict]:
    with httpx.Client(timeout=_TIMEOUT) as c:
        resp = c.post(f"{conn.base_url}/druid/v2/sql", json={"query": query})
        resp.raise_for_status()
        return resp.json()


def list_tables(conn: DruidConnection) -> list[str]:
    """Every datasource in the ``druid`` schema, sorted — Druid's own INFORMATION_SCHEMA.TABLES,
    the same catalog Trino's connector itself reads for table listing."""
    rows = _sql(
        conn,
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'",
    )
    return sorted(r["TABLE_NAME"] for r in rows)


def table_columns(conn: DruidConnection, table: str) -> list[dict]:
    """``[{"name", "type"}]`` for `table`, from Druid's own INFORMATION_SCHEMA.COLUMNS."""
    safe_table = table.replace("'", "''")
    rows = _sql(
        conn,
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '{safe_table}' "
        "ORDER BY ORDINAL_POSITION",
    )
    return [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"]} for r in rows]


def fetch_rows(conn: DruidConnection, table: str, columns: list[str]) -> list[dict]:
    """Every current row of `table`'s given columns (or every column when none are given)."""
    safe_table = table.replace('"', '""')
    select = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    return _sql(conn, f'SELECT {select} FROM "druid"."{safe_table}"')
