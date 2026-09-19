# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e9a34-5b82-4f61-9d3a-2e08c6f47a91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Apache Pinot over its REST APIs, engine-independently (REQ-1730).

Trino's own connector (TrinoPinotConnector, trino_connectors.py) was the only reader of a Pinot
source. This module is the native reader, used when the active engine has no live Pinot ATTACH
connector of its own (every engine but Trino today): table listing over the controller's REST API
(``GET /tables``), and both column discovery and row fetch over the broker's SQL-over-HTTP query
API (``POST /query/sql``) — a table's own ``dataSchema`` in the query response IS its column list,
so no separate metadata call is needed.

Controller vs broker: Trino's connector needs only the CONTROLLER address (it discovers brokers
itself); a plain HTTP client reading rows needs the BROKER's own query endpoint directly. Pinot
has no single "front door" that serves both roles, and the controller's own broker-discovery
response returns each broker's self-registered network identity, which is a container-internal
address in this project's own docker-networked test fixtures (see demo/sources/pinot/compose.yml)
and useless outside that same network. Rather than depend on that discovery result, this always
prefers an explicit ``broker_url`` (``Source.federation_hints["pinot_broker_url"]``) when one is
set, falling back to the controller's own host on Pinot's default broker port (8099) — correct for
a real single-broker/dev deployment, and the same "explicit hint overrides a same-host guess"
shape kafka's schema-registry-URL resolution already uses (REQ-1730's own kafka fix)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

# Pinot's docs cite 8099 as the broker's default query port, but a plain `QuickStart` container's
# own bundled config binds it to 8000 instead (confirmed live: `docker logs` shows "Started
# listener bound to [0.0.0.0:8000]" for the broker specifically, and a direct query against 8099
# gets connection-refused while 8000 answers). This fallback only matters when no explicit
# `pinot_broker_url` hint is set — a real deployment should set one rather than rely on a guess.
_DEFAULT_BROKER_PORT = 8000
_TIMEOUT = 30.0

# Pinot's own column-type vocabulary (from a query response's dataSchema.columnDataTypes) mapped
# to the same coarse SQL-ish type names this codebase's other native `fetch.py` readers use
# (elasticsearch/prometheus) — good enough for the Register Table column-type dropdown; the exact
# value is not otherwise load-bearing (the write face maps IR types, not these, onto the store).
_TYPE_MAP: dict[str, str] = {
    "INT": "INTEGER",
    "LONG": "BIGINT",
    "FLOAT": "REAL",
    "DOUBLE": "DOUBLE",
    "BIG_DECIMAL": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "STRING": "VARCHAR",
    "JSON": "VARCHAR",
    "BYTES": "VARBINARY",
}


@dataclass(frozen=True)
class PinotConnection:
    controller_url: str
    broker_url: str

    @classmethod
    def build(
        cls,
        controller_host: str | None,
        controller_port: int | None,
        broker_url: str | None = None,
    ) -> "PinotConnection":
        host = controller_host or "localhost"
        controller_url = f"http://{host}:{controller_port or 9000}"
        resolved_broker = (broker_url or f"http://{host}:{_DEFAULT_BROKER_PORT}").rstrip("/")
        return cls(controller_url=controller_url.rstrip("/"), broker_url=resolved_broker)


def list_tables(conn: PinotConnection) -> list[str]:
    """Every table (Pinot calls them "tables") the controller knows, sorted."""
    with httpx.Client(timeout=_TIMEOUT) as c:
        resp = c.get(f"{conn.controller_url}/tables")
        resp.raise_for_status()
        return sorted(resp.json().get("tables", []))


def _query(conn: PinotConnection, sql: str) -> dict[str, Any]:
    with httpx.Client(timeout=60.0) as c:
        resp = c.post(f"{conn.broker_url}/query/sql", json={"sql": sql})
        resp.raise_for_status()
        body = resp.json()
    exceptions = body.get("exceptions")
    if exceptions:
        raise ValueError(f"Pinot query {sql!r} failed: {exceptions}")
    return body


def table_columns(conn: PinotConnection, table: str) -> list[dict]:
    """``[{"name", "type"}]`` for `table`, from the controller's own schema definition
    (``GET /tables/{table}/schema`` — dimension, date-time, and metric field specs). Reading the
    declared schema instead of a live query's own ``dataSchema`` (this function's first, now-
    reverted implementation) avoids paying for a real broker/segment scan just to discover column
    names — confirmed live to matter: a wide table (airlineStats, 80+ columns) occasionally took
    long enough under load to blow the Register Table form's 30s column-population wait, even
    though the query itself always eventually succeeded."""
    with httpx.Client(timeout=_TIMEOUT) as c:
        resp = c.get(f"{conn.controller_url}/tables/{table}/schema")
        resp.raise_for_status()
        spec = resp.json()
    fields = (
        spec.get("dimensionFieldSpecs", [])
        + spec.get("dateTimeFieldSpecs", [])
        + spec.get("metricFieldSpecs", [])
    )
    # A multi-value Pinot column (schema's own "singleValueField": false) returns a JSON array
    # per row, not a scalar — Provisa's landing write face has no array IR type, and attempting to
    # land one raised a real write-side error that a best-effort landing boundary silently
    # swallowed into "0 rows landed" (reproduced live: airlineStats' own DivAirportIDs/DivAirports/
    # etc. are all multi-value; excluding them was the fix, not a workaround for a Provisa bug).
    return [
        {"name": f["name"], "type": _TYPE_MAP.get(f["dataType"], "VARCHAR")}
        for f in fields
        if f.get("singleValueField", True)
    ]


_MAX_ROWS = 1_000_000


def fetch_rows(conn: PinotConnection, table: str, columns: list[str]) -> list[dict]:
    """Every current row of `table`'s given columns (or every column when none are given).

    Pinot's SQL endpoint defaults to ``LIMIT 10`` when a query names none at all — reproduced
    live: a plain unlimited SELECT against a 9746-row table came back with exactly 10 rows, no
    error or truncation notice. An explicit high ceiling is required for a landing read to see
    the table's real current state rather than an arbitrary first-10 slice."""
    select = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    body = _query(conn, f'SELECT {select} FROM "{table}" LIMIT {_MAX_ROWS}')
    result = body.get("resultTable", {})
    names = result.get("dataSchema", {}).get("columnNames", [])
    return [dict(zip(names, row, strict=False)) for row in result.get("rows", [])]
