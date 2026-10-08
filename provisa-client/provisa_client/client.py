# Copyright (c) 2026 Kenneth Stott
# Canary: 411db4e5-adaa-4c0d-a65a-1dfc35c59897
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import httpx
import pyarrow as pa
import pyarrow.flight as fl


class ProvisaClient:
    """Client for Provisa GraphQL and Arrow Flight endpoints.

    Args:
        url: Base URL of the Provisa server (default: http://localhost:8001).
        token: Bearer token for authentication.
        role: The role to act as, sent as ``X-Provisa-Role`` (REQ-273). The server honours it
            only when the authenticated identity is assigned it. ``None`` (the default) names
            none, and the request runs as the identity's own role.
        org: The org the requests are for (REQ-1235). A multi-tenant deployment refuses a request
            that names none; a single-tenant deployment has no org to name, so leave it ``None``.
            Sent as ``X-Org-Provisa`` over HTTP and as ``org`` in an Arrow Flight ticket.
        flight_port: Port of the Arrow Flight server (default: 8815).
    """

    def __init__(
        self,
        url: str = "http://localhost:8001",
        *,
        token: str | None = None,
        role: str | None = None,
        org: str | None = None,
        flight_port: int = 8815,
    ) -> None:
        self._base = url.rstrip("/")
        self._token = token
        self._role = role
        self._org = org
        self._flight_port = flight_port

    # ── HTTP / GraphQL ────────────────────────────────────────────────────

    def _http_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        # REQ-273: the role header the server validates; sent only when a role was chosen.
        if self._role:
            headers["X-Provisa-Role"] = self._role
        # REQ-1235: the org this request is for; sent only when one was given.
        if self._org:
            headers["X-Org-Provisa"] = self._org
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL, SQL, or Cypher query. Returns the raw response dict."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        r = httpx.post(
            f"{self._base}/data/query",
            json=payload,
            headers=self._http_headers(),
        )
        r.raise_for_status()
        return r.json()

    def query_df(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ):
        """Execute a GraphQL, SQL, or Cypher query. Returns a pandas DataFrame.

        GraphQL responses (``{data: {field: [...]}}``): extracts the first root field.
        SQL/Cypher responses (``{columns: [...], rows: [...]}``) are mapped directly.
        """
        import pandas as pd

        result = self.query(query, variables)
        if "errors" in result:
            raise RuntimeError(result["errors"])
        if "columns" in result and "rows" in result:
            return pd.DataFrame(result["rows"], columns=result["columns"])
        root = next(iter(result.get("data", {}).values()))
        return pd.DataFrame(root)

    async def aquery(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Async variant of query(). Supports GraphQL, SQL, and Cypher."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{self._base}/data/query",
                json=payload,
                headers=self._http_headers(),
            )
        r.raise_for_status()
        return r.json()

    # ── Arrow Flight ──────────────────────────────────────────────────────

    def _flight_client(self) -> fl.FlightClient:  # pyright: ignore[reportPrivateImportUsage]
        host = urlparse(self._base).hostname or "localhost"
        return fl.connect(f"grpc://{host}:{self._flight_port}")  # pyright: ignore[reportPrivateImportUsage]

    def _flight_call_options(self) -> fl.FlightCallOptions:  # pyright: ignore[reportPrivateImportUsage]
        """The credential and role for a Flight call that carries no ticket (``list_flights``):
        the server reads them from the call's headers and lists the catalog as that role."""
        headers: list[tuple[bytes, bytes]] = []
        if self._token:
            headers.append((b"authorization", f"Bearer {self._token}".encode()))
        if self._role:
            headers.append((b"x-provisa-role", self._role.encode()))
        return fl.FlightCallOptions(headers=headers)  # pyright: ignore[reportPrivateImportUsage]

    def _flight_ticket(self, query: str, variables: dict[str, Any] | None) -> fl.Ticket:  # pyright: ignore[reportPrivateImportUsage]
        data: dict[str, Any] = {"query": query}
        if self._role:
            data["role"] = self._role
        if self._org:  # REQ-1235
            data["org"] = self._org
        if variables:
            data["variables"] = variables
        # REQ-1263: Flight authenticates every ticket. Without the credential here the HTTP path
        # would authenticate and the Flight path would be rejected by the same server.
        if self._token:
            data["token"] = self._token
        return fl.Ticket(json.dumps(data).encode())  # pyright: ignore[reportPrivateImportUsage]

    def flight(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> pa.Table:
        """Execute a GraphQL, SQL, or Cypher query via Arrow Flight. Returns a pyarrow Table."""
        reader = self._flight_client().do_get(self._flight_ticket(query, variables))
        return reader.read_all()

    def flight_df(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ):
        """Execute a GraphQL, SQL, or Cypher query via Arrow Flight. Returns a pandas DataFrame."""
        return self.flight(query, variables).to_pandas()

    # ── Catalog / discovery ───────────────────────────────────────────────

    def list_tables(self):
        """List semantic layer tables (catalog mode). Returns a pandas DataFrame."""
        import pandas as pd

        criteria = json.dumps({"mode": "catalog"}).encode()
        infos = list(self._flight_client().list_flights(criteria, self._flight_call_options()))
        rows = []
        for info in infos:
            path = [p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path]
            rows.append(
                {
                    "schema_name": path[0] if len(path) > 0 else "",
                    "table_name": path[1] if len(path) > 1 else "",
                }
            )
        return pd.DataFrame(rows, columns=["schema_name", "table_name"])
