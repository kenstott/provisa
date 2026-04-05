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
        role: Role name sent with every request (default: "admin").
        flight_port: Port of the Arrow Flight server (default: 8815).
    """

    def __init__(
        self,
        url: str = "http://localhost:8001",
        *,
        token: str | None = None,
        role: str = "admin",
        flight_port: int = 8815,
    ) -> None:
        self._base = url.rstrip("/")
        self._token = token
        self._role = role
        self._flight_port = flight_port

    # ── HTTP / GraphQL ────────────────────────────────────────────────────

    def _http_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json", "X-Role": self._role}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def query(
        self,
        gql: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query. Returns the raw response dict."""
        payload: dict[str, Any] = {"query": gql}
        if variables:
            payload["variables"] = variables
        r = httpx.post(
            f"{self._base}/data/graphql",
            json=payload,
            headers=self._http_headers(),
        )
        r.raise_for_status()
        return r.json()

    def query_df(
        self,
        gql: str,
        variables: dict[str, Any] | None = None,
    ):
        """Execute a GraphQL query. Returns a pandas DataFrame from the first root field."""
        import pandas as pd

        result = self.query(gql, variables)
        if "errors" in result:
            raise RuntimeError(result["errors"])
        root = next(iter(result.get("data", {}).values()))
        return pd.DataFrame(root)

    async def aquery(
        self,
        gql: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Async variant of query()."""
        payload: dict[str, Any] = {"query": gql}
        if variables:
            payload["variables"] = variables
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{self._base}/data/graphql",
                json=payload,
                headers=self._http_headers(),
            )
        r.raise_for_status()
        return r.json()

    # ── Arrow Flight ──────────────────────────────────────────────────────

    def _flight_client(self) -> fl.FlightClient:
        host = urlparse(self._base).hostname or "localhost"
        return fl.connect(f"grpc://{host}:{self._flight_port}")

    def _flight_ticket(self, gql: str, variables: dict[str, Any] | None) -> fl.Ticket:
        data: dict[str, Any] = {"query": gql, "role": self._role}
        if variables:
            data["variables"] = variables
        return fl.Ticket(json.dumps(data).encode())

    def flight(
        self,
        gql: str,
        variables: dict[str, Any] | None = None,
    ) -> pa.Table:
        """Execute a GraphQL query via Arrow Flight. Returns a pyarrow Table."""
        reader = self._flight_client().do_get(self._flight_ticket(gql, variables))
        return reader.read_all()

    def flight_df(
        self,
        gql: str,
        variables: dict[str, Any] | None = None,
    ):
        """Execute a GraphQL query via Arrow Flight. Returns a pandas DataFrame."""
        return self.flight(gql, variables).to_pandas()

    # ── Catalog / discovery ───────────────────────────────────────────────

    def list_tables(self):
        """List semantic layer tables (catalog mode). Returns a pandas DataFrame."""
        import pandas as pd

        criteria = json.dumps({"mode": "catalog"}).encode()
        infos = list(self._flight_client().list_flights(criteria))
        rows = []
        for info in infos:
            path = [p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path]
            rows.append({
                "schema_name": path[0] if len(path) > 0 else "",
                "table_name": path[1] if len(path) > 1 else "",
            })
        return pd.DataFrame(rows, columns=["schema_name", "table_name"])

    def list_approved(self):
        """List approved persisted queries. Returns a pandas DataFrame."""
        import pandas as pd

        criteria = json.dumps({"mode": "approved"}).encode()
        infos = list(self._flight_client().list_flights(criteria))
        rows = [
            {"stable_id": (p := [s.decode() if isinstance(s, bytes) else s for s in info.descriptor.path])[1] if len(p) > 1 else ""}
            for info in infos
        ]
        return pd.DataFrame(rows, columns=["stable_id"])
