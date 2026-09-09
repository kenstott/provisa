# Copyright (c) 2026 Kenneth Stott
# Canary: 93d1dfab-f49d-40b1-9d70-1fbeb6e86876
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Elasticsearch over HTTP, engine-independently (REQ-1672).

The Trino connector was the only reader of an Elasticsearch source; every other engine registered
the source and then had nothing to scan. This module is the native reader: index listing and
mapping for Register Table, and a scroll read of an index's documents for the landing path
(:func:`provisa.events.source_loader.make_elasticsearch_loader`). Synchronous httpx — callers run
it in a thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from provisa.elasticsearch.source import discover_schema, extract_mapping_properties

_SCROLL_KEEPALIVE = "2m"
_PAGE_SIZE = 1000


@dataclass(frozen=True)
class ESConnection:  # REQ-1672
    base_url: str
    auth: tuple[str, str] | None = None
    timeout: float = 60.0

    @classmethod
    def build(
        cls,
        host: str,
        port: int,
        *,
        tls: bool = False,
        username: str | None = None,
        password: str | None = None,
    ) -> "ESConnection":
        # A host given as a URL (the form's placeholder shows one) is taken as-is.
        base = (
            host
            if host.startswith(("http://", "https://"))
            else (f"{'https' if tls else 'http'}://{host}:{port}")
        )
        auth = (username, password) if username and password else None
        return cls(base_url=base.rstrip("/"), auth=auth)

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, auth=self.auth, timeout=self.timeout)


def list_indices(conn: ESConnection) -> list[str]:  # REQ-1672
    """User indices, sorted; system/hidden indices (``.``-prefixed) are not tables."""
    with conn._client() as c:
        resp = c.get("/_cat/indices", params={"format": "json", "h": "index"})
        resp.raise_for_status()
        return sorted(
            e["index"]
            for e in resp.json()
            if isinstance(e, dict) and not e["index"].startswith(".")
        )


def index_columns(conn: ESConnection, index: str) -> list[dict]:  # REQ-1672
    """The index's flattened mapping as ``{name, type, sourcePath}`` entries (``discover_schema``)."""
    with conn._client() as c:
        resp = c.get(f"/{index}/_mapping")
        resp.raise_for_status()
    return discover_schema(extract_mapping_properties(resp.json(), index))


def _pluck(doc: dict, path: str) -> Any:
    cur: Any = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def fetch_rows(
    conn: ESConnection, index: str, columns: list[tuple[str, str]]
) -> list[dict]:  # REQ-1672
    """Every document of ``index`` as a row of ``columns`` — ``(column name, source path)`` pairs —
    read through the scroll API so an index larger than one page comes back whole."""
    rows: list[dict] = []
    with conn._client() as c:
        resp = c.post(
            f"/{index}/_search",
            params={"scroll": _SCROLL_KEEPALIVE},
            json={"size": _PAGE_SIZE, "sort": ["_doc"], "query": {"match_all": {}}},
        )
        resp.raise_for_status()
        body = resp.json()
        scroll_id = body.get("_scroll_id")
        try:
            while True:
                hits = body.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for hit in hits:
                    src = hit.get("_source") or {}
                    rows.append({name: _pluck(src, path) for name, path in columns})
                if scroll_id is None:
                    break
                resp = c.post(
                    "/_search/scroll", json={"scroll": _SCROLL_KEEPALIVE, "scroll_id": scroll_id}
                )
                resp.raise_for_status()
                body = resp.json()
                scroll_id = body.get("_scroll_id", scroll_id)
        finally:
            if scroll_id is not None:
                c.request("DELETE", "/_search/scroll", json={"scroll_id": scroll_id})
    return rows


def table_index_and_columns(
    conn: ESConnection, mapping: dict, table_name: str, column_names: list[str]
) -> tuple[str, list[tuple[str, str]]]:  # REQ-1672
    """Resolve a registered table to its index and each registered column's document path.

    A ``mapping.tables`` entry (the type's mapping DSL, REQ-251) names the index and may give a
    column its ``path``; otherwise the table name IS the index and the live mapping supplies the
    path of every flattened column name (``discover_schema`` naming). A registered column the
    index cannot supply is an error, never a silently null column.
    """
    entry = next((t for t in mapping.get("tables", []) if t.get("name") == table_name), None)
    index = entry["index"] if entry else table_name
    declared = {c["name"]: c.get("path") or c["name"] for c in (entry or {}).get("columns", [])}
    live = {c["name"]: c["sourcePath"] for c in index_columns(conn, index)}
    resolved: list[tuple[str, str]] = []
    missing: list[str] = []
    for name in column_names:
        path = declared.get(name) or live.get(name)
        if path is None:
            missing.append(name)
        else:
            resolved.append((name, path))
    if missing:
        raise ValueError(
            f"Elasticsearch index {index!r} has no field for registered column(s) "
            f"{', '.join(missing)} of table {table_name!r}"
        )
    return index, resolved
