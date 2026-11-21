# Copyright (c) 2026 Kenneth Stott
# Canary: 4eb4d8c8-3bd5-4ee1-bc85-5a9f9ecaaf56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2265: where a governance catalog and Provisa's model disagree is itself a query.

A real server (DuckDB engine, its own control plane) holds a PostgreSQL table of customers whose
phone is masked and whose email is not, and an OpenMetadata catalog read live as an OpenAPI
source: the recorded OpenMetadata spec, and an HTTP API the test serves that answers the
catalog's tables in two pages, the second reached by the cursor nested in the first
(``paging.after``). The catalog classifies both email and phone as personal data. One governed
SQL query joins the catalog's column tags to Provisa's own column masks (``meta.table_columns``)
and answers the column the catalog calls personal and Provisa does not mask: email.
"""

# Requirements: REQ-2265

from __future__ import annotations

import gzip
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import pytest_asyncio
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_ORG = "catalog_brand_drift"
_SCHEMA = "drift_e2e"
_ROLE = "org_admin"
_SPEC = Path(__file__).parents[1] / "fixtures/catalog_brands/openmetadata_swagger.json.gz"
_PII = {"tagFQN": "PII.Sensitive", "source": "Classification", "labelType": "Manual"}


def _om_table(name: str, columns: dict[str, list[dict]], after: str | None) -> dict:
    page = {
        "data": [
            {
                "id": f"id-{name}",
                "name": name,
                "fullyQualifiedName": f"warehouse.shop.{_SCHEMA}.{name}",
                "columns": [
                    {"name": column, "dataType": "VARCHAR", "tags": tags}
                    for column, tags in columns.items()
                ],
                "tags": [],
            }
        ],
        "paging": {"total": 2},
    }
    if after is not None:
        page["paging"]["after"] = after
    return page


# The catalog's tables, a page each: the first names where the second starts.
_PAGES = {
    None: _om_table("customers", {"id": [], "email": [_PII], "phone": [_PII]}, "cGFnZS0y"),
    "cGFnZS0y": _om_table("orders", {"id": [], "total": []}, None),
}


class _OpenMetadata:
    """The catalog's API: /api/v1/tables, paged by the ``after`` cursor; every query it is
    asked is kept."""

    def __init__(self) -> None:
        asked: list[dict] = []

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's handler name
                url = urlsplit(self.path)
                query = {k: v[0] for k, v in parse_qs(url.query).items()}
                asked.append(query)
                if url.path != "/api/v1/tables" or query.get("after") not in _PAGES:
                    self.send_response(404)
                    self.end_headers()
                    return
                body = json.dumps(_PAGES[query.get("after")]).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 - http.server's name
                pass

        self.asked = asked
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()


def _config(spec_path: str, api: str) -> dict:
    visible = {"visible_to": [_ROLE]}
    masked = [{"to": None, "mask": {"type": "regex", "pattern": ".+", "replace": "***"}}]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [
            {"id": "shop", "description": "The shop"},
            {"id": "catalog", "description": "The governance catalog"},
        ],
        "roles": [],
        "sources": [
            {
                "id": "shop-pg",
                "type": "postgresql",
                "host": os.environ.get("PG_HOST", "localhost"),
                "port": int(os.environ.get("PG_PORT", "5432")),
                "database": os.environ.get("PG_DATABASE", "provisa"),
                "username": os.environ.get("PG_USER", "provisa"),
                "password": os.environ.get("PG_PASSWORD", "provisa"),
            },
            {"id": "om", "type": "openapi", "path": spec_path, "base_url": f"{api}/api"},
        ],
        "tables": [
            {
                "source_id": "shop-pg",
                "domain_id": "shop",
                "schema": _SCHEMA,
                "table": "customers",
                "columns": [
                    {"name": "id", "data_type": "integer", **visible},
                    {"name": "email", "data_type": "varchar", **visible},
                    {"name": "phone", "data_type": "varchar", **visible, "masks": masked},
                ],
            },
            {
                "source_id": "om",
                "domain_id": "catalog",
                "schema": "catalog",
                "table": "list_tables",
                # what a read asks OpenMetadata to include (REQ-2061)
                "arguments": {"fields": "columns,tags"},
                "pagination": {
                    "type": "cursor",
                    "cursor_param": "after",
                    "cursor_field": "paging.after",
                    "rows_field": "data",
                },
                "columns": [
                    {"name": "name", "data_type": "varchar", **visible},
                    {"name": "columns", "data_type": "jsonb", **visible},
                ],
            },
        ],
        "relationships": [],
        "rls_rules": [],
        "functions": [],
        "webhooks": [],
    }


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


async def _shop(create: bool) -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        if create:
            await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
            await conn.execute(
                f"CREATE TABLE {_SCHEMA}.customers (id int PRIMARY KEY, email text, phone text)"
            )
    finally:
        await conn.close()


@pytest_asyncio.fixture(loop_scope="session")
async def served():
    from tests.integration.isolated_server import IsolatedServer

    await _shop(create=True)
    catalog = _OpenMetadata()
    workdir = tempfile.TemporaryDirectory()
    work = Path(workdir.name)
    (work / "swagger.json").write_bytes(gzip.open(_SPEC).read())
    (work / "config.yaml").write_text(
        yaml.safe_dump(_config(str(work / "swagger.json"), catalog.url))
    )
    server = IsolatedServer(_ORG, engine="duckdb", config=str(work / "config.yaml"))
    server.start()
    try:
        yield server, catalog
    finally:
        server.stop_process()
        workdir.cleanup()
        catalog.close()
        await _shop(create=False)


# The catalog's personal columns of a table that Provisa holds, none of whose mask entries masks.
# A governed statement reads registered relations only (no table functions), so the catalog's
# column list is tested by JSON containment: it holds a column of that name carrying the tag.
_DRIFT = f"""
SELECT r.table_name, c.column_name
FROM meta.registered_tables r
JOIN meta.table_columns c ON c.table_id = r.id
JOIN catalog.list_tables t ON t.name = r.table_name
WHERE r.schema_name = '{_SCHEMA}'
  AND t.columns @> jsonb_build_array(
    jsonb_build_object('name', c.column_name, 'tags', '[{{"tagFQN": "PII.Sensitive"}}]'::jsonb)
  )
  AND NOT c.masks @> '[{{"mask": {{}}}}]'::jsonb
"""


async def test_the_columns_the_catalog_calls_personal_and_provisa_does_not_mask(served):
    srv, catalog = served
    async with httpx.AsyncClient(base_url=srv.base_url, timeout=120.0) as client:
        resp = await client.post(
            "/data/sql", json={"sql": _DRIFT}, headers={"X-Provisa-Role": _ROLE}
        )
    assert resp.status_code == 200, f"{resp.text}\n{srv.dump_stderr_debug()[-6000:]}"
    body = resp.json()
    rows = body["data"]["sql"]
    assert [(r["table_name"], r["column_name"]) for r in rows] == [("customers", "email")], body
    # the catalog was read to its end, the second page by the cursor the first answered
    reads = [q.get("after") for q in catalog.asked]
    assert reads[-2:] == [None, "cGFnZS0y"], catalog.asked
    assert all(q["fields"] == "columns,tags" for q in catalog.asked), catalog.asked
