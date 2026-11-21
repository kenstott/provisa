# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c7e2a-9b44-4d0e-a3c8-6e2d18b7f903
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1661, amended 2026-09-30): a query whose source must land first, and whose land
fails, errors -- it never answers from the stale replica.

A real Provisa server (DuckDB engine, SQLite control plane, DuckDB materialize store) registers an
RSS feed served by this test's own HTTP server. RSS is attached by no engine, so every read lands
it. The first query lands the feed and returns its items; the feed then fails (HTTP 500) and the
source's 2s cache_ttl runs out.

The source declares no freshness guarantee, so a read past its TTL may be answered from the copy it
holds while the refresh runs in the background (REQ-860 and REQ-1661, amended 2026-11-09). That
refresh is the land that fails. Once its failure is recorded, the same query must fail with the
land's own cause rather than return the landed rows.
"""

# Requirements: REQ-1661

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_ISOLATED_ORG = "failed_land_fails_query"
_CONFIG = "tests/fixtures/duckdb_rss_land_config.yaml"
_FEED = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>t</title>
<item><title>stale-one</title><link>http://x/1</link><guid>1</guid>
<pubDate>Tue, 29 Sep 2026 10:00:00 GMT</pubDate></item>
<item><title>stale-two</title><link>http://x/2</link><guid>2</guid>
<pubDate>Tue, 29 Sep 2026 11:00:00 GMT</pubDate></item>
</channel></rss>"""
_QUERY = {"query": "{ feedItems { title } }"}
_REPLICA = ("feed-rss", "default", "feed_items")
_STALE = ["stale-one", "stale-two"]


class _Feed:
    """The RSS feed: serves ``_FEED`` until ``fail`` is set, then answers HTTP 500."""

    def __init__(self) -> None:
        feed = self
        self.fail = False

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's handler name
                if feed.fail:
                    self.send_response(500)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/rss+xml")
                self.end_headers()
                self.wfile.write(_FEED)

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/rss"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()


@pytest_asyncio.fixture(scope="module")
async def feed_server():
    from tests.integration.isolated_server import IsolatedServer

    feed = _Feed()
    os.environ["PROVISA_TEST_RSS_FEED_URL"] = feed.url
    store_dir = tempfile.TemporaryDirectory()
    server = IsolatedServer(
        _ISOLATED_ORG,
        engine="duckdb",
        config=_CONFIG,
        control_plane="own",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    server.start()
    try:
        yield feed, server
    finally:
        server.stop_process()
        store_dir.cleanup()
        feed.close()
        del os.environ["PROVISA_TEST_RSS_FEED_URL"]


def _failed_land(server) -> str | None:
    """The error of the replica's failed land, read from the server's own control plane, waiting
    for the land the read asked for to finish; None when no land of it failed by the deadline."""
    import sqlalchemy as sa

    from provisa.core.database import create_engine_from_url

    engine = create_engine_from_url(server.plane.url)
    query = sa.text(
        f"SELECT last_error FROM org_{server.org_id}.replica_state "
        "WHERE source_id = :s AND schema_name = :sc AND table_name = :t AND build_state = 'failed'"
    )
    source_id, schema_name, table_name = _REPLICA
    deadline = time.monotonic() + 60
    try:
        while True:
            with engine.connect() as conn:
                error = conn.execute(
                    query, {"s": source_id, "sc": schema_name, "t": table_name}
                ).scalar()
            if error is not None or time.monotonic() > deadline:
                return error
            time.sleep(0.2)
    finally:
        engine.dispose()


def _body(response: httpx.Response) -> dict:
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return {}


def _assert_fails_with_the_lands_cause(response: httpx.Response) -> None:
    body = _body(response)
    stale_rows = (body.get("data") or {}).get("feedItems")
    assert not stale_rows, f"served the stale replica: {response.text}"
    assert response.status_code >= 400 or body.get("errors"), response.text
    assert "500" in response.text, response.text  # the land's own cause reaches the caller


async def test_a_failed_land_fails_the_query_instead_of_serving_the_stale_replica(feed_server):
    from provisa.federation.query_residency import _BUILD_POLL_S

    feed, server = feed_server
    async with httpx.AsyncClient(base_url=server.base_url, timeout=60.0) as client:
        first = await client.post("/data/graphql", json=_QUERY)
        assert first.status_code == 200, first.text
        body = first.json()
        assert not body.get("errors"), body
        assert sorted(r["title"] for r in body["data"]["feedItems"]) == _STALE

        feed.fail = True
        await asyncio.sleep(3)  # past the source's 2s cache_ttl: the replica must be re-landed

        # This read asks for the re-land. Before any land has failed it may be answered from the
        # copy held (REQ-860: no freshness guarantee); once one has failed it fails with its cause.
        past_ttl = await client.post("/data/graphql", json=_QUERY)
        asked_at = time.monotonic()
        past_ttl_rows = (_body(past_ttl).get("data") or {}).get("feedItems")
        if past_ttl_rows:
            assert past_ttl.status_code == 200, past_ttl.text
            assert sorted(r["title"] for r in past_ttl_rows) == _STALE
        else:
            _assert_fails_with_the_lands_cause(past_ttl)

        error = await asyncio.to_thread(_failed_land, server)
        assert error is not None, "the re-land of the failing feed was never recorded as failed"
        assert "500" in error, error
        # A reader reuses its process's copy of a record that shows a build running for
        # _BUILD_POLL_S without reading it again (query_residency._joins): the next read comes
        # after that copy has lapsed, so it reads the failure the control plane now holds.
        await asyncio.sleep(max(0.0, asked_at + _BUILD_POLL_S - time.monotonic()))

        after_failure = await client.post("/data/graphql", json=_QUERY)
    _assert_fails_with_the_lands_cause(after_failure)
