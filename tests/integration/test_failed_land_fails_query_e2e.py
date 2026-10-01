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
it. The first query lands the feed and returns its items; the feed then fails (HTTP 500), the
source's 2s cache_ttl runs out, and the same query must fail rather than return the landed rows.
"""

# Requirements: REQ-1661

from __future__ import annotations

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
        control_plane="sqlite",
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


async def test_a_failed_land_fails_the_query_instead_of_serving_the_stale_replica(feed_server):
    feed, server = feed_server
    async with httpx.AsyncClient(base_url=server.base_url, timeout=60.0) as client:
        first = await client.post("/data/graphql", json=_QUERY)
        assert first.status_code == 200, first.text
        body = first.json()
        assert not body.get("errors"), body
        assert sorted(r["title"] for r in body["data"]["feedItems"]) == ["stale-one", "stale-two"]

        feed.fail = True
        time.sleep(3)  # past the source's 2s cache_ttl: the next read must re-land

        second = await client.post("/data/graphql", json=_QUERY)
    body = (
        second.json()
        if second.headers.get("content-type", "").startswith("application/json")
        else {}
    )
    stale_rows = (body.get("data") or {}).get("feedItems")
    assert not stale_rows, f"served the stale replica: {second.text}"
    assert second.status_code >= 400 or body.get("errors"), second.text
    assert "500" in second.text, second.text  # the land's own cause reaches the caller
