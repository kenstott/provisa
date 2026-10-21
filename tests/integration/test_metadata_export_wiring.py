# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4a6e13-0f72-4c58-b3a1-5e8c2d7f0b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2119: every configured metadata-export target is called by the production publish.

A Provisa server is given each HTTP target in turn, pointed at a receiver this test runs on a
real socket, and publishes through the admin API exactly as an admin does. The receiver records
what arrives and echoes each document back with an id, as a catalog's create call answers. A
target the production path never calls -- the failure that let ``bigquery_dataplex`` pass its unit
tests while never running -- receives nothing, and the test fails naming it.

Whether a target maps the snapshot correctly is its end-to-end test's question, against the real
service; this one asks only whether the publish reaches it. A target that publishes inside the
engine (``snowflake_horizon``) has no HTTP endpoint to receive on: its end-to-end test publishes
through the same production path and is its wiring proof (``ENGINE_NATIVE``).
"""

# Requirements: REQ-2119

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from tests.integration.metadata_export_server import published_server

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ORG = "mdx_wiring"

#: Each HTTP target, with the settings an admin saves for it.
HTTP_TARGETS: dict[str, dict] = {
    "atlan": {"auth_mode": "bearer", "token": "wiring"},
    "atlas": {"auth_mode": "basic", "username": "wiring", "token": "wiring"},
    "collibra": {"auth_mode": "basic", "username": "wiring", "token": "wiring"},
    "datahub": {},
    "openlineage": {},
    "openmetadata": {"auth_mode": "bearer", "token": "wiring"},
}
#: Targets publishing inside the engine, proven by their own end-to-end test.
ENGINE_NATIVE = {
    "snowflake_horizon": "tests/integration/test_metadata_export_snowflake_horizon_e2e.py",
    # A Dataplex asset is a BigQuery object: its members are published by the BigQuery engine.
    "dataplex": "tests/integration/test_metadata_export_dataplex_e2e.py",
    # Purview signs in with Entra before any call, which no local receiver can answer.
    "purview": "tests/integration/test_metadata_export_purview_e2e.py",
}
#: What a delivered publish names: one of the data product's member tables.
MEMBER = "orders"


class _Receiver(BaseHTTPRequestHandler):
    received: list[tuple[str, str, str]]

    def _answer(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode(errors="replace") if length else ""
        self.received.append((self.command, self.path, body))
        try:
            document = json.loads(body) if body else {}
        except ValueError:
            document = {}
        if isinstance(document, dict):
            document = {**document, "id": str(uuid.uuid4()), "guid": str(uuid.uuid4())}
            # A catalog names what it created by its fully qualified name (OpenMetadata).
            document.setdefault("fullyQualifiedName", document.get("name", ""))
        out = json.dumps(document).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _answer

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the base's name
        return


@contextmanager
def _receiving() -> Iterator[tuple[str, list[tuple[str, str, str]]]]:
    received: list[tuple[str, str, str]] = []
    handler = type("Receiver", (_Receiver,), {"received": received})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", received
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def receiver() -> Iterator[tuple[str, list[tuple[str, str, str]]]]:
    with _receiving() as received:
        yield received


@pytest.fixture(scope="module")
async def server():
    async with published_server(ORG) as client:
        yield client


@pytest.mark.parametrize("provider", sorted(HTTP_TARGETS))
async def test_the_production_publish_reaches_the_target(server, receiver, provider):
    url, received = receiver
    target = {"id": provider, "provider": provider, "endpoint": url, **HTTP_TARGETS[provider]}
    response = await server.put(
        "/admin/metadata-export", json={"enabled": True, "targets": [target]}
    )
    assert response.status_code == 200, response.text
    # The publish's own verdict is the end-to-end test's concern: an echoing receiver is no
    # catalog. What it must show is that this target was called with the product's tables.
    await server.post("/admin/metadata-export/publish")
    delivered = [r for r in received if MEMBER in r[2] or MEMBER in r[1]]
    assert delivered, (
        f"metadata export target {provider!r} received nothing naming the member table "
        f"{MEMBER!r} from the production publish ({len(received)} requests in all)"
    )


async def test_one_publish_reaches_every_target_two_of_one_provider_among_them(server):
    """REQ-2118: an org publishes to several catalogs at once; each is called by the one publish."""
    with _receiving() as (first, got_first), _receiving() as (second, got_second):
        targets = [
            {"id": "lineage-a", "provider": "openlineage", "endpoint": first},
            {"id": "lineage-b", "provider": "openlineage", "endpoint": second},
        ]
        response = await server.put(
            "/admin/metadata-export", json={"enabled": True, "targets": targets}
        )
        assert response.status_code == 200, response.text
        published = (await server.post("/admin/metadata-export/publish")).json()
        assert [t["id"] for t in published["targets"]] == ["lineage-a", "lineage-b"]
        for name, received in (("lineage-a", got_first), ("lineage-b", got_second)):
            assert any(MEMBER in r[2] for r in received), f"{name} received nothing"


def test_every_registered_target_is_covered() -> None:
    """A target added to the registry with no wiring proof fails here, by name."""
    from provisa.api.metadata_export.registry import registered_providers

    uncovered = set(registered_providers()) - set(HTTP_TARGETS) - set(ENGINE_NATIVE)
    assert not uncovered, f"metadata export targets with no wiring proof: {sorted(uncovered)}"
