# Copyright (c) 2026 Kenneth Stott
# Canary: 3e6a2fe5-de83-443f-a68c-aac7f3c20894
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warming a demo sandbox's tour as it opens (REQ-2006, REQ-2010).

A sandbox's first reads are its slowest: the engine attaches each source, a connector's server
prepares its catalog, an embedding model loads, the GraphQL schema and the API specs are built. A
visitor walking the tour met each of these as a step that timed out. When a sandbox opens, the work
of every tour step is therefore sent once in the background -- one request at a time, in tour order,
as the visitor and through the same transports a step uses, so it is governed as the visitor's own
request is -- and its answer discarded. The visitor never waits for it."""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

#: The specs and the schema every sandbox's explorers read on opening.
_EXPLORERS: tuple[tuple[str, str, dict | None], ...] = (
    ("GET", "/data/rest/openapi.json", None),
    ("GET", "/data/jsonapi/openapi.json", None),
    ("POST", "/data/graphql", {"query": "{ __schema { queryType { name } } }"}),
)


def tour_requests(directory: Path) -> list[tuple[str, str, str, dict | None]]:
    """The requests that do the work of the demo's tour steps, in tour order, then the explorers':
    ``(what, method, path, body)``. A step that runs nothing (a page, a source, a term) and a
    Polly step (answered when the bundle was built) send none."""
    from provisa.core.demo import demo_tour, example_queries

    examples = {q["name"]: q["query"] for q in example_queries(directory)}
    out: list[tuple[str, str, str, dict | None]] = []
    for step in demo_tour(directory):
        key = step["key"]
        if "sql" in step:
            out.append((key, "POST", "/data/sql", {"sql": step["sql"]}))
        if "cypher" in step:
            out.append((key, "POST", "/data/cypher", {"query": step["cypher"]}))
        if "example" in step:
            out.append((key, "POST", "/data/graphql", {"query": examples[step["example"]]}))
        for kind in ("api", "jsonapi"):
            if kind in step:
                method, _, path = (
                    step[kind].partition(" ") if kind == "api" else ("GET", "", step[kind])
                )
                out.append((key, method, path, None))
    out.extend((path, method, path, body) for method, path, body in _EXPLORERS)
    return out


async def _warm(app: Any, org_id: str, headers: dict[str, str], directory: Path) -> None:
    import httpx

    sent = {**headers, "x-org-provisa": org_id}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://warmup") as client:
        for what, method, path, body in tour_requests(directory):
            try:
                resp = await client.request(method, path, json=body, headers=sent, timeout=None)
            except Exception:
                log.exception("sandbox %s tour warm-up: %s failed", org_id, what)
                continue
            if resp.status_code >= 400:
                # Not a failure of the warm-up: the visitor's own request meets the same answer.
                log.warning(
                    "sandbox %s tour warm-up: %s answered %s: %s",
                    org_id,
                    what,
                    resp.status_code,
                    resp.text[:300],
                )
    # At WARNING: the hosted demo tier logs at WARNING, and this one line per sandbox is how its
    # operator sees that a sandbox's tour was warmed.
    log.warning("sandbox %s tour warmed", org_id)


def warm_tour(app: Any, org_id: str, demo: str, headers: dict[str, str]) -> None:
    """Start warming sandbox ``org_id``'s tour of ``demo`` in the background, sending the
    visitor's ``headers`` (its credential) with each request.

    On a thread of its own, with its own loop: the request that opened the sandbox runs on a
    request thread whose loop stops when that request ends (provisa/core/request_thread.py), and
    a new thread starts with an empty context, so no request deadline is bound over the warm-up."""
    from provisa.api.demo_templates import published_demo

    directory = published_demo(demo).directory
    threading.Thread(
        target=lambda: asyncio.run(_warm(app, org_id, headers, directory)),
        daemon=True,
        name=f"tour-warmup:{org_id}",
    ).start()
