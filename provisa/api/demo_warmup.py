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
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

#: REQ-2065: set on the warm-up's own requests. They are the tier's, not the visitor's: they neither
#: restart the sandbox's idle expiry nor revive one whose tab said it was closing (demo_sandbox.touch).
#: A request thread runs in the context it was handed (core/request_thread.py), so the middleware
#: reads it.
WARMING: ContextVar[bool] = ContextVar("provisa_tour_warmup", default=False)

#: The specs and the schema every sandbox's explorers read on opening.
_EXPLORERS: tuple[tuple[str, str, dict | None], ...] = (
    ("GET", "/data/rest/openapi.json", None),
    ("GET", "/data/jsonapi/openapi.json", None),
    ("POST", "/data/graphql", {"query": "{ __schema { queryType { name } } }"}),
)


def tour_requests(directory: Path, all_roles: str) -> list[tuple[str, str, str, dict | None, str]]:
    """The requests that do the work of the demo's tour steps, in tour order, then the explorers':
    ``(what, method, path, body, roles)``. A step that runs nothing (a page, a source, a term), a
    Polly step (answered when the bundle was built) and a subscription send none.

    ``roles`` is the X-Provisa-Role the visitor's page sends it with, as the steps before it chose
    (provisa-ui/src/tour/demoTour.ts): the page opens acting as "Role: All", ``all_roles`` (every
    role the visitor holds, comma-separated); a role picked anywhere but the SQL explorer is the
    app's, which every request after it carries; one picked on the SQL explorer, or a set of roles,
    is the explorer's run-as and reaches only its statements."""
    from provisa.core.demo import demo_tour, example_queries

    def chosen(role: str) -> str:
        return all_roles if role == "all" else role

    examples = {q["name"]: q["query"] for q in example_queries(directory)}
    out: list[tuple[str, str, str, dict | None, str]] = []
    page: str | None = None
    app_role = all_roles
    sql_role: str | None = None
    for step in demo_tour(directory):
        key = step["key"]
        page = step.get("route", page)
        if "role" in step:
            if "," in step["role"] or page == "/sql":
                sql_role = chosen(step["role"])
            else:
                app_role = chosen(step["role"])
        if "sql" in step:
            out.append((key, "POST", "/data/sql", {"sql": step["sql"]}, sql_role or app_role))
        if "cypher" in step:
            out.append((key, "POST", "/data/cypher", {"query": step["cypher"]}, app_role))
        if "example" in step and not _is_subscription(examples[step["example"]]):
            query = {"query": examples[step["example"]]}
            out.append((key, "POST", "/data/graphql", query, app_role))
        for kind in ("api", "jsonapi"):
            if kind in step:
                method, _, path = (
                    step[kind].partition(" ") if kind == "api" else ("GET", "", step[kind])
                )
                out.append((key, method, path, None, app_role))
    out.extend((path, method, path, body, app_role) for method, path, body in _EXPLORERS)
    return out


def _is_subscription(query: str) -> bool:
    """A subscription's answer is a stream that runs until the request timeout ends it: there is
    nothing for a warm-up to wait for, and it held the requests after it back for that long. Its
    first answer is the query the tour's other steps have already warmed."""
    from graphql import OperationDefinitionNode, OperationType, parse

    return any(
        isinstance(d, OperationDefinitionNode) and d.operation == OperationType.SUBSCRIPTION
        for d in parse(query).definitions
    )


async def _warm(
    app: Any, org_id: str, headers: dict[str, str], directory: Path, all_roles: str
) -> None:
    import httpx

    WARMING.set(True)
    started = time.monotonic()
    sent = {**headers, "x-org-provisa": org_id}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://warmup") as client:
        for what, method, path, body, roles in tour_requests(directory, all_roles):
            try:
                resp = await client.request(
                    method, path, json=body, headers={**sent, "X-Provisa-Role": roles}, timeout=None
                )
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
    log.warning("sandbox %s tour warmed in %.1fs", org_id, time.monotonic() - started)


def warm_tour(app: Any, org_id: str, demo: str, headers: dict[str, str]) -> None:
    """Start warming sandbox ``org_id``'s tour of ``demo`` in the background, sending the
    visitor's ``headers`` (its credential) with each request.

    On a thread of its own, with its own loop: the request that opened the sandbox runs on a
    request thread whose loop stops when that request ends (provisa/core/request_thread.py), and
    a new thread starts with an empty context, so no request deadline is bound over the warm-up."""
    from provisa.api.demo_sandbox import SANDBOX_ROLE
    from provisa.api.demo_templates import demo_roles, published_demo

    published = published_demo(demo)
    # The roles the sandbox's person holds (demo_sandbox.open_sandbox): the page's "Role: All".
    all_roles = ",".join([SANDBOX_ROLE, *demo_roles(published.template_org)])
    threading.Thread(
        target=lambda: asyncio.run(_warm(app, org_id, headers, published.directory, all_roles)),
        daemon=True,
        name=f"tour-warmup:{org_id}",
    ).start()
