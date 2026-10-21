# Copyright (c) 2026 Kenneth Stott
# Canary: 5664f51b-0778-4c33-8126-222f0852ddbf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's tour is warmed as it opens: every step's work, one request at a time, in tour order,
through the transports a step uses, with the visitor's credential (REQ-2006, REQ-2010)."""

# Requirements: REQ-2006, REQ-2010, REQ-2065

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import yaml

from provisa.api import demo_warmup

DEMOS = Path(__file__).resolve().parents[2] / "demo" / "named"


def test_every_step_that_runs_something_is_sent_in_tour_order_then_the_explorers():
    tour = yaml.safe_load((DEMOS / "weblogs" / "tour.yaml").read_text())["steps"]
    sent = demo_warmup.tour_requests(DEMOS / "weblogs")
    runs = [s for s in tour if {"sql", "cypher", "example", "api", "jsonapi"} & s.keys()]
    assert [what for what, *_ in sent[: len(runs)]] == [s["key"] for s in runs]
    assert sent[len(runs) :] == [
        ("/data/rest/openapi.json", "GET", "/data/rest/openapi.json", None),
        ("/data/jsonapi/openapi.json", "GET", "/data/jsonapi/openapi.json", None),
        (
            "/data/graphql",
            "POST",
            "/data/graphql",
            {"query": "{ __schema { queryType { name } } }"},
        ),
    ]
    api = next(s for s in tour if "api" in s)
    method, path = api["api"].split(" ", 1)
    assert (api["key"], method, path, None) in sent


def test_an_example_step_sends_the_demos_own_query():
    tour = yaml.safe_load((DEMOS / "sonnets" / "tour.yaml").read_text())["steps"]
    step = next(s for s in tour if "example" in s)
    text = (DEMOS / "sonnets" / "queries" / f"{step['example']}.graphql").read_text()
    assert (step["key"], "POST", "/data/graphql", {"query": text}) in demo_warmup.tour_requests(
        DEMOS / "sonnets"
    )


def test_a_subscription_step_is_not_sent():
    """The streaming demo's live step is a subscription: sent, its stream ran until the request
    timeout and the warm-up logged it as failed, every sandbox, holding the steps after it back."""
    tour = yaml.safe_load((DEMOS / "streaming" / "tour.yaml").read_text())["steps"]
    live = next(s for s in tour if s["key"] == "streamingLive")
    text = (DEMOS / "streaming" / "queries" / f"{live['example']}.graphql").read_text()
    assert "subscription" in text
    sent = demo_warmup.tour_requests(DEMOS / "streaming")
    assert "streamingLive" not in [what for what, *_ in sent]
    queries = [s for s in tour if "example" in s and s["key"] != "streamingLive"]
    assert queries, "the streaming tour's other example steps are still sent"
    assert {s["key"] for s in queries} <= {what for what, *_ in sent}


def test_the_warmup_sends_each_request_as_the_visitor_and_carries_on_past_a_refusal(caplog):
    seen: list[tuple[str, str, str | None, str | None, bytes]] = []

    async def app(scope, receive, send):
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        headers = dict(scope["headers"])
        seen.append(
            (
                scope["method"],
                scope["path"],
                (headers.get(b"authorization") or b"").decode() or None,
                (headers.get(b"x-org-provisa") or b"").decode() or None,
                body,
            )
        )
        status = 500 if len(seen) == 1 else 200
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    with caplog.at_level(logging.WARNING, logger="provisa.api.demo_warmup"):
        asyncio.run(
            demo_warmup._warm(app, "sbxabc", {"Authorization": "Bearer t"}, DEMOS / "weblogs")
        )
    expected = demo_warmup.tour_requests(DEMOS / "weblogs")
    assert [(m, p) for m, p, *_ in seen] == [(m, p.split("?")[0]) for _, m, p, _ in expected]
    assert {(a, o) for _, _, a, o, _ in seen} == {("Bearer t", "sbxabc")}
    assert json.loads(seen[0][4]) == expected[0][3]
    assert "answered 500" in caplog.text  # the refusal is logged, and the rest still sent
    # the completion line shows at the hosted tier's WARNING level, once
    assert caplog.text.count("sandbox sbxabc tour warmed") == 1


def test_the_warmup_s_requests_are_marked_as_its_own_and_no_one_else_s():
    """REQ-2065: the app reads WARMING on each warm-up request and skips the sandbox's touch, so a
    warm-up still running when the visitor's tab closes does not revive the sandbox."""
    marked: list[bool] = []

    async def app(scope, receive, send):
        await receive()
        marked.append(demo_warmup.WARMING.get())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    asyncio.run(demo_warmup._warm(app, "sbxabc", {"Authorization": "Bearer t"}, DEMOS / "weblogs"))
    assert marked and all(marked)
    assert demo_warmup.WARMING.get() is False  # a request of the visitor's own is not marked
