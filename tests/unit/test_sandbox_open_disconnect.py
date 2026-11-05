# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e9a21-5b4d-4f08-8e62-1d9a7b0c4e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A visitor's sandbox whose visitor left before it was opened does not hold its place (REQ-2006,
REQ-2065).

``POST /public/demos/<name>`` takes a place before the sandbox is built. A visitor whose connection
was cut while it built -- the front door drops a quiet connection -- never received the sandbox's
token, so nobody could reach it or close it, and it held its place until the idle expiry. It is now
marked closing, as a closed tab's is, so the grace passes with no request and the place is freed."""

# Requirements: REQ-2006, REQ-2065

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request

from provisa.api import demo_sandbox, demo_sandbox_router, demo_warmup, sandbox_tier
from provisa.core.request_thread import RequestThreadMiddleware


class _Request:
    def __init__(self, gone: bool) -> None:
        self._gone = gone
        self.app = object()

    async def body(self) -> bytes:
        return b""

    async def receive(self) -> dict:
        if self._gone:
            return {"type": "http.disconnect"}
        await asyncio.sleep(3600)  # a client still connected says nothing
        raise AssertionError("the wait for a disconnect is bounded")


@pytest.fixture
def opened(monkeypatch):
    calls: dict[str, list] = {"closing": [], "warmed": []}

    async def _open(name, *, kind, user_id=None):
        return demo_sandbox.OpenedSandbox(
            org_id="sbx0123456789ab", demo=name, user_id="v", token="tok"
        )

    async def _closing(org_id):
        calls["closing"].append(org_id)

    monkeypatch.setattr(sandbox_tier, "serves_sandboxes", lambda: True)
    monkeypatch.setattr(demo_sandbox, "open_sandbox", _open)
    monkeypatch.setattr(demo_sandbox, "mark_closing", _closing)
    monkeypatch.setattr(
        demo_warmup, "warm_tour", lambda *args: calls["warmed"].append(SimpleNamespace(args=args))
    )
    return calls


@pytest.mark.asyncio
async def test_an_open_whose_visitor_left_is_closing_and_not_warmed(opened):
    await demo_sandbox_router.open_demo("worldcup", _Request(gone=True))
    assert opened["closing"] == ["sbx0123456789ab"]
    assert opened["warmed"] == []


@pytest.mark.asyncio
async def test_an_open_handed_over_is_warmed_and_left_open(opened):
    answer = await demo_sandbox_router.open_demo("worldcup", _Request(gone=False))
    assert answer == {"org_id": "sbx0123456789ab", "demo": "worldcup", "token": "tok"}
    assert opened["closing"] == []
    assert len(opened["warmed"]) == 1


def _left_through_the_request_thread(disconnected: bool) -> bool:
    """Whether the router finds the visitor gone, its request on a request thread as the tier serves
    it: there every receive after the pre-read body is relayed to the accepting loop, so
    request.is_disconnected(), whose receive is cancelled before it can be answered, never saw a
    visitor leave (the reviews sandbox of 2026-11-01 held its place an hour)."""
    answers: list[bool] = []
    app = FastAPI()

    @app.post("/public/demos/reviews")
    async def _open(request: Request) -> dict:
        answers.append(await demo_sandbox_router._visitor_left(request))
        return {}

    wrapped = RequestThreadMiddleware(app)
    queue: list[dict] = [{"type": "http.request", "body": b"", "more_body": False}]
    if disconnected:
        queue.append({"type": "http.disconnect"})

    async def _receive():
        if queue:
            return queue.pop(0)
        await asyncio.sleep(3600)

    async def _send(message):
        return None

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "method": "POST",
        "path": "/public/demos/reviews",
        "raw_path": b"/public/demos/reviews",
        "query_string": b"",
        "root_path": "",
        "headers": [],
    }
    asyncio.run(wrapped(scope, _receive, _send))
    return answers[0]


def test_a_visitor_gone_is_seen_through_the_request_thread():
    assert _left_through_the_request_thread(disconnected=True)


def test_a_visitor_still_connected_is_not_taken_for_gone():
    assert not _left_through_the_request_thread(disconnected=False)
