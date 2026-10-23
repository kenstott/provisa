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
ALL = "sandbox,analyst"  # the visitor's "Role: All"


def test_every_step_that_runs_something_is_sent_in_tour_order_then_the_explorers():
    tour = yaml.safe_load((DEMOS / "weblogs" / "tour.yaml").read_text())["steps"]
    sent = demo_warmup.tour_requests(DEMOS / "weblogs", ALL)
    runs = [s for s in tour if {"sql", "cypher", "example", "api", "jsonapi"} & s.keys()]
    assert [what for what, *_ in sent[: len(runs)]] == [s["key"] for s in runs]
    assert [request[:4] for request in sent[len(runs) :]] == [
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
    assert (api["key"], method, path, None) in [request[:4] for request in sent]


def test_an_example_step_sends_the_demos_own_query():
    tour = yaml.safe_load((DEMOS / "sonnets" / "tour.yaml").read_text())["steps"]
    step = next(s for s in tour if "example" in s)
    text = (DEMOS / "sonnets" / "queries" / f"{step['example']}.graphql").read_text()
    sent = demo_warmup.tour_requests(DEMOS / "sonnets", ALL)
    assert (step["key"], "POST", "/data/graphql", {"query": text}) in [r[:4] for r in sent]


def test_a_subscription_step_is_not_sent():
    """The streaming demo's live step is a subscription: sent, its stream ran until the request
    timeout and the warm-up logged it as failed, every sandbox, holding the steps after it back."""
    tour = yaml.safe_load((DEMOS / "streaming" / "tour.yaml").read_text())["steps"]
    live = next(s for s in tour if s["key"] == "streamingLive")
    text = (DEMOS / "streaming" / "queries" / f"{live['example']}.graphql").read_text()
    assert "subscription" in text
    sent = demo_warmup.tour_requests(DEMOS / "streaming", ALL)
    assert "streamingLive" not in [what for what, *_ in sent]
    queries = [s for s in tour if "example" in s and s["key"] != "streamingLive"]
    assert queries, "the streaming tour's other example steps are still sent"
    assert {s["key"] for s in queries} <= {what for what, *_ in sent}


def test_the_warmup_sends_each_request_as_the_visitor_and_carries_on_past_a_refusal(caplog):
    seen: list[tuple[str, str, str | None, str | None, str | None, bytes]] = []

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
                (headers.get(b"x-provisa-role") or b"").decode() or None,
                body,
            )
        )
        status = 500 if len(seen) == 1 else 200
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    with caplog.at_level(logging.WARNING, logger="provisa.api.demo_warmup"):
        asyncio.run(
            demo_warmup._warm(app, "sbxabc", {"Authorization": "Bearer t"}, DEMOS / "weblogs", ALL)
        )
    expected = demo_warmup.tour_requests(DEMOS / "weblogs", ALL)
    assert [(m, p, r) for m, p, _, _, r, _ in seen] == [
        (m, p.split("?")[0], r) for _, m, p, _, _, r in expected
    ]
    assert {(a, o) for _, _, a, o, _, _ in seen} == {("Bearer t", "sbxabc")}
    assert json.loads(seen[0][5]) == expected[0][3]
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

    asyncio.run(
        demo_warmup._warm(app, "sbxabc", {"Authorization": "Bearer t"}, DEMOS / "weblogs", ALL)
    )
    assert marked and all(marked)
    assert demo_warmup.WARMING.get() is False  # a request of the visitor's own is not marked


def test_each_request_carries_the_role_the_tour_has_chosen_by_then():
    """The hasuramigration tour picks ana in the app's role picker, then support, then the analyst
    as the SQL explorer's run-as, then "Role: All". Sent with no role, its profile and support
    steps were read as a role that is not served the customers' names, and never warmed."""
    every = "sandbox,analyst,customer,support,support_lead,ana"
    sent = {
        what: roles
        for what, *_, roles in demo_warmup.tour_requests(DEMOS / "hasuramigration", every)
    }
    assert sent["hasuramigrationMyOrders"] == "ana"
    assert sent["hasuramigrationProfile"] == "ana"
    assert sent["hasuramigrationSupportScreen"] == "support"
    assert sent["hasuramigrationByIndustry"] == "analyst"
    assert sent["/data/graphql"] == every  # the explorers, after the recap's "Role: All"


def test_the_page_opens_as_role_all_and_an_explorer_role_reaches_only_statements(tmp_path):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "q.graphql").write_text("query Q { a }")
    (tmp_path / "tour.yaml").write_text(
        yaml.safe_dump(
            {
                "steps": [
                    {"key": "first", "sql": "SELECT 0"},
                    {
                        "key": "app",
                        "route": "/query",
                        "capability": "query_development",
                        "role": "ana",
                    },
                    {
                        "key": "explorer",
                        "route": "/sql",
                        "capability": "query_development",
                        "role": "analyst",
                    },
                    {"key": "stmt", "sql": "SELECT 1"},
                    {
                        "key": "graph",
                        "route": "/query",
                        "capability": "query_development",
                        "example": "q",
                    },
                    {
                        "key": "set",
                        "route": "/query",
                        "capability": "query_development",
                        "role": "ana,region__eu",
                    },
                    {"key": "stmt2", "sql": "SELECT 2"},
                ]
            }
        )
    )
    sent = {what: roles for what, *_, roles in demo_warmup.tour_requests(tmp_path, ALL)}
    assert sent == {
        "first": ALL,
        "stmt": "analyst",
        "graph": "ana",
        "stmt2": "ana,region__eu",
        "/data/rest/openapi.json": "ana",
        "/data/jsonapi/openapi.json": "ana",
        "/data/graphql": "ana",
    }


def test_each_statement_carries_the_time_travel_the_tour_has_set_by_then():
    """The reproduce tour reads its report as of 30 September, then as recorded when it was first
    filed (the first refresh's time, read from the refreshes step's answer), then its audit log.
    Sent with no time travel, the warm-up and the walk read the report as it stands now, and the
    audit step's ``as_of`` rows were never written."""
    sent = {
        what: travel
        for what, _m, _p, _b, travel, _r in demo_warmup.tour_requests(DEMOS / "reproduce", ALL)
    }
    assert sent["reproduceNow"] == {}
    assert sent["reproduceAsOfNow"] == {"as_of": "2026-09-30T12:00:00"}
    filed = {
        "as_of": "2026-09-30T12:00:00",
        "as_recorded_from": {"step": "reproduceRefreshes", "row": 0, "column": "refreshed_at"},
    }
    assert sent["reproduceFiled"] == filed
    assert sent["reproduceAudit"] == filed
    answers = {"reproduceRefreshes": [{"refreshed_at": "2026-10-01T09:00:00"}]}
    assert demo_warmup.travel_headers(filed, answers) == {
        "X-Provisa-As-Of": "2026-09-30T12:00:00",
        "X-Provisa-As-Recorded": "2026-10-01T09:00:00",
    }
    assert demo_warmup.travel_headers({}, answers) == {}


def test_a_later_recorded_time_replaces_the_one_before(tmp_path):
    (tmp_path / "tour.yaml").write_text(
        yaml.safe_dump(
            {
                "steps": [
                    {"key": "from", "as_recorded_from": {"step": "a", "row": 0, "column": "t"}},
                    {"key": "a", "sql": "SELECT 1"},
                    {"key": "at", "as_recorded": "2026-10-12T17:00:00"},
                    {"key": "b", "sql": "SELECT 2"},
                ]
            }
        )
    )
    sent = {
        what: travel for what, _m, _p, _b, travel, _r in demo_warmup.tour_requests(tmp_path, ALL)
    }
    assert sent["b"] == {"as_recorded": "2026-10-12T17:00:00"}


def test_the_warmup_sends_a_statement_as_recorded_at_an_earlier_statements_answer():
    seen: dict[str, dict[bytes, bytes]] = {}

    async def app(scope, receive, send):
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        sql = json.loads(body)["sql"] if body and scope["path"] == "/data/sql" else ""
        seen[sql] = dict(scope["headers"])
        rows = [{"refreshed_at": "2026-10-01T09:00:00"}] if "mv_refresh_lineage" in sql else []
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send(
            {"type": "http.response.body", "body": json.dumps({"data": {"sql": rows}}).encode()}
        )

    asyncio.run(
        demo_warmup._warm(app, "sbxabc", {"Authorization": "Bearer t"}, DEMOS / "reproduce", ALL)
    )
    [audit] = [h for sql, h in seen.items() if "query_audit_log" in sql]
    assert audit[b"x-provisa-as-of"] == b"2026-09-30T12:00:00"
    assert audit[b"x-provisa-as-recorded"] == b"2026-10-01T09:00:00"
