# Copyright (c) 2026 Kenneth Stott
# Canary: 6bdd9e35-8117-4455-bc75-6937db148157
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's services to a code source's worker beside its secrets and the LLM (REQ-2106): its
settings, its own cache and vector collection, a fetch Provisa makes for it within the egress
allow-list, and its log lines and metrics -- each answered for the worker holding the token, as a
stage of the request the call serves; the worker's calls themselves a stage, with the request's
traceparent sent to the host."""

# Requirements: REQ-2106

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from provisa.codesource import service_ops, services, worker

pytestmark = pytest.mark.unit


def _grant(**extra) -> services.ServiceGrant:
    return services.ServiceGrant(org_id="acme", source_id="scorer", **extra)


@pytest.fixture
def token():
    issued = services.issue(
        _grant(settings={"region": "eu-west", "threshold": 0.8}, egress=("api.example.com",))
    )
    yield issued
    services.revoke(issued)


def _call(token: str, method: str, path: str, body=None, headers=None) -> tuple[int, Any]:
    status, content_type, answered = services.answer(
        method, f"/{token}/{path}", b"" if body is None else json.dumps(body).encode(), headers
    )
    assert content_type == services.JSON
    return status, json.loads(answered)


@pytest.fixture
def org(monkeypatch):
    """The org binding the services read through, with no database behind it."""

    @asynccontextmanager
    async def bound(grant):
        yield SimpleNamespace(), {}

    monkeypatch.setattr("provisa.codesource.llm_service.org_bound", bound)


# -- settings -----------------------------------------------------------------------------------


def test_a_declared_setting_is_read_and_another_refused_by_name(token):
    assert _call(token, "GET", "settings/region") == (200, {"value": "eu-west"})
    assert _call(token, "GET", "settings") == (200, {"region": "eu-west", "threshold": 0.8})
    status, refused = _call(token, "GET", "settings/colour")
    assert status == 404 and "'colour'" in refused["error"]["message"]


def test_a_setting_is_a_text_a_number_or_true_false():
    from provisa.codesource.bundle import BundleRefused

    assert services.declared_settings({"a": "x", "b": 2, "c": True}) == {
        "a": "x",
        "b": 2,
        "c": True,
    }
    for bad in ({"a": [1]}, {"a": {"b": 1}}, ["a"]):
        with pytest.raises(BundleRefused):
            services.declared_settings(bad)


# -- the cache ----------------------------------------------------------------------------------


@pytest.fixture
def cache(monkeypatch, org) -> dict:
    held: dict = {}

    async def get(db, source_id, key):
        found = held.get((source_id, key))
        return None if found is None else found[0]

    async def put(db, source_id, key, text, expires):
        held[(source_id, key)] = (text, expires)

    async def delete(db, source_id, key):
        held.pop((source_id, key), None)

    monkeypatch.setattr(service_ops, "_cache_get", get)
    monkeypatch.setattr(service_ops, "_cache_put", put)
    monkeypatch.setattr(service_ops, "_cache_delete", delete)
    return held


def test_the_cache_keeps_a_value_for_its_ttl_and_answers_a_miss(token, cache):
    assert _call(token, "GET", "cache/rates")[0] == 404
    assert _call(token, "PUT", "cache/rates", {"value": {"usd": 1.0}, "ttl": 60}) == (200, {})
    text, expires = cache[("scorer", "rates")]
    assert json.loads(text) == {"usd": 1.0} and isinstance(expires, datetime)
    assert _call(token, "GET", "cache/rates") == (200, {"value": {"usd": 1.0}})
    assert _call(token, "DELETE", "cache/rates") == (200, {})
    assert ("scorer", "rates") not in cache


def test_a_cache_value_is_at_most_a_megabyte_and_a_ttl_is_above_zero(token, cache):
    assert _call(token, "PUT", "cache/big", {"value": "x" * (1024 * 1024 + 1)})[0] == 413
    assert _call(token, "PUT", "cache/k", {"value": 1, "ttl": 0})[0] == 400
    assert _call(token, "PUT", "cache/k", {"ttl": 5})[0] == 400


def test_the_caches_statement_is_the_control_planes_own_upsert():
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.dialects.postgresql import insert

    from provisa.core.schema_org import code_source_cache as t

    stmt = insert(t).values(source_id="s", key="k", value="1", expires_at=None)
    stmt = stmt.on_conflict_do_update(
        index_elements=[t.c.source_id, t.c.key],
        set_={"value": stmt.excluded.value, "expires_at": stmt.excluded.expires_at},
    )
    assert "ON CONFLICT (source_id, key) DO UPDATE" in str(
        stmt.compile(dialect=postgresql.dialect())
    )


# -- the vector collection ----------------------------------------------------------------------


@pytest.fixture
def collection(monkeypatch, org) -> dict:
    held: dict = {}

    async def embed(grant, db, cfg, model, texts):
        assert model == "m"
        return [[1.0, float(len(t))] for t in texts]

    async def dimension(db, source_id):
        rows = [r for (s, _), r in held.items() if s == source_id]
        return len(json.loads(rows[0][0])) if rows else None

    async def count(db, source_id):
        return sum(1 for (s, _) in held if s == source_id)

    async def put(db, source_id, items):
        for i in items:
            held[(source_id, i["id"])] = (
                json.dumps(i["vector"]),
                i.get("text"),
                json.dumps(i.get("metadata") or {}),
            )

    async def every(db, source_id):
        return [(k[1], *v) for k, v in held.items() if k[0] == source_id]

    monkeypatch.setattr("provisa.codesource.llm_service.embed", embed)
    monkeypatch.setattr(service_ops, "_dimension", dimension)
    monkeypatch.setattr(service_ops, "_vector_count", count)
    monkeypatch.setattr(service_ops, "_vectors_put", put)
    monkeypatch.setattr(service_ops, "_vectors_all", every)
    return held


def test_the_collection_is_built_and_searched_nearest_first(token, collection):
    items = [
        {"id": "east", "vector": [1.0, 0.0], "metadata": {"r": "e"}},
        {"id": "north", "vector": [0.0, 1.0]},
        {"id": "ne", "vector": [1.0, 1.0]},
    ]
    assert _call(token, "POST", "vectors/upsert", {"items": items}) == (200, {"upserted": 3})
    status, found = _call(token, "POST", "vectors/search", {"query": [1.0, 0.1], "k": 2})
    assert status == 200
    assert [m["id"] for m in found["matches"]] == ["east", "ne"]
    assert found["matches"][0]["metadata"] == {"r": "e"}


def test_a_text_is_embedded_by_the_orgs_vector_model(token, collection):
    assert (
        _call(
            token, "POST", "vectors/upsert", {"items": [{"id": "a", "text": "abc"}], "model": "m"}
        )[0]
        == 200
    )
    assert json.loads(collection[("scorer", "a")][0]) == [1.0, 3.0]
    status, refused = _call(token, "POST", "vectors/upsert", {"items": [{"id": "b", "text": "x"}]})
    assert status == 400 and "model" in refused["error"]["message"]


def test_the_collection_keeps_one_dimension_and_k_is_bounded(token, collection):
    _call(token, "POST", "vectors/upsert", {"items": [{"id": "a", "vector": [1.0, 0.0]}]})
    status, refused = _call(
        token, "POST", "vectors/upsert", {"items": [{"id": "b", "vector": [1.0, 0.0, 0.0]}]}
    )
    assert status == 400 and "3 dimensions" in refused["error"]["message"]
    assert _call(token, "POST", "vectors/search", {"query": [1.0, 0.0], "k": 0})[0] == 400


# -- a fetch through Provisa --------------------------------------------------------------------


@pytest.fixture
def fetched(monkeypatch, org) -> dict:
    held: dict = {"audited": []}

    def answer(request: httpx.Request) -> httpx.Response:
        held["request"] = request
        return httpx.Response(200, json={"ok": True})

    real = httpx.AsyncClient

    def client(**kw):
        held["client"] = kw
        return real(transport=httpx.MockTransport(answer))

    async def audit(db, **kw):
        held["audited"].append(kw)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    monkeypatch.setattr("provisa.core.org_membership.record_admin_action", audit)
    return held


def test_a_fetch_to_an_allowed_host_is_made_and_recorded(token, fetched):
    status, answered = _call(
        token,
        "POST",
        "http",
        {"method": "GET", "url": "https://api.example.com/rates", "headers": {"A": "b"}},
    )
    assert (
        status == 200 and answered["status"] == 200 and json.loads(answered["body"]) == {"ok": True}
    )
    assert fetched["request"].headers["A"] == "b"
    assert fetched["client"]["trust_env"] is True  # the deployment's outbound proxy
    (audited,) = fetched["audited"]
    assert audited["action"] == service_ops.HTTP_CALL and audited["subject_id"] == "api.example.com"


@pytest.mark.parametrize(
    "url", ["https://evil.example.com/x", "http://127.0.0.1:8001/admin", "http://localhost/x"]
)
def test_a_fetch_off_the_allow_list_or_to_loopback_is_refused_by_name(token, fetched, url):
    status, refused = _call(token, "POST", "http", {"method": "GET", "url": url})
    assert status == 403 and "egress allow-list" in refused["error"]["message"]
    assert "request" not in fetched


def test_a_binary_answer_comes_back_as_base64(token, fetched, monkeypatch):
    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kw: real(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"\xff\x00"))
        ),
    )
    _status, answered = _call(token, "POST", "http", {"url": "https://api.example.com/b"})
    assert answered["body_base64"] == "/wA="


# -- log and metric -----------------------------------------------------------------------------


def test_a_log_line_is_written_for_the_source_and_its_request(token, caplog, monkeypatch):
    events: list = []
    monkeypatch.setattr(
        "provisa.otel_compat.request_event", lambda name, **a: events.append((name, a))
    )
    caplog.set_level(logging.INFO, logger="provisa.codesource.code")
    assert _call(
        token, "POST", "log", {"level": "warning", "message": "scored", "fields": {"n": 3}}
    ) == (200, {})
    (record,) = [r for r in caplog.records if r.name == "provisa.codesource.code"]
    assert record.levelno == logging.WARNING and "scorer: scored" in record.getMessage()
    assert events == [
        ("codesource.log", {"source": "scorer", "level": "warning", "message": "scored"})
    ]
    assert _call(token, "POST", "log", {"level": "loud", "message": "x"})[0] == 400


def test_a_metric_point_carries_its_source_and_org(token, monkeypatch):
    points: list = []

    class Instrument:
        def add(self, value, attributes):
            points.append(("add", value, attributes))

        def set(self, value, attributes):
            points.append(("set", value, attributes))

    monkeypatch.setattr(service_ops, "_instrument", lambda kind, name: Instrument())
    assert _call(
        token, "POST", "metric", {"name": "scored", "value": 3, "attributes": {"r": "eu"}}
    ) == (200, {})
    assert _call(token, "POST", "metric", {"name": "depth", "value": 7, "kind": "gauge"}) == (
        200,
        {},
    )
    assert points[0] == (
        "add",
        3,
        {"r": "eu", "provisa.codesource.source": "scorer", "provisa.tenant": "acme"},
    )
    assert points[1][0] == "set"
    for bad in (
        {"name": "Bad Name", "value": 1},
        {"name": "n", "value": "1"},
        {"name": "n", "value": -1},
        {"name": "n", "value": 1, "kind": "summary"},
    ):
        assert _call(token, "POST", "metric", bad)[0] == 400


# -- the request's scope ------------------------------------------------------------------------


def test_a_services_request_naming_its_call_runs_in_that_calls_scope(token, monkeypatch):
    from provisa import otel_compat

    entered: list = []

    class Scope:
        def __enter__(self):
            entered.append("in")

        def __exit__(self, *a):
            entered.append("out")

    monkeypatch.setattr(
        worker, "call_scope", lambda tok, call_id: "scope" if (tok, call_id) == (token, 7) else None
    )
    monkeypatch.setattr(otel_compat, "in_carried_scope", lambda scope: Scope())
    _call(token, "GET", "settings/region", headers={"x-provisa-call": "7"})
    assert entered == ["in", "out"]
    _call(token, "GET", "settings/region", headers={"x-provisa-call": "8"})
    assert entered == ["in", "out"]  # a call not running: no scope


def test_a_call_carries_the_requests_trace_and_holds_its_scope_while_it_runs(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "current_traceparent", lambda: "00-trace-span-01")
    w = worker.Worker("scorer", "h1", worker.HostCommand(argv=lambda a: [a], env={}, cwd=tmp_path))
    w._services_token = "tok"  # noqa: SLF001
    frame = w._call_frame(5, "rates", "table", [1, worker.ABSENT], ["n", "d"], None)  # noqa: SLF001
    assert frame["traceparent"] == "00-trace-span-01"
    assert worker.call_scope("tok", 5) is not None
    w._close(5)  # noqa: SLF001
    assert worker.call_scope("tok", 5) is None


def test_a_calls_stage_names_what_it_is_never_its_arguments(tmp_path):
    seen: dict = {}
    span = SimpleNamespace(set_attribute=lambda k, v: seen.__setitem__(k, v))
    w = worker.Worker("scorer", "h1", worker.HostCommand(argv=lambda a: [a], env={}, cwd=tmp_path))
    w._describe(span, "rates", "table", ["secret-arg", worker.ABSENT])  # noqa: SLF001
    assert seen == {
        "codesource.source": "scorer",
        "codesource.function": "rates",
        "codesource.kind": "table",
        "codesource.arg_count": 1,
        "codesource.host": "local",
        "codesource.code_hash": "h1",
    }
    assert "secret-arg" not in json.dumps(seen)
