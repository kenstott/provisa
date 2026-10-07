# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e4b19-6f3a-4d85-9e12-b8a05d7c3f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST and JSON:API: what one request costs (REQ-1877, REQ-1197, REQ-257).

Both surfaces synthesize GraphQL text from the request and hand the compiled SQL to the one
governed pipeline. Two properties are pinned here, with the real routers, the real schema and
compiler, and the real plan store; only the pipeline's two entry points are replaced by a recorder
that answers from a 60-row table:

- a repeated request is parsed, validated and compiled ONCE: its compiled form is kept through the
  same plan store every other surface keeps a governed plan in (``pgwire.governed_plan``);
- a JSON:API list request sends the pipeline ONE statement. The total is counted only when the
  request asks for it (``page[total]=true``); without it the page's ``next`` link comes from the
  page's own rows and neither ``meta.total`` nor ``links.last`` is reported.
"""

# Requirements: REQ-1877, REQ-1197, REQ-257, REQ-222

from __future__ import annotations

import re
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from fastapi import FastAPI, Request

from provisa.api import generated_plan
from provisa.api.jsonapi import generator as jsonapi_generator
from provisa.api.rest import generator as rest_generator
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.parser import parse_query as real_parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query as real_compile_query
from provisa.pgwire import governed_plan
from tests.unit.test_jsonapi import _build_test_schema

pytestmark = [pytest.mark.anyio]

_ROLE = "admin"
_TABLE_ROWS = 60
_JSONAPI = {"accept": "application/vnd.api+json", "X-Provisa-Role": _ROLE}
_REST = {"X-Provisa-Role": _ROLE}


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _row(i: int) -> dict:
    return {
        "id": i,
        "customer_id": i % 7,
        "amount": float(i),
        "region": "US" if i % 2 else "EU",
        "created_at": "2026-01-01",
        "_name_": "sales.orders",
        "_domain_": "sales",
    }


class _Pipeline:
    """Stands in for ``_govern_and_route_compiled`` / ``_execute_plan``: records every statement
    the surface hands the pipeline and answers it from a ``_TABLE_ROWS``-row ``orders`` table."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, list]] = []

    async def govern(self, sql, role_id, *, exec_params=None, state=None, **_kwargs):
        self.statements.append((sql, list(exec_params or [])))
        return SimpleNamespace(sql=sql, params=list(exec_params or []))

    async def execute(self, plan, state):
        sql, params = plan.sql, plan.params
        if "COUNT(*)" in sql:
            return SimpleNamespace(rows=[(_TABLE_ROWS,)], redirect=None)

        def _bound(keyword: str) -> int | None:
            m = re.search(rf"{keyword} \$(\d+)", sql)
            return int(params[int(m.group(1)) - 1]) if m else None

        rows = [_row(i) for i in range(1, _TABLE_ROWS + 1)]
        id_eq = re.search(r'"id" = \$(\d+)', sql)
        if id_eq:
            wanted = int(params[int(id_eq.group(1)) - 1])
            rows = [r for r in rows if r["id"] == wanted]
        offset, limit = _bound("OFFSET") or 0, _bound("LIMIT")
        rows = rows[offset : offset + limit] if limit is not None else rows[offset:]
        select_list = sql[len("SELECT ") : sql.index(" FROM ")]
        names = [re.findall(r'"([^"]+)"', part)[-1] for part in select_list.split(", ")]
        return SimpleNamespace(rows=[tuple(r[n] for n in names) for r in rows], redirect=None)

    @property
    def sql(self) -> list[str]:
        return [s for s, _ in self.statements]


class _Compiles:
    """Counts the parse and the compile of synthesized GraphQL text, wherever a surface runs them."""

    def __init__(self) -> None:
        self.parsed: list[str] = []
        self.compiled = 0

    def parse_query(self, schema, query, *args, **kwargs):
        self.parsed.append(query)
        return real_parse_query(schema, query, *args, **kwargs)

    def compile_query(self, *args, **kwargs):
        self.compiled += 1
        return real_compile_query(*args, **kwargs)


@pytest.fixture
def surface(monkeypatch):
    schema, ctx = _build_test_schema()
    state = SimpleNamespace(
        schemas={_ROLE: schema},
        contexts={_ROLE: ctx},
        rls_contexts={_ROLE: RLSContext.empty()},
        roles={_ROLE: {"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]}},
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        approval_hook=None,
        kafka_table_configs={},
        source_types={"sales-pg": "postgresql"},
        table_path_maps={
            _ROLE: {
                "orders": {
                    "domain_id": "sales",
                    "table_name": "orders",
                    "write_ops": ["delete", "insert", "update"],
                    "write_returns_rows": True,
                },
                "customers": {
                    "domain_id": "sales",
                    "table_name": "customers",
                    "write_ops": ["delete", "insert", "update"],
                    "write_returns_rows": True,
                },
            }
        },
    )
    pipeline, compiles = _Pipeline(), _Compiles()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", pipeline.govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", pipeline.execute)
    # The synthesized text is parsed and compiled by the surface itself or by the helper both
    # surfaces share; the count covers every one of those call sites.
    for module in (rest_generator, jsonapi_generator, generated_plan):
        monkeypatch.setattr(module, "parse_query", compiles.parse_query, raising=False)
        monkeypatch.setattr(module, "compile_query", compiles.compile_query, raising=False)

    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = request.headers.get("x-provisa-role")
        return await call_next(request)

    app.include_router(rest_generator.create_rest_router(state))
    app.include_router(jsonapi_generator.create_jsonapi_router(state))
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(state=state, pipeline=pipeline, compiles=compiles, client=client)


def _page_of(link: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(link).query).items()}


# -- JSON:API: one statement per list request ------------------------------------------------------


async def test_jsonapi_list_request_sends_one_statement_and_no_count(surface):
    resp = await surface.client.get(
        "/data/jsonapi/sales/orders", params={"page[size]": "25"}, headers=_JSONAPI
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["data"]) == 25
    assert len(surface.pipeline.sql) == 1, surface.pipeline.sql
    assert not any("COUNT(" in s.upper() for s in surface.pipeline.sql), surface.pipeline.sql


async def test_jsonapi_page_links_come_from_the_page_itself(surface):
    """Without a total, ``next`` is known from one row past the page; ``last`` and ``meta.total``
    are not reported — never a page-length or estimated figure in their place."""

    async def _page(number: int) -> dict:
        resp = await surface.client.get(
            "/data/jsonapi/sales/orders",
            params={"page[number]": str(number), "page[size]": "25"},
            headers=_JSONAPI,
        )
        assert resp.status_code == 200, resp.text
        return resp.json()

    first, second, third = await _page(1), await _page(2), await _page(3)

    assert [r["id"] for r in first["data"]] == [str(i) for i in range(1, 26)]
    assert [r["id"] for r in second["data"]] == [str(i) for i in range(26, 51)]
    assert [r["id"] for r in third["data"]] == [str(i) for i in range(51, 61)]
    assert first["data"][0]["attributes"]["amount"] == 1.0
    assert first["data"][0]["relationships"]["customer"]["data"] == {"type": "customer", "id": "1"}

    assert first["links"]["prev"] is None
    assert _page_of(first["links"]["next"]) == {"page[number]": "2", "page[size]": "25"}
    assert _page_of(second["links"]["prev"])["page[number]"] == "1"
    assert _page_of(second["links"]["next"])["page[number]"] == "3"
    assert _page_of(third["links"]["prev"])["page[number]"] == "2"
    assert third["links"]["next"] is None
    for doc in (first, second, third):
        assert _page_of(doc["links"]["first"])["page[number]"] == "1"
        assert "last" not in doc["links"]
        assert "total" not in doc.get("meta", {})


async def test_jsonapi_full_last_page_has_no_next(surface):
    """60 rows in pages of 30: page 2 is full and is the last one."""
    resp = await surface.client.get(
        "/data/jsonapi/sales/orders",
        params={"page[number]": "2", "page[size]": "30"},
        headers=_JSONAPI,
    )
    body = resp.json()
    assert len(body["data"]) == 30
    assert body["links"]["next"] is None


async def test_the_probe_row_never_carries_a_page_over_the_redirect_threshold(surface, monkeypatch):
    """REQ-1224: a buffered result above the redirect threshold is landed, not inlined. A page
    exactly at the threshold stays inline, so it is fetched without the extra row and reports a
    next page because it is full."""
    monkeypatch.setenv("PROVISA_REDIRECT_ENABLED", "true")
    monkeypatch.setenv("PROVISA_REDIRECT_THRESHOLD", "30")

    async def _fetch(size: int) -> tuple[int, dict]:
        resp = await surface.client.get(
            "/data/jsonapi/sales/orders", params={"page[size]": str(size)}, headers=_JSONAPI
        )
        assert resp.status_code == 200, resp.text
        return surface.pipeline.statements[-1][1][0], resp.json()

    limit, body = await _fetch(29)
    assert limit == 30 and len(body["data"]) == 29
    limit, body = await _fetch(30)
    assert limit == 30 and len(body["data"]) == 30
    assert _page_of(body["links"]["next"])["page[number]"] == "2"


async def test_jsonapi_total_is_counted_when_the_request_asks(surface):
    """``page[total]=true``: the governed COUNT(*) wrapper (REQ-1197) runs, and the document
    carries the exact total and the ``last`` link it makes known."""
    resp = await surface.client.get(
        "/data/jsonapi/sales/orders",
        params={"page[number]": "1", "page[size]": "25", "page[total]": "true"},
        headers=_JSONAPI,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    counts = [s for s in surface.pipeline.sql if "COUNT(" in s.upper()]
    assert len(surface.pipeline.sql) == 2, surface.pipeline.sql
    assert len(counts) == 1
    assert counts[0].startswith("SELECT COUNT(*) AS total FROM (")
    assert counts[0].endswith(") AS _provisa_count")

    assert body["meta"]["total"] == _TABLE_ROWS
    assert [r["id"] for r in body["data"]] == [str(i) for i in range(1, 26)]
    assert _page_of(body["links"]["last"])["page[number]"] == "3"
    assert _page_of(body["links"]["next"])["page[number]"] == "2"
    assert body["links"]["prev"] is None
    # Every link keeps the request's own choice, so following one counts again.
    for name in ("self", "first", "next", "last"):
        assert _page_of(body["links"][name])["page[total]"] == "true", name


async def test_jsonapi_rejects_a_malformed_total_flag(surface):
    resp = await surface.client.get(
        "/data/jsonapi/sales/orders", params={"page[total]": "maybe"}, headers=_JSONAPI
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["errors"][0]["source"] == {"parameter": "page[total]"}
    assert surface.pipeline.sql == []


# -- a repeated request compiles once --------------------------------------------------------------


@pytest.mark.parametrize(
    "path, params, headers",
    [
        ("/data/rest/sales/orders", {"limit": "25"}, _REST),
        (
            "/data/rest/sales/orders",
            {"filter": '[{"field":"id","comparator":"eq","value":"42"}]'},
            _REST,
        ),
        ("/data/jsonapi/sales/orders", {"page[size]": "25"}, _JSONAPI),
        ("/data/jsonapi/sales/orders", {"filter[id]": "42"}, _JSONAPI),
        ("/data/jsonapi/sales/orders", {"page[size]": "25", "page[total]": "true"}, _JSONAPI),
    ],
    ids=["rest-list", "rest-point", "jsonapi-list", "jsonapi-point", "jsonapi-list-total"],
)
async def test_a_repeated_request_is_parsed_and_compiled_once(surface, path, params, headers):
    first = await surface.client.get(path, params=params, headers=headers)
    assert first.status_code == 200, first.text
    parsed, compiled = len(surface.compiles.parsed), surface.compiles.compiled
    assert parsed >= 1 and compiled >= 1
    statements = list(surface.pipeline.statements)

    for _ in range(3):
        again = await surface.client.get(path, params=params, headers=headers)
        assert again.status_code == 200, again.text
        assert again.json() == first.json()

    assert len(surface.compiles.parsed) == parsed, surface.compiles.parsed
    assert surface.compiles.compiled == compiled
    # The kept form drives the same statements, with the same bound values, every time.
    assert surface.pipeline.statements == statements * 4


async def test_a_kept_request_plan_is_not_served_to_another_request(surface):
    """Two point lookups differ only in a value: each gets its own row."""

    async def _lookup(order_id: int) -> list[dict]:
        resp = await surface.client.get(
            "/data/rest/sales/orders",
            params={
                "filter": f'[{{"field":"id","comparator":"eq","value":"{order_id}"}}]',
                "fields": "id,region",
            },
            headers=_REST,
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["data"]

    assert await _lookup(7) == [{"id": 7, "region": "US"}]
    assert await _lookup(8) == [{"id": 8, "region": "EU"}]
    assert await _lookup(7) == [{"id": 7, "region": "US"}]
    assert [p for _, p in surface.pipeline.statements] == [[7], [8], [7]]


async def test_a_new_schema_generation_compiles_again(surface):
    path, params = "/data/rest/sales/orders", {"limit": "5"}
    await surface.client.get(path, params=params, headers=_REST)
    compiled = surface.compiles.compiled
    await surface.client.get(path, params=params, headers=_REST)
    assert surface.compiles.compiled == compiled

    surface.state.schema_version += 1
    resp = await surface.client.get(path, params=params, headers=_REST)
    assert resp.status_code == 200, resp.text
    assert surface.compiles.compiled == 2 * compiled


async def test_a_replaced_schema_compiles_again(surface):
    """A rebuild swaps the role's schema object before it bumps the generation: the kept form
    was validated against the old one and must not answer."""
    path, params = "/data/jsonapi/sales/orders", {"page[size]": "5"}
    await surface.client.get(path, params=params, headers=_JSONAPI)
    await surface.client.get(path, params=params, headers=_JSONAPI)
    compiled = surface.compiles.compiled

    schema, ctx = _build_test_schema()
    surface.state.schemas = {_ROLE: schema}
    resp = await surface.client.get(path, params=params, headers=_JSONAPI)
    assert resp.status_code == 200, resp.text
    assert surface.compiles.compiled > compiled


async def test_an_invalid_request_is_rejected_every_time_and_nothing_is_kept(surface):
    for _ in range(2):
        resp = await surface.client.get(
            "/data/rest/sales/orders", params={"fields": "no_such_column"}, headers=_REST
        )
        assert resp.status_code == 400, resp.text
    assert len(surface.compiles.parsed) == 2
    assert surface.pipeline.sql == []
    assert len(surface.state.compiled_query_cache) == 0


# -- one kept plan per request shape: values stay bound --------------------------------------------


def _rest_filter(field: str, op: str, value) -> dict:
    import json

    return {"filter": json.dumps([{"field": field, "comparator": op, "value": value}])}


async def test_rest_point_lookups_on_different_ids_compile_once(surface):
    """The id is a bound value, not part of the kept plan: one compile serves every id."""
    first = await surface.client.get(
        "/data/rest/sales/orders", params=_rest_filter("id", "eq", "1"), headers=_REST
    )
    assert first.status_code == 200, first.text
    parsed, compiled = len(surface.compiles.parsed), surface.compiles.compiled

    for order_id in range(2, 12):
        resp = await surface.client.get(
            "/data/rest/sales/orders", params=_rest_filter("id", "eq", str(order_id)), headers=_REST
        )
        assert resp.status_code == 200, resp.text
        assert [r["id"] for r in resp.json()["data"]] == [order_id]

    assert len(surface.compiles.parsed) == parsed, surface.compiles.parsed
    assert surface.compiles.compiled == compiled
    # One statement text for every id; the id travels as its bound value.
    assert len(set(surface.pipeline.sql)) == 1
    assert '"id" = $1' in surface.pipeline.sql[0]
    assert [p for _, p in surface.pipeline.statements] == [[i] for i in range(1, 12)]


async def test_jsonapi_point_lookups_on_different_ids_compile_once(surface):
    first = await surface.client.get(
        "/data/jsonapi/sales/orders", params={"filter[id]": "1"}, headers=_JSONAPI
    )
    assert first.status_code == 200, first.text
    parsed, compiled = len(surface.compiles.parsed), surface.compiles.compiled

    for order_id in range(2, 12):
        resp = await surface.client.get(
            "/data/jsonapi/sales/orders", params={"filter[id]": str(order_id)}, headers=_JSONAPI
        )
        assert resp.status_code == 200, resp.text
        assert [r["id"] for r in resp.json()["data"]] == [str(order_id)]

    assert len(surface.compiles.parsed) == parsed, surface.compiles.parsed
    assert surface.compiles.compiled == compiled
    assert len(set(surface.pipeline.sql)) == 1


async def test_jsonapi_pages_of_one_list_compile_once(surface):
    """Page size and offset are bound values: pages 2..N of a list share one kept plan."""

    async def _page(number: int) -> list[str]:
        resp = await surface.client.get(
            "/data/jsonapi/sales/orders",
            params={"page[number]": str(number), "page[size]": "10"},
            headers=_JSONAPI,
        )
        assert resp.status_code == 200, resp.text
        return [r["id"] for r in resp.json()["data"]]

    assert await _page(2) == [str(i) for i in range(11, 21)]
    parsed = len(surface.compiles.parsed)
    for number in (3, 4, 5, 6):
        first_id = (number - 1) * 10 + 1
        assert await _page(number) == [str(i) for i in range(first_id, first_id + 10)]
    assert len(surface.compiles.parsed) == parsed, surface.compiles.parsed


async def test_string_values_rotate_on_one_kept_plan(surface):
    params = {"fields": "id,region", "limit": "3"}
    us = await surface.client.get(
        "/data/rest/sales/orders",
        params={**params, **_rest_filter("region", "eq", "US")},
        headers=_REST,
    )
    parsed = len(surface.compiles.parsed)
    eu = await surface.client.get(
        "/data/rest/sales/orders",
        params={**params, **_rest_filter("region", "eq", "EU")},
        headers=_REST,
    )
    assert us.status_code == 200 and eu.status_code == 200, (us.text, eu.text)
    assert len(surface.compiles.parsed) == parsed
    assert [p[0] for _, p in surface.pipeline.statements] == ["US", "EU"]
    assert surface.pipeline.sql[0] == surface.pipeline.sql[1]


async def test_a_different_shape_compiles_separately(surface):
    """Another operator, another column, another fieldset or a list of another length is another
    statement: each is compiled for itself and never answered from a neighbour's plan."""
    requests = [
        _rest_filter("id", "eq", "5"),
        _rest_filter("id", "gt", "5"),
        _rest_filter("customerId", "eq", "5"),
        {**_rest_filter("id", "eq", "5"), "fields": "id"},
        _rest_filter("region", "in", ["US", "EU"]),
        _rest_filter("region", "in", ["US", "EU", "APAC"]),
    ]
    statements = []
    for params in requests:
        before = len(surface.compiles.parsed)
        resp = await surface.client.get("/data/rest/sales/orders", params=params, headers=_REST)
        assert resp.status_code == 200, resp.text
        assert len(surface.compiles.parsed) > before, params
        statements.append(surface.pipeline.sql[-1])
    assert len(set(statements)) == len(requests)


async def test_a_value_the_compiler_writes_into_the_statement_is_never_bound(surface):
    """An ISO date compiles to a TIMESTAMP literal, any other string to a bound value: the two
    are different statements, so a plan kept for one never serves the other."""
    plain = await surface.client.get(
        "/data/rest/sales/orders",
        params={"fields": "id", **_rest_filter("createdAt", "eq", "yesterday")},
        headers=_REST,
    )
    dated = await surface.client.get(
        "/data/rest/sales/orders",
        params={"fields": "id", **_rest_filter("createdAt", "eq", "2026-01-01")},
        headers=_REST,
    )
    other_day = await surface.client.get(
        "/data/rest/sales/orders",
        params={"fields": "id", **_rest_filter("createdAt", "eq", "2026-02-02")},
        headers=_REST,
    )
    assert plain.status_code == dated.status_code == other_day.status_code == 200
    plain_sql, dated_sql, other_sql = surface.pipeline.sql
    assert '"created_at" = $1' in plain_sql
    assert "TIMESTAMP '2026-01-01'" in dated_sql
    assert "TIMESTAMP '2026-02-02'" in other_sql
    assert [p for _, p in surface.pipeline.statements] == [["yesterday"], [], []]


async def test_a_value_the_schema_rejects_is_rejected_after_a_plan_is_kept(surface):
    """A plan kept for a valid id does not let an id the schema rejects through."""
    ok = await surface.client.get(
        "/data/rest/sales/orders", params=_rest_filter("id", "eq", "5"), headers=_REST
    )
    assert ok.status_code == 200, ok.text
    for bad in ("99999999999999", "5.5", "five"):
        resp = await surface.client.get(
            "/data/rest/sales/orders", params=_rest_filter("id", "eq", bad), headers=_REST
        )
        assert resp.status_code == 400, (bad, resp.text)
    assert len(surface.pipeline.sql) == 1


async def test_a_value_with_a_quote_never_binds_into_a_kept_plan(surface):
    """Synthesized text a value breaks out of is not the shape a plan was kept for."""
    ok = await surface.client.get(
        "/data/rest/sales/orders",
        params={"fields": "id", **_rest_filter("region", "eq", "US")},
        headers=_REST,
    )
    assert ok.status_code == 200, ok.text
    parsed = len(surface.compiles.parsed)
    resp = await surface.client.get(
        "/data/rest/sales/orders",
        params={"fields": "id", **_rest_filter("region", "eq", 'US"}, id: {eq: 3')},
        headers=_REST,
    )
    # Whatever the schema makes of that text, it was parsed for itself.
    assert len(surface.compiles.parsed) > parsed
    assert resp.status_code in (200, 400)


@pytest.mark.parametrize(
    "path, headers",
    [("/data/rest/sales/orders", _REST), ("/data/jsonapi/sales/orders", _JSONAPI)],
    ids=["rest", "jsonapi"],
)
async def test_an_internal_failure_is_not_answered_as_a_bad_request(surface, path, headers):
    """Only text the schema rejects is the caller's error. A failure inside the server — here a
    state with no plan store — is raised, never restated as a 400."""
    del surface.state.compiled_query_cache
    with pytest.raises(AttributeError, match="compiled_query_cache"):
        await surface.client.get(path, headers=headers)


# -- REST issues no count ---------------------------------------------------------------------------


async def test_rest_list_request_sends_one_statement_and_no_count(surface):
    resp = await surface.client.get(
        "/data/rest/sales/orders", params={"limit": "25"}, headers=_REST
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["data"]) == 25
    assert len(surface.pipeline.sql) == 1, surface.pipeline.sql
    assert "COUNT(" not in surface.pipeline.sql[0].upper()


# -- the request the NL layer writes ---------------------------------------------------------------


async def test_the_nl_generated_request_reports_the_total(surface):
    """The NL layer's JSON:API request always asks for the total; served as written."""
    from provisa.nl.runner import _generate_jsonapi_query

    node = SimpleNamespace(type_name="Orders", domain_id="sales", table_name="orders")
    url, error = _generate_jsonapi_query(None, {"Orders"}, {"Orders": node})
    assert error is None and url is not None

    resp = await surface.client.get(url, headers=_JSONAPI)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["data"]) == 20
    assert body["meta"]["total"] == _TABLE_ROWS
    assert _page_of(body["links"]["last"])["page[number]"] == "3"


@pytest.mark.parametrize(
    ("path", "params", "headers"),
    [
        ("/data/rest/sales/orders", {"limit": "5"}, _REST),
        ("/data/jsonapi/sales/orders", {"page[size]": "5"}, _JSONAPI),
        ("/data/jsonapi/sales/orders", {"page[size]": "5", "page[total]": "true"}, _JSONAPI),
    ],
    ids=["rest", "jsonapi", "jsonapi-total"],
)
async def test_a_statement_that_outruns_its_deadline_is_a_504(
    surface, monkeypatch, path, params, headers
):
    """REQ-1905: the pipeline fails a statement at its request deadline with a message naming the
    transport and the setting; REST and JSON:API report it as a gateway timeout, not a 500."""

    from provisa.core.request_deadline import RequestTimedOut

    async def _timed_out(plan, state):
        raise RequestTimedOut("http", 60.0, "limits.request_timeouts.http")

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _timed_out)
    resp = await surface.client.get(path, params=params, headers=headers)
    assert resp.status_code == 504, resp.text
    assert "http request exceeded its 60s request timeout (limits.request_timeouts.http)" in (
        resp.text
    )

    async def _stopping(plan, state):
        raise TimeoutError("the server is stopping")

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _stopping)
    resp = await surface.client.get(path, params=params, headers=headers)
    assert resp.status_code == 504 and "the server is stopping" in resp.text


def test_the_timeout_error_carries_the_params_its_catalog_string_renders():
    """``data.query_timeout`` renders as `Query timed out after {{timeout_s}}s`: the error an
    HTTP surface raises carries that param. A timeout with no request timeout behind it (the
    server stopping) has its own code."""
    from provisa.api.errors import timeout_error
    from provisa.core.request_deadline import RequestTimedOut

    outran = timeout_error(RequestTimedOut("http", 60.0, "limits.request_timeouts.http"))
    assert (outran.status_code, outran.code) == (504, "data.query_timeout")
    assert outran.params == {
        "timeout_s": "60",
        "transport": "http",
        "setting": "limits.request_timeouts.http",
    }
    stopping = timeout_error(TimeoutError("the server is stopping"))
    assert (stopping.status_code, stopping.code) == (504, "data.request_interrupted")
    assert stopping.params == {"error": "the server is stopping"}
