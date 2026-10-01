# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a9c15-4b2d-4f68-9d0e-8a1c5b7f2e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: native gRPC serves an opted-in read from the response cache on BOTH routes, and a
repeated request shape is compiled once (REQ-544, REQ-1897, REQ-1877).

One real isolated server (``main:app`` plus the test-only driver/sqlglot tap; DuckDB engine; the
stack's Redis as the response cache) over two tables: ``orders`` in the stack's Postgres — one
source on its own driver, the DIRECT route — and ``events`` in a SQLite file, a virtual source
the engine reads, the ENGINE route. The tap records every statement a source driver or the engine
receives, so a HIT is proven by the statement that never arrived:

- ``x-provisa-cache: true``: the first request runs at the source and writes one entry; the
  second returns the same rows and NO statement reaches the source or the engine;
- no hint: every request runs at the source and nothing is written;
- two read_masks, or two filters, never serve each other's entry;
- a repeated request tokenizes and copies no SQL at all — its statement was compiled once;
- filter values are bound parameters, so requests of one shape with DIFFERENT filter values are
  still one compiled statement, each returning its own rows, each with its own cache entry;
- typed filters (timestamp, date, numeric, boolean) bind correctly at a Postgres source.

With ``$PROVISA_TEST_PROBE_OUT`` set, the repeat test also appends the server's CPU per request.
"""

# Requirements: REQ-544, REQ-1897, REQ-1877

from __future__ import annotations

import asyncio
import datetime
import decimal
import json
import os
import sqlite3
from collections import Counter
from pathlib import Path

import pytest
import yaml

from tests.integration.test_grpc_native_read_mask_e2e import _pg
from tests.integration.test_raw_sql_response_cache_e2e import _entry_keys

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "grpc_cache_plan_e2e"
_SCHEMA = "grpc_cache_plan_e2e_src"
_ROLE = "org_admin"
_HINT = (("x-provisa-cache", "true"),)
_ORDERS = [(1, "east", "open"), (2, "west", "open"), (3, "east", "closed"), (4, "north", "open")]
# orders' typed columns, by id: placed_at timestamp, ship_date date, amount numeric, shipped boolean.
_TYPED = {
    1: (datetime.datetime(2026, 1, 1, 8, 30, 0), datetime.date(2026, 1, 5), "10.50", True),
    2: (datetime.datetime(2026, 2, 1, 9, 0, 0), datetime.date(2026, 2, 5), "20.25", False),
    3: (datetime.datetime(2026, 3, 1, 10, 15, 0), datetime.date(2026, 3, 5), "30.75", True),
    4: (datetime.datetime(2026, 4, 1, 11, 45, 0), datetime.date(2026, 4, 5), "40.00", False),
}
_EVENTS = [(1, "east", "open"), (2, "west", "open"), (3, "east", "closed")]
_ROUTES = ("Orders", "Events")  # DIRECT (Postgres source), ENGINE (SQLite source)


def _seed_postgres() -> None:
    asyncio.run(
        _pg(
            [
                (f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', []),
                (f'CREATE SCHEMA "{_SCHEMA}"', []),
                (
                    f'CREATE TABLE "{_SCHEMA}".orders (id integer PRIMARY KEY, '
                    "region varchar, status varchar, placed_at timestamp, ship_date date, "
                    "amount numeric(10,2), shipped boolean)",
                    [],
                ),
                (
                    f'INSERT INTO "{_SCHEMA}".orders VALUES ($1, $2, $3, $4, $5, $6, $7)',
                    [
                        (
                            *order,
                            _TYPED[order[0]][0],
                            _TYPED[order[0]][1],
                            decimal.Decimal(_TYPED[order[0]][2]),
                            _TYPED[order[0]][3],
                        )
                        for order in _ORDERS
                    ],
                ),
            ]
        )
    )


def _seed_sqlite(db: Path) -> None:
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER, region TEXT, status TEXT)")
        con.executemany("INSERT INTO events VALUES (?, ?, ?)", _EVENTS)
        con.commit()
    finally:
        con.close()


def _config(work: Path) -> Path:
    db = work / "events.sqlite"
    _seed_sqlite(db)
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    pg_source = next(s for s in base["sources"] if s["id"] == "sales-pg")
    columns = [
        {"name": name, "data_type": data_type, "visible_to": [_ROLE]}
        for name, data_type in (("id", "integer"), ("region", "varchar"), ("status", "varchar"))
    ]
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [
        {**pg_source, "id": "cp-pg"},
        {"id": "cp-sqlite", "type": "sqlite", "path": str(db)},
    ]
    cfg["domains"] = [{"id": "sales-analytics", "description": "gRPC cache + plan reuse e2e"}]
    typed_columns = [
        {"name": name, "data_type": data_type, "visible_to": [_ROLE]}
        for name, data_type in (
            ("placed_at", "timestamp"),
            ("ship_date", "date"),
            ("amount", "numeric"),
            ("shipped", "boolean"),
        )
    ]
    cfg["tables"] = [
        {
            "source_id": "cp-pg",
            "domain_id": "sales-analytics",
            "schema": _SCHEMA,
            "table": "orders",
            "columns": columns + typed_columns,
        },
        {
            "source_id": "cp-sqlite",
            "domain_id": "sales-analytics",
            "schema": "default",
            "table": "events",
            "columns": columns,
        },
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


class _Server:
    """The isolated server, its tap, and one native gRPC channel."""

    def __init__(self, srv, tap: Path) -> None:
        import grpc

        from tests.grpc_proto_client import role_descriptor_pool

        self.srv = srv
        self.tap = tap
        _pool, svc = role_descriptor_pool(srv.base_url, _ROLE)
        self.service = svc
        self.channel = grpc.insecure_channel(f"127.0.0.1:{srv.grpc_port}")
        self._rpcs: dict[str, tuple] = {}

    def _rpc(self, table: str) -> tuple:
        from google.protobuf.message_factory import GetMessageClass

        if table not in self._rpcs:
            method = next(
                m
                for m in self.service.methods
                if m.name.startswith("Query") and m.name.endswith(table)
            )
            req_cls = GetMessageClass(method.input_type)
            resp_cls = GetMessageClass(method.output_type)
            call = self.channel.unary_stream(
                f"/{self.service.full_name}/{method.name}",
                request_serializer=req_cls.SerializeToString,
                response_deserializer=resp_cls.FromString,
            )
            self._rpcs[table] = (req_cls, call)
        return self._rpcs[table]

    def rows(
        self,
        table: str,
        limit: int,
        *,
        metadata: tuple = (),
        paths: tuple[str, ...] = (),
        filter_: dict | None = None,
    ) -> list:
        req_cls, call = self._rpc(table)
        request = req_cls(limit=limit)
        request.read_mask.paths.extend(paths)
        for name, value in (filter_ or {}).items():
            setattr(request.filter, name, value)
        return list(call(request, metadata=(("x-provisa-role", _ROLE), *metadata), timeout=120))

    def records(self) -> list[dict]:
        return [json.loads(line) for line in self.tap.read_text().splitlines() if line]

    def bound(self, seen: int, limit: int) -> list[list[str]]:
        """The values bound to each statement received since ``seen`` for this limit."""
        return [r["params"] for r in self._statement_records(seen, limit)]

    def _statement_records(self, seen: int, limit: int) -> list[dict]:
        """The limit is a bound value: it tells one request's statements from another's."""
        return [
            r
            for r in self.records()[seen:]
            if r["terminal"] in ("source", "engine") and repr(limit) in r["params"]
        ]

    def statements(self, seen: int, limit: int) -> list[str]:
        """The statements a source driver or the engine received since ``seen`` for this limit."""
        return [r["sql"] for r in self._statement_records(seen, limit)]

    def sqlglot_calls(self, seen: int) -> Counter:
        """``op at`` → count of the SQL tokenizations and tree copies since ``seen``."""
        return Counter(
            f"{r['op']} {r['at']}" for r in self.records()[seen:] if r["terminal"] == "sqlglot"
        )


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("grpc_cache_plan")
    tap = work / "tap.jsonl"
    tap.touch()
    _seed_postgres()
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        await_grpc=True,
        config=str(_config(work)),
        control_plane="postgres",
        app="tests.integration.driver_sql_tap_app:app",
        env={"PROVISA_TEST_DRIVER_SQL_TAP": str(tap)},
    )
    server = None
    try:
        srv.start(timeout=240)
        server = _Server(srv, tap)
        yield server
    finally:
        if server is not None:
            server.channel.close()
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg([(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', [])]))


def _ids(rows: list) -> list[int]:
    return sorted(r.id for r in rows)


def _expected(table: str) -> list[tuple]:
    return _ORDERS if table == "Orders" else _EVENTS


# One limit per (test, table): the limit is in the statement text, so each case starts from its
# own MISS and its statements are told apart in the tap.
_LIMITS = {"Orders": 0, "Events": 50}


def test_each_table_takes_the_route_this_module_names(server):
    """``orders`` is read on its source's own driver (DIRECT), ``events`` by the engine."""
    for table, terminal in (("Orders", "source"), ("Events", "engine")):
        limit = 8100 + _LIMITS[table]
        seen = len(server.records())
        server.rows(table, limit)
        terminals = {r["terminal"] for r in server._statement_records(seen, limit)}
        assert terminals == {terminal}, table


@pytest.mark.parametrize("table", _ROUTES)
def test_hinted_request_is_a_cache_hit_the_second_time(server, table):
    limit = 8200 + _LIMITS[table]
    entries = _entry_keys(_ORG)

    seen = len(server.records())
    miss = server.rows(table, limit, metadata=_HINT)
    assert len(server.statements(seen, limit)) == 1  # ran at the source
    assert len(_entry_keys(_ORG) - entries) == 1  # and wrote exactly one entry

    entries = _entry_keys(_ORG)
    seen = len(server.records())
    hit = server.rows(table, limit, metadata=_HINT)
    assert server.statements(seen, limit) == []  # nothing reached the source or the engine
    assert server.sqlglot_calls(seen) == Counter()  # answered before routing: no SQL derived
    assert _entry_keys(_ORG) == entries
    assert [(r.id, r.region, r.status) for r in hit] == [(r.id, r.region, r.status) for r in miss]
    assert sorted((r.id, r.region, r.status) for r in hit) == sorted(_expected(table))


@pytest.mark.parametrize("table", _ROUTES)
def test_unhinted_request_never_touches_the_cache(server, table):
    limit = 8300 + _LIMITS[table]
    entries = _entry_keys(_ORG)
    for _ in range(2):
        seen = len(server.records())
        rows = server.rows(table, limit)
        assert len(server.statements(seen, limit)) == 1
        assert _ids(rows) == [r[0] for r in _expected(table)]
    assert _entry_keys(_ORG) == entries


@pytest.mark.parametrize("table", _ROUTES)
def test_cache_ttl_metadata_alone_opts_in(server, table):
    limit = 8400 + _LIMITS[table]
    hint = (("x-provisa-cache-ttl", "60"),)
    server.rows(table, limit, metadata=hint)
    seen = len(server.records())
    assert _ids(server.rows(table, limit, metadata=hint)) == [r[0] for r in _expected(table)]
    assert server.statements(seen, limit) == []


@pytest.mark.parametrize("table", _ROUTES)
def test_two_masks_never_share_an_entry(server, table):
    limit = 8500 + _LIMITS[table]
    masks = (("id", "region"), ("id", "status"))
    for expect_statement in (1, 0):  # first pass: each mask misses; second: each hits its own
        for paths in masks:
            seen = len(server.records())
            rows = server.rows(table, limit, metadata=_HINT, paths=paths)
            assert len(server.statements(seen, limit)) == expect_statement, paths
            assert all({f.name for f, _v in r.ListFields()} == set(paths) for r in rows), paths
            assert _ids(rows) == [r[0] for r in _expected(table)]


@pytest.mark.parametrize("table", _ROUTES)
def test_two_filters_never_share_an_entry(server, table):
    limit = 8600 + _LIMITS[table]
    by_region = {
        region: sorted(r[0] for r in _expected(table) if r[1] == region)
        for region in ("east", "west")
    }
    for expect_statement in (1, 0):
        for region, ids in by_region.items():
            seen = len(server.records())
            rows = server.rows(table, limit, metadata=_HINT, filter_={"region": region})
            assert len(server.statements(seen, limit)) == expect_statement, region
            assert _ids(rows) == ids, region


@pytest.mark.parametrize("table", _ROUTES)
def test_a_repeated_request_is_compiled_once(server, table):
    """After the first request of a shape, a repeat derives nothing from SQL text: no
    tokenization (every parse starts with one) and no expression-tree copy anywhere in the
    server while the repeats run."""
    import psutil

    limit = 8700 + _LIMITS[table]
    repeats = 200
    server.rows(table, limit, paths=("id", "region"), filter_={"status": "open"})  # compiles

    process = psutil.Process(server.srv._proc.pid)
    cpu_before = sum(process.cpu_times()[:2])
    seen = len(server.records())
    for _ in range(repeats):
        rows = server.rows(table, limit, paths=("id", "region"), filter_={"status": "open"})
    cpu_per_request_ms = (sum(process.cpu_times()[:2]) - cpu_before) / repeats * 1000

    assert _ids(rows) == sorted(r[0] for r in _expected(table) if r[2] == "open")
    assert len(server.statements(seen, limit)) == repeats  # unhinted: each one ran at the source
    probe = os.environ.get("PROVISA_TEST_PROBE_OUT")
    if probe:
        calls = server.sqlglot_calls(seen)
        with open(probe, "a") as out:
            out.write(
                json.dumps(
                    {
                        "table": table,
                        "repeats": repeats,
                        "server_cpu_ms_per_request": round(cpu_per_request_ms, 3),
                        "sqlglot_calls_per_request": {
                            k: round(v / repeats, 2) for k, v in sorted(calls.items())
                        },
                    }
                )
                + "\n"
            )
    assert server.sqlglot_calls(seen) == Counter()


@pytest.mark.parametrize("table", _ROUTES)
def test_one_request_shape_over_rotating_filter_values_is_compiled_once(server, table):
    """Filter values are bound parameters: after the first request of a shape, requests with
    OTHER values derive nothing from SQL text, the source receives one statement text with each
    request's value bound, and every request returns its own rows."""
    limit = 8800 + _LIMITS[table]
    by_id = {r[0]: r for r in _expected(table)}
    ids = sorted(by_id)
    rounds = 25
    server.rows(table, limit, paths=("id", "region"), filter_={"id": ids[-1]})  # compiles

    unseen = list(range(1000, 1100))  # values no request has carried before, matching no row
    seen = len(server.records())
    for _ in range(rounds):
        for order_id in ids:
            rows = server.rows(table, limit, paths=("id", "region"), filter_={"id": order_id})
            assert [(r.id, r.region) for r in rows] == [(order_id, by_id[order_id][1])]
    for order_id in unseen:
        assert server.rows(table, limit, paths=("id", "region"), filter_={"id": order_id}) == []

    requests = rounds * len(ids) + len(unseen)
    calls = server.sqlglot_calls(seen)
    probe = os.environ.get("PROVISA_TEST_PROBE_OUT")
    if probe:
        with open(probe, "a") as out:
            out.write(
                json.dumps(
                    {
                        "probe": "rotating filter values",
                        "table": table,
                        "requests": requests,
                        "sqlglot_calls_per_request": {
                            k: round(v / requests, 2) for k, v in sorted(calls.items())
                        },
                    }
                )
                + "\n"
            )
    statements = server.statements(seen, limit)
    assert len(statements) == requests
    assert len(set(statements)) == 1, set(statements)  # one statement text for every value
    assert sorted(server.bound(seen, limit)) == sorted(
        [repr(i), repr(limit)] for i in [*ids * rounds, *unseen]
    )
    assert calls == Counter()


@pytest.mark.parametrize("table", _ROUTES)
def test_each_filter_value_has_its_own_cache_entry(server, table):
    """The response-cache key includes the bound values: a HIT for one value is never served to
    a request for another."""
    limit = 8900 + _LIMITS[table]
    by_id = {r[0]: r for r in _expected(table)}
    entries = _entry_keys(_ORG)
    for expect_statement in (1, 0):  # each value misses once, then hits its own entry
        for order_id, row in by_id.items():
            seen = len(server.records())
            rows = server.rows(table, limit, metadata=_HINT, filter_={"id": order_id})
            assert len(server.statements(seen, limit)) == expect_statement, order_id
            assert [(r.id, r.region, r.status) for r in rows] == [row[:3]], order_id
    assert len(_entry_keys(_ORG) - entries) == len(by_id)


@pytest.mark.parametrize(
    ("filter_", "ids"),
    [
        ({"placed_at": "2026-02-01 09:00:00"}, [2]),
        ({"placed_at": "2026-03-01T10:15:00"}, [3]),
        ({"ship_date": "2026-04-05"}, [4]),
        ({"amount": 10.5}, [1]),
        ({"shipped": True}, [1, 3]),
        ({"shipped": False}, [2, 4]),
        ({"shipped": False, "region": "west", "amount": 20.25}, [2]),
    ],
)
def test_typed_filters_bind_at_a_postgres_source(server, filter_, ids):
    """Timestamp and date values arrive as text, numeric as double, boolean as bool; each is bound
    — never inlined — and compares against its typed column."""
    limit = 9000
    seen = len(server.records())
    rows = server.rows("Orders", limit, paths=("id",), filter_=filter_)

    assert _ids(rows) == ids
    (statement,) = server.statements(seen, limit)
    assert not any(
        str(value) in statement for value in filter_.values() if isinstance(value, str)
    ), statement
    (bound,) = server.bound(seen, limit)
    assert len(bound) == len(filter_) + 1  # each filter value, then the row limit


@pytest.mark.parametrize("table", _ROUTES)
def test_another_limit_is_the_same_compiled_statement(server, table):
    """The row limit is bound: requests that differ only in their limit share one statement text
    and one compiled plan, and each returns its own number of rows."""
    base = 9100 + _LIMITS[table]
    server.rows(table, base)  # compiles
    seen = len(server.records())
    texts = set()
    for limit in (base + 1, base + 2, 2, 1):
        mark = len(server.records())
        rows = server.rows(table, limit)
        assert len(rows) == min(limit, len(_expected(table)))
        texts.update(server.statements(mark, limit))
    assert len(texts) == 1, texts
    assert not any(str(base + 1) in text for text in texts)
    assert server.sqlglot_calls(seen) == Counter()
