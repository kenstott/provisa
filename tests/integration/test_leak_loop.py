# Copyright (c) 2026 Kenneth Stott
# Canary: b83fe37d-ed33-4992-85f0-72c4b1e3d65e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2149: one server, many requests, and nothing it holds grows.

One server process on the local platform (an embedded PostgreSQL control plane, DuckDB) serves a
loop of every surface -- GraphQL, SQL, Cypher, SPARQL, REST, JSON:API, pgwire and Arrow Flight
reads, a write (an insert and its delete, so the table stays as it was), a subscription opened and
closed, a sign-in -- for a warm-up, then N times, then N more. Over the second N its resident
memory, file descriptors, threads, sockets and asyncio tasks grow by no more than the slack stated
below. Then K orgs are made, used and deleted, and every map the process keeps by org (the set an
org's deletion clears, provisa/core/org_forget.py) is the size it was before them. A failure names
what grew, and for memory the allocation sites under provisa/ that grew most (tracemalloc)."""

# Requirements: REQ-2149

from __future__ import annotations

import json
import os
import time
import urllib.parse
from typing import Any

import psutil
import pytest

from tests.integration import tenancy_e2e_support as tenancy
from tests.integration.worker_boot_harness import FLIGHT_QUERY, WorkerBoot

pytestmark = [pytest.mark.integration, pytest.mark.local_platform, pytest.mark.leak]

PUBLIC_URL = "http://leak.test"
# The server's native pools (DuckDB, Arrow, the allocator) settle over the first ~60 passes
# (measured: resident memory rose to ~900 MiB, then held).
WARMUP = int(os.environ.get("PROVISA_LEAK_WARMUP", "60"))
N = int(os.environ.get("PROVISA_LEAK_N", "40"))
K = int(os.environ.get("PROVISA_LEAK_K", "5"))
# What the second N may add. The program's own Python allocations are exact, so they are the leak
# signal: a pass is a dozen requests, and an object kept per request is hundreds over N.
TRACED_SLACK = 256 * 2**10
# Resident memory swings by +-150 MiB from pass to pass with no trend (native pools): it is judged
# by its least-squares slope over the second N, which a native leak raises and a swing does not.
RSS_SLOPE_MAX = 1 * 2**20  # bytes per pass
SLACK = {"fds": 4, "threads": 2, "sockets": 4, "tasks": 3}
_ROLE = "org_admin"
_COLUMNS = [
    {"name": "id", "data_type": "integer", "visible_to": [_ROLE, tenancy.MEMBER_ROLE]},
    {"name": "region", "data_type": "varchar", "visible_to": [_ROLE, tenancy.MEMBER_ROLE]},
]


def _boot() -> WorkerBoot:
    boot = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        extra_config={
            "multitenancy": True,
            "auth": {
                "provider": "basic",
                "jwt_secret": "leak-loop-signing-key-not-a-secret",
                "bootstrap_superadmin": True,
                "assignments_source": "provisa",
                "superuser": {"username": "breakglass", "password": "open sesame, operator"},
                # pgwire signs a password session in as this role (its client names no role).
                "default_role": _ROLE,
            },
            "roles": [
                {
                    "id": tenancy.MEMBER_ROLE,
                    "description": "Test role.",
                    "capabilities": ["query_development"],
                    "domain_access": ["*"],
                }
            ],
            "tables": [
                {
                    "source_id": "sales-pg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "orders",
                    "columns": _COLUMNS,
                    "write_grants": [
                        {"role": _ROLE, "operation": "insert", "columns": ["id", "region"]},
                        {"role": _ROLE, "operation": "delete", "rows": "TRUE"},
                    ],
                }
            ],
        },
        env={
            "PROVISA_REDIRECT_ENABLED": "false",
            "PROVISA_LEAK_PROBE": "1",
            # One frame: the probe groups growth by allocation site (file and line), and a whole
            # server's snapshot with deeper stacks took minutes.
            "PYTHONTRACEMALLOC": "1",
            "PROVISA_PUBLIC_URL": PUBLIC_URL,
        },
    )
    boot.create_database()
    boot.start()
    boot.wait_all_ready(timeout=300)
    return boot


@pytest.fixture(scope="module")
def server():
    boot = _boot()
    try:
        operator = tenancy.operator_token(boot)
        tenancy.register(boot, operator, "founder", "founder@leak.test")
        founder = tenancy.sign_in(boot, "founder")
        tenancy.claim_platform_admin(boot, founder)
        admin = tenancy.org_admin(boot, operator, founder, boot.org_id)
        yield boot, founder, admin
    finally:
        boot.cleanup()


def _worker(boot: WorkerBoot) -> psutil.Process:
    (pid,) = boot.worker_pids()
    return psutil.Process(pid)


def _probe(boot: WorkerBoot, token: str) -> dict[str, Any]:
    status, body = tenancy.call(
        boot, "GET", "/debug/leak-probe?top=12", token=token, org=boot.org_id
    )
    assert status == 200, tenancy.said(boot, "leak probe", status, body)
    return body


def _measure(boot: WorkerBoot, token: str) -> dict[str, Any]:
    proc = _worker(boot)
    probe = _probe(boot, token)  # collects the process's garbage first: read after it
    return {
        "rss_bytes": proc.memory_info().rss,
        "fds": proc.num_fds(),
        "threads": proc.num_threads(),
        "sockets": len(proc.net_connections(kind="inet")),
        "tasks": probe["tasks"],
        "traced_bytes": probe["traced_bytes"],
        "org_maps": probe["org_maps"],
        "grew": probe["grew"],
    }


class _Surfaces:
    """The names each surface serves the one table under, read from the server itself."""

    def __init__(self, boot: WorkerBoot, token: str) -> None:
        self.boot = boot
        org = boot.org_id
        status, body = tenancy.call(
            boot,
            "POST",
            "/data/graphql",
            token=token,
            org=org,
            body={"query": "{ __schema { queryType { fields { name } } } }"},
        )
        assert status == 200, tenancy.said(boot, "graphql schema", status, body)
        fields = [f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]]
        self.gql_field = next(f for f in fields if f.endswith("orders"))
        status, body = tenancy.call(
            boot, "POST", "/data/cypher", token=token, org=org, body={"query": "CALL db.labels()"}
        )
        assert status == 200, tenancy.said(boot, "cypher labels", status, body)
        self.label = next(r["label"] for r in body["rows"] if r["label"].lower() == "orders")
        status, body = tenancy.call(boot, "GET", "/data/rest/openapi.json", token=token, org=org)
        assert status == 200, tenancy.said(boot, "rest openapi", status, body)
        self.rest_path = next(p for p in body["paths"] if p.endswith("orders") and "{" not in p)

    def once(self, token: str, iteration: int) -> None:
        """One pass of every surface, as the signed-in ``token``."""
        boot, org = self.boot, self.boot.org_id

        def ok(what: str, status: int, body: Any) -> None:
            assert 200 <= status < 300, tenancy.said(boot, what, status, body)

        ok(
            "graphql",
            *tenancy.call(
                boot,
                "POST",
                "/data/graphql",
                token=token,
                org=org,
                body={"query": f"{{ {self.gql_field} {{ id region }} }}"},
            ),
        )
        ok(
            "sql",
            *tenancy.call(
                boot,
                "POST",
                "/data/sql",
                token=token,
                org=org,
                body={"sql": "SELECT id, region FROM sales.orders"},
            ),
        )
        ok(
            "cypher",
            *tenancy.call(
                boot,
                "POST",
                "/data/cypher",
                token=token,
                org=org,
                body={"query": f"MATCH (o:{self.label}) RETURN o.id AS id LIMIT 5"},
            ),
        )
        sparql = urllib.parse.quote(
            f"SELECT ?x WHERE {{ ?x a <{PUBLIC_URL}/vocab/sales/orders> }} LIMIT 2"
        )
        ok(
            "sparql",
            *tenancy.call(
                boot,
                "GET",
                f"/data/sparql?query={sparql}",
                token=token,
                org=org,
            ),
        )
        ok("rest", *tenancy.call(boot, "GET", f"/data/rest{self.rest_path}", token=token, org=org))
        ok(
            "jsonapi",
            *tenancy.call(boot, "GET", "/data/jsonapi/sales/orders", token=token, org=org),
        )
        row = 1000 + iteration
        ok(
            "insert",
            *tenancy.call(
                boot,
                "POST",
                "/data/sql",
                token=token,
                org=org,
                body={"sql": f"INSERT INTO sales.orders (id, region) VALUES ({row}, 'leak')"},
            ),
        )
        ok(
            "delete",
            *tenancy.call(
                boot,
                "POST",
                "/data/sql",
                token=token,
                org=org,
                body={"sql": f"DELETE FROM sales.orders WHERE id = {row}"},
            ),
        )
        _subscription(boot, token)
        _pgwire(boot, token)
        _flight(boot, token)


def _subscription(boot: WorkerBoot, token: str) -> None:
    """Open the table's change stream, read its first bytes, and close it."""
    import urllib.request

    request = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/subscribe/orders",
        headers={"Authorization": f"Bearer {token}", "X-Org-Provisa": boot.org_id},
    )
    with urllib.request.urlopen(request, timeout=30) as stream:
        assert stream.status == 200
        stream.read1(1)  # the stream answered; closing it is the client leaving


def _pgwire(boot: WorkerBoot, token: str) -> None:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        dbname=boot.org_id,
        user="admin-of-" + boot.org_id,
        password=tenancy.PASSWORD,
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        rows = conn.execute("SELECT id, region FROM sales.orders").fetchall()
    assert rows


def _flight(boot: WorkerBoot, token: str) -> None:
    import pyarrow.flight as flight

    # A ticket carries its own credential and the org it is for (api/flight/server.py do_get).
    request = {"query": FLIGHT_QUERY, "role": _ROLE, "token": token, "org": boot.org_id}
    client = flight.FlightClient(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = flight.Ticket(json.dumps(request).encode())
        table = client.do_get(ticket, flight.FlightCallOptions(timeout=30)).read_all()
        assert table.num_rows > 0
    finally:
        client.close()


def _growth(before: dict, after: dict) -> dict[str, int]:
    return {k: after[k] - before[k] for k in SLACK}


def _slope(points: list[tuple[int, int]]) -> float:
    """The least-squares slope of ``points`` (pass, bytes): bytes gained per pass."""
    assert len(points) >= 3, f"too few samples to fit a slope: {points}"
    mx = sum(x for x, _ in points) / len(points)
    my = sum(y for _, y in points) / len(points)
    return sum((x - mx) * (y - my) for x, y in points) / sum((x - mx) ** 2 for x, _ in points)


def test_nothing_grows_over_a_mixed_loop_and_org_churn(server):
    boot, founder, admin = server
    surfaces = _Surfaces(boot, admin)

    series: list[tuple[int, int, int]] = []  # (pass, rss, traced) every 5 passes

    def run(start: int, count: int) -> None:
        for i in range(start, start + count):
            token = tenancy.sign_in(boot, "admin-of-" + boot.org_id)  # a sign-in per pass
            surfaces.once(token, i)
            # Signing out is the client discarding its bearer: nothing of it is held to close.
            if i % 5 == 4:
                m = _measure(boot, admin)
                series.append((i + 1, m["rss_bytes"], m["traced_bytes"]))

    run(0, WARMUP + N)
    _probe(boot, admin)  # the tracemalloc baseline is the snapshot this call takes
    before = _measure(boot, admin)
    run(WARMUP + N, N)
    after = _measure(boot, admin)
    grown = _growth(before, after)
    traced = after["traced_bytes"] - before["traced_bytes"]
    second = [(n, r) for n, r, _ in series if n > WARMUP + N]
    slope = _slope(second)
    # Said on every run, so a slack that is tight or loose is seen before it fails.
    print(
        f"\nleak loop: over the second {N} passes {grown} (slack {SLACK}), Python "
        f"{traced} B (slack {TRACED_SLACK}), resident slope {slope:.0f} B/pass "
        f"(at most {RSS_SLOPE_MAX}); "
        "resident/Python MiB by pass: "
        + ", ".join(f"{n}: {r / 2**20:.0f}/{t / 2**20:.1f}" for n, r, t in series)
        + "\ntop allocation sites that grew:\n"
        + "\n".join(
            f"  {g['size_diff']:>9} B {g['count_diff']:>6}  {g['site']}" for g in after["grew"]
        )
    )
    over = {k: v for k, v in grown.items() if v > SLACK[k]}
    if traced > TRACED_SLACK:
        over["python_bytes"] = traced
    if slope > RSS_SLOPE_MAX:
        over["resident_bytes_per_pass"] = round(slope)
    assert not over, (
        f"over {N} passes after warm-up, grew past the slack: {over}\n"
        f"resident and Python memory by pass (MiB): "
        + ", ".join(f"{n}: {r / 2**20:.0f}/{t / 2**20:.1f}" for n, r, t in series)
        + "\n"
        "top allocation sites that grew:\n"
        + "\n".join(
            f"  {g['size_diff']:>9} B  {g['count_diff']:>6}  {g['site']}" for g in after["grew"]
        )
    )

    # K orgs made, used and deleted: every org-kept map is the size it was before them.
    maps_before = _measure(boot, admin)["org_maps"]
    for k in range(K):
        org_id = f"leak{k}{int(time.time())}"
        status, body = tenancy.call(
            boot,
            "POST",
            "/admin/orgs/",
            token=founder,
            body={"id": org_id, "name": org_id, "include_demo": False},
        )
        assert status == 200, tenancy.said(boot, "create org", status, body)
        _ready(boot, founder, org_id)
        status, body = tenancy.call(
            boot, "POST", "/data/sql", token=founder, org=org_id, body={"sql": "SELECT 1 AS one"}
        )
        status, body = tenancy.call(
            boot, "DELETE", f"/admin/orgs/{org_id}?confirm={org_id}", token=founder, org=org_id
        )
        assert status == 200, tenancy.said(boot, "delete org", status, body)
    maps_after = _measure(boot, admin)["org_maps"]
    assert maps_after == maps_before, (
        f"org-kept maps after {K} orgs came and went: {json.dumps(maps_after)} "
        f"(before: {json.dumps(maps_before)})"
    )


def _ready(boot: WorkerBoot, token: str, org_id: str, timeout: float = 180.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = tenancy.call(boot, "GET", f"/admin/orgs/{org_id}/status", token=token)
        if status == 200 and body.get("provisioning_state") == "ready":
            return
        assert body.get("provisioning_state") != "failed", body
        time.sleep(1)
    raise AssertionError(f"org {org_id} not ready in {timeout}s")
