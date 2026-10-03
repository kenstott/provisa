# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2f8b40-9a1c-4e57-b3d8-0e7c5f1a2b96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the per-source live-read cap is cluster-wide (REQ-1909).

Two real isolated servers (DuckDB engine, the stack's Postgres control plane and Redis) serve the
same org. The SQLite source is read LIVE by DuckDB through its attach connector, and carries
``max_live_concurrency: 1``. Concurrent reads fired at BOTH servers never hold more than one permit
at a time — sampled straight from the Redis permit set — and every read still succeeds, having
queued for its turn. The admin API round-trips every "Load Management and Timeliness" source field
and refuses an invalid cap.
"""

# Requirements: REQ-1909

from __future__ import annotations

import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
import redis
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ROLE = "org_admin"
_ORG = "live_cap_e2e"
_SOURCE = "cap-sqlite"
_ROWS = 1_500_000
_SQL = "SELECT count(DISTINCT payload) AS n FROM live_cap.events"


def _seed(db: Path) -> None:
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER, payload TEXT)")
        con.executemany(
            "INSERT INTO events VALUES (?, ?)",
            ((i, f"payload-{i % 700_001}-{i}") for i in range(_ROWS)),
        )
        con.commit()
    finally:
        con.close()


def _config(work: Path, db: Path) -> Path:
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{"id": _SOURCE, "type": "sqlite", "path": str(db), "max_live_concurrency": 1}]
    cfg["domains"] = [{"id": "live-cap", "description": "live concurrency cap e2e"}]
    cfg["tables"] = [
        {
            "source_id": _SOURCE,
            "domain_id": "live-cap",
            "schema": "default",
            "table": "events",
            "columns": [
                {"name": n, "data_type": t, "visible_to": [_ROLE]}
                for n, t in (("id", "integer"), ("payload", "varchar"))
            ],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


@pytest.fixture(scope="module")
def servers(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("livecap")
    db = work / "events.sqlite"
    _seed(db)
    cfg = _config(work, db)
    a = IsolatedServer(_ORG, engine="duckdb", config=str(cfg), control_plane="postgres")
    b = IsolatedServer(_ORG, engine="duckdb", config=str(cfg), control_plane="postgres")
    a.start()
    try:
        b.start()
        try:
            yield a, b
        finally:
            b.stop_process()
    finally:
        a.stop_process()
        import asyncio

        asyncio.run(drop_org_schema(_ORG))


def _read(base_url: str) -> tuple[float, float, dict]:
    t0 = time.monotonic()
    resp = httpx.post(f"{base_url}/data/sql", json={"sql": _SQL, "role": _ROLE}, timeout=300)
    t1 = time.monotonic()
    assert resp.status_code == 200, resp.text
    return t0, t1, resp.json()


def _permit_sampler(stop: threading.Event, peaks: list[int], holders: set[str]) -> None:
    """Every few milliseconds: how many unexpired leases the source's permit set holds
    (``peaks``), and which leases were ever seen (``holders`` — one token per live read)."""
    r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    keys = [k for k in r.scan_iter(f"provisa:live_permits:*:{_SOURCE}")]
    while not stop.is_set():
        keys = keys or [k for k in r.scan_iter(f"provisa:live_permits:*:{_SOURCE}")]
        now_s, now_us = r.time()
        now = now_s + now_us / 1_000_000
        held = [token for k in keys for token in r.zrangebyscore(k, now, "+inf")]
        peaks.append(len(held))
        holders.update(held)
        time.sleep(0.005)


def test_the_cap_holds_across_two_instances_and_every_read_completes(servers):
    a, b = servers
    # Warm both servers (attach, first land of schema metadata) and measure one uncontended read.
    _read(a.base_url)
    _read(b.base_url)
    s0, s1, single = _read(a.base_url)
    single_s = s1 - s0
    expected = single["data"]["sql"][0]["n"]

    stop = threading.Event()
    peaks: list[int] = []
    holders: set[str] = set()
    sampler = threading.Thread(target=_permit_sampler, args=(stop, peaks, holders), daemon=True)
    sampler.start()
    targets = [a.base_url, b.base_url] * 2  # four concurrent reads, two per instance
    t_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        results = list(pool.map(_read, targets))
    wall = time.monotonic() - t_start
    stop.set()
    sampler.join(5)

    for _, _, body in results:
        n = body["data"]["sql"][0]["n"]
        assert n == expected
    assert peaks, "the sampler never read the permit set"
    assert max(peaks) == 1, f"more than one live read held a permit at once: max {max(peaks)}"
    # Every one of the four reads went through the cap: each held its own lease, one at a time.
    # (Only the live read itself holds the permit; the rest of a request — governance, planning,
    # serializing the answer — runs beside the other requests, so the wall time of four capped
    # reads is not a multiple of one uncontended read's and is not what is asserted.)
    assert len(holders) == len(targets), (
        f"{len(holders)} lease(s) seen for {len(targets)} reads (wall {wall:.2f}s, "
        f"single {single_s:.2f}s): a read was answered without taking the permit"
    )


_SOURCE_FIELDS = """
    id cacheEnabled cacheTtl changeSignal sentinelPath freshnessGate replicate
    loadProtected offPeakWindow offPeakTz maxLiveConcurrency
"""


def _admin(base_url: str, query: str, variables: dict | None = None) -> dict:
    resp = httpx.post(
        f"{base_url}/admin/graphql", json={"query": query, "variables": variables or {}}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def test_admin_round_trips_every_load_management_field_and_refuses_a_zero_cap(servers, tmp_path):
    a, _ = servers
    db = tmp_path / "admin.sqlite"
    sqlite3.connect(db).close()
    source = {
        "id": "cap-admin",
        "type": "sqlite",
        "path": str(db),
        "cacheEnabled": False,
        "cacheTtl": 120,
        "changeSignal": "ttl_probe",
        "sentinelPath": "https://example.com/marker",
        "freshnessGate": True,
        "replicate": 0,
        "loadProtected": True,
        "offPeakWindow": "01:00-05:00",
        "offPeakTz": "America/New_York",
        "maxLiveConcurrency": 3,
    }
    create = _admin(
        a.base_url,
        "mutation($i: SourceInput!) { createSource(input: $i) { success message code } }",
        {"i": source},
    )["createSource"]
    assert create["success"], create
    got = next(
        s
        for s in _admin(a.base_url, f"{{ sources {{ {_SOURCE_FIELDS} }} }}")["sources"]
        if s["id"] == "cap-admin"
    )
    for field, value in source.items():
        if field in ("type", "path"):
            continue
        assert got[field] == value, (field, got[field], value)

    refused = _admin(
        a.base_url,
        "mutation($i: SourceInput!) { updateSource(input: $i) { success message code } }",
        {"i": {**source, "maxLiveConcurrency": 0}},
    )["updateSource"]
    assert refused["success"] is False
    assert refused["code"] == "schema.max_live_concurrency_invalid"
