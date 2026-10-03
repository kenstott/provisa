# Copyright (c) 2026 Kenneth Stott
# Canary: 71e6c0a9-b354-4d8f-92c7-0a5d3f8e1b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model or governance change reaches every worker of every instance (REQ-1914).

Boots ``uvicorn --workers 4`` and a SECOND instance on the same control plane, makes a change
through the admin API on ONE worker, and asks EACH worker — on a connection pinned to it — what
it serves:

* a column hidden from a role is refused by every worker of both instances within the reload
  interval;
* ``/health`` shows each worker's loaded config stamp at the control plane's;
* a source registered through one worker, and a table on it, are queryable on every worker;
* the stored stamp does not move while nothing changes — a reload writes nothing that advances
  it, so workers do not reload each other in a loop;
* the evidence script (``tests/integration/cross_worker_evidence.py``: register table, column
  visibility, row filter, mask, relationship) shows every change on every worker, for one launch
  and for two.

Each runs twice: with the boot org named "default", and with a named one (``ORG_ID``). The boot
runtime is registered under the compile-time id and moved to ``ORG_ID`` when the control plane is
read; a reload of a named boot org once checked for "default", found nothing and reloaded
nothing, so a change through one worker never reached the others (REQ-1914). "default" alone
cannot see that.

The test instance only: its own database, org, data directory and leased ports
(``tests/integration/worker_boot_harness.py``)."""

# Requirements: REQ-1914

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import sqlalchemy as sa

from tests.integration.cross_worker_evidence import Worker
from tests.integration.test_settings_catalog_workers import _one_connection_per_worker
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_WORKERS = 4
# The operator's reload interval for these launches (config.reload_interval).
_INTERVAL_S = 1.0
# How long a worker may take to serve another worker's change: one interval to notice the stamp,
# then its own schema rebuild. A worker still serving the old model after this is the defect.
_REACHES_EVERY_WORKER_S = _INTERVAL_S + 20.0
_ENV = {"PROVISA_CONFIG_RELOAD_INTERVAL": str(_INTERVAL_S)}
_REPO_ROOT = Path(__file__).parents[2]


@pytest.fixture(
    scope="module", params=["default", "acme"], ids=["boot-org-default", "boot-org-named"]
)
def launches(request):
    """Launch A (four workers) and launch B (one worker) on the same control plane, both serving
    the boot org ``request.param``."""
    env = {**_ENV, "ORG_ID": request.param}
    first = WorkerBoot(_WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT, env=env)
    first.create_database()
    own = sa.create_engine(first.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.customers (id integer PRIMARY KEY, name text)"))
        conn.execute(sa.text("INSERT INTO public.customers VALUES (1, 'ann'), (2, 'bob')"))
    own.dispose()
    second = None
    try:
        first.start()
        first.wait_all_ready(timeout=300)
        second = WorkerBoot(
            1,
            pg_host=_PG_HOST,
            pg_port=_PG_PORT,
            database=first.database,
            data_dir=first.data_dir,
            env=env,
        )
        second.start()
        second.wait_all_ready(timeout=300)
        yield first, second
    finally:
        if second is not None:
            second.stop()
        first.cleanup()


@pytest.fixture
def workers(launches):
    """One pinned connection per worker: launch A's four, then launch B's one."""
    first, second = launches
    pinned = _one_connection_per_worker(first.ports["http"], _WORKERS)
    assert len(pinned) == _WORKERS, f"reached {len(pinned)} of {_WORKERS} workers"
    pinned.append(Worker(second.ports["http"]))
    yield pinned
    for w in pinned:
        w.conn.close()


def _health(worker: Worker) -> dict:
    worker.conn.request("GET", "/health")
    resp = worker.conn.getresponse()
    text = resp.read().decode()
    assert resp.status == 200, text
    return json.loads(text)


def _still_pinned(workers: list[Worker]) -> None:
    for w in workers:
        assert w.schema_version()[0] == w.boot_id, "connection moved to another worker"


def _until_every_worker(workers: list[Worker], probe, what: str) -> float:
    """Wait until ``probe(worker)`` holds on every worker; how long that took."""
    started = time.monotonic()
    seen: list = []
    while time.monotonic() - started < _REACHES_EVERY_WORKER_S:
        _still_pinned(workers)
        seen = [probe(w) for w in workers]
        if all(ok for ok, _ in seen):
            return time.monotonic() - started
        time.sleep(0.25)
    shown = [f"{w.boot_id[:6]}: {json.dumps(raw)[:160]}" for w, (_, raw) in zip(workers, seen)]
    raise AssertionError(f"{what}: not on every worker after {_REACHES_EVERY_WORKER_S}s — {shown}")


def _refused(body: dict) -> bool:
    return "errors" in body or "detail" in body


def _stamps_agree(worker: Worker) -> tuple[bool, dict]:
    config = _health(worker)["config"]
    return all(kind["loaded"] == kind["stored"] for kind in config.values()), config


def test_every_worker_boots_at_the_control_planes_stamp(workers):
    _until_every_worker(workers, _stamps_agree, "loaded stamp == stored stamp")
    stored = {_health(w)["config"]["model"]["stored"] for w in workers}
    assert len(stored) == 1, f"workers disagree about the stored stamp: {stored}"


def test_a_column_hidden_through_one_worker_is_refused_by_every_worker_of_both_instances(workers):
    author = workers[0]
    query = "{ s__orders { id region } }"
    assert not any(_refused(w.data(query)) for w in workers)
    before = _health(author)["config"]["model"]["stored"]

    res = author.admin(
        'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
        'schemaName: "public", tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "region", visibleTo: ["org_admin"], dataType: "varchar"}]}) '
        "{ success message } }"
    )
    assert res["data"]["updateTable"]["success"], res

    took = _until_every_worker(
        workers,
        lambda w: (lambda body: (_refused(body), body))(w.data(query)),
        "analyst refused `region`",
    )
    print(f"\ncolumn hidden on all {len(workers)} workers after {took:.1f}s")
    # The role that still holds the column still reads it, on every worker.
    for w in workers:
        body = w.data(query, "org_admin")
        assert not _refused(body) and len(body["data"]["s__orders"]) == 2, body

    # Visible, not only enforced: each worker reports the stamp it loaded, now the stored one.
    _until_every_worker(workers, _stamps_agree, "loaded stamp == stored stamp")
    assert _health(author)["config"]["model"]["stored"] > before


def test_a_source_registered_through_one_worker_is_queryable_on_every_worker(workers, launches):
    first, _ = launches
    author = workers[1]
    res = author.admin(
        'mutation { createSource(input: {id: "crm-pg", type: "postgresql", '
        f'host: "{_PG_HOST}", port: {_PG_PORT}, database: "{first.database}", '
        # A literal password: it goes into the org vault through this one worker, and every
        # other worker decrypts it with the launch's one master key.
        'username: "provisa", password: "provisa"}) { success message } }'
    )
    assert res["data"]["createSource"]["success"], res
    res = author.admin(
        'mutation { registerTable(input: {sourceId: "crm-pg", domainId: "sales", '
        'schemaName: "public", tableName: "customers", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "name", visibleTo: ["org_admin", "analyst"], dataType: "varchar"}]}) '
        "{ success message } }"
    )
    assert res["data"]["registerTable"]["success"], res

    def _reads_customers(w: Worker) -> tuple[bool, dict]:
        body = w.data("{ s__customers { id name } }")
        rows = (body.get("data") or {}).get("s__customers") or []
        return sorted(r["name"] for r in rows) == ["ann", "bob"], body

    _until_every_worker(workers, _reads_customers, "table on the new source queryable")


def test_the_stored_stamp_does_not_move_while_nothing_changes(workers):
    """Five workers have each reloaded several times by now. A reload that advanced the stamp
    would make every other worker reload, and they would never settle."""
    _until_every_worker(workers, _stamps_agree, "loaded stamp == stored stamp")
    before = _health(workers[0])["config"]
    versions = [w.schema_version()[1] for w in workers]
    deadline = time.monotonic() + 10 * _INTERVAL_S
    while time.monotonic() < deadline:
        _still_pinned(workers)  # also keeps each keep-alive connection in use
        time.sleep(1)
    assert _health(workers[0])["config"] == before
    assert [w.schema_version()[1] for w in workers] == versions, "a worker rebuilt with no change"


def _evidence(*flags: str) -> list[tuple[str, str, str, str]]:
    """Run the evidence script; its result table as (change, at 0 s, at 20 s, at 70 s)."""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.integration.cross_worker_evidence",
            "--pg-host",
            _PG_HOST,
            "--pg-port",
            str(_PG_PORT),
            *flags,
        ],
        cwd=str(_REPO_ROOT),
        env={**os.environ, **_ENV},
        capture_output=True,
        text=True,
        timeout=1800,
    )
    print(proc.stdout)
    assert proc.returncode == 0, proc.stderr[-4000:]
    table = proc.stdout.split("\nchange", 1)[1].splitlines()[1:]
    rows = [
        (line[:61].strip(), *(line[61 + 14 * i : 75 + 14 * i].strip() for i in range(3)))
        for line in table
        if line.strip()
    ]
    assert len(rows) == 5, rows
    return rows  # type: ignore[return-value]


@pytest.mark.parametrize("flags", [(), ("--second-launch",)], ids=["one-launch", "two-launches"])
def test_the_evidence_script_shows_every_change_on_every_worker(flags):
    for change, _at_0s, at_20s, at_70s in _evidence(*flags):
        assert set(at_20s.split()) == {"Y"}, f"{change}: at 20 s {at_20s!r}"
        assert set(at_70s.split()) == {"Y"}, f"{change}: at 70 s {at_70s!r}"
