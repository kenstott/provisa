# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9a1f64-7d28-4b05-a6e3-5f1b8c2d9e70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A retired source does not stop a worker picking up the sources registered after it
(REQ-1914, REQ-2248).

Boots ``uvicorn --workers 2`` on a fresh control plane. Through ONE worker: two sources are
registered, one of them is deleted while it holds no table (which retires it: its row is kept
under a key of its own), and a third is registered with a table. EVERY worker then reads that
table, and no worker logged that its source state could not be brought in line.

A worker that did not make the change reaches the new source through its schema build's source
step (``provisa/api/model_reload.py`` ``reconcile_sources``). Given the retired row, that step
failed whole on every build, so the worker never built a pool for the third source.

The test instance only: its own database, org, data directory and leased ports
(``tests/integration/worker_boot_harness.py``)."""

# Requirements: REQ-1914, REQ-2248

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

from tests.integration.cross_worker_evidence import Worker
from tests.integration.test_config_sync_workers import _stamps_agree, _until_every_worker
from tests.integration.test_settings_catalog_workers import _one_connection_per_worker
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_WORKERS = 2
_ENV = {"PROVISA_CONFIG_RELOAD_INTERVAL": "1.0"}
_NOT_IN_LINE = "source state could not be brought in line"


@pytest.fixture(scope="module")
def launch():
    boot = WorkerBoot(_WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT, env=_ENV)
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.customers (id integer PRIMARY KEY, name text)"))
        conn.execute(sa.text("INSERT INTO public.customers VALUES (1, 'ann'), (2, 'bob')"))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture
def workers(launch):
    pinned = _one_connection_per_worker(launch.ports["http"], _WORKERS)
    assert len(pinned) == _WORKERS, f"reached {len(pinned)} of {_WORKERS} workers"
    yield pinned
    for w in pinned:
        w.conn.close()


def _create_source(author: Worker, source_id: str, database: str) -> None:
    res = author.admin(
        f'mutation {{ createSource(input: {{id: "{source_id}", type: "postgresql", '
        f'host: "{_PG_HOST}", port: {_PG_PORT}, database: "{database}", '
        'username: "provisa", password: "provisa"}) { success message } }'
    )
    assert res["data"]["createSource"]["success"], res


def _source_ids(worker: Worker) -> set[str]:
    return {s["id"] for s in worker.admin("{ sources { id } }")["data"]["sources"]}


def test_a_source_registered_after_another_was_retired_is_queryable_on_every_worker(
    workers, launch
):
    author = workers[0]
    _create_source(author, "kept-pg", launch.database)
    _create_source(author, "old-pg", launch.database)
    # Every worker has built with both sources in service before one of them is retired.
    _until_every_worker(workers, _stamps_agree, "loaded stamp == stored stamp, two sources")

    res = author.admin('mutation { deleteSource(id: "old-pg") { success message } }')
    assert res["data"]["deleteSource"]["success"], res
    assert "old-pg" not in _source_ids(author)

    _create_source(author, "new-pg", launch.database)
    res = author.admin(
        'mutation { registerTable(input: {sourceId: "new-pg", domainId: "sales", '
        'schemaName: "public", tableName: "customers", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "name", visibleTo: ["org_admin", "analyst"], dataType: "varchar"}]}) '
        "{ success message } }"
    )
    assert res["data"]["registerTable"]["success"], res
    # REQ-1921: registered, it is live: no step puts it in service.

    def _reads_customers(w: Worker) -> tuple[bool, dict]:
        body = w.data("{ s__customers { id name } }")
        rows = (body.get("data") or {}).get("s__customers") or []
        return sorted(r["name"] for r in rows) == ["ann", "bob"], body

    _until_every_worker(workers, _reads_customers, "table on the source registered afterwards")
    _until_every_worker(workers, _stamps_agree, "loaded stamp == stored stamp, after the retire")
    for w in workers:
        assert _source_ids(w) >= {"kept-pg", "new-pg"}
        assert "old-pg" not in _source_ids(w)
    assert _NOT_IN_LINE not in launch.log_text()
