# Copyright (c) 2026 Kenneth Stott
# Canary: 6b3e8d14-0f72-4c95-a8e1-2d7c5b90f436
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a principal's ephemeral graph session is one, whichever connection and worker a
statement reaches (REQ-2176).

A two-worker launch serves each request on a connection of its own, so its statements are spread
over both workers: every one of them sees the node the first made, the Cypher page reads the
session's label with its count, and clearing it ends it for every connection.
"""

# Requirements: REQ-2176

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLE = "graph_user"


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        2,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        per_worker_http=True,
        extra_config={
            "tables": [
                {
                    "source_id": "sales-pg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "people",
                    "columns": [
                        {
                            "name": c,
                            "data_type": t,
                            "visible_to": [_ROLE],
                            "is_primary_key": c == "id",
                        }
                        for c, t in (("id", "integer"), ("name", "varchar"))
                    ],
                }
            ],
            "roles": [
                {
                    "description": "Test role.",
                    "id": _ROLE,
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE public.people (id integer PRIMARY KEY, name text)"))
            conn.execute(sa.text("INSERT INTO public.people VALUES (1, 'ann')"))
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """One request, on a connection of its own."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "x-provisa-role": _ROLE,
            "Connection": "close",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _cypher(boot, statement: str) -> tuple[int, dict]:
    return _call(boot, "POST", "/data/cypher", {"query": statement})


def test_every_connection_and_worker_sees_one_graph_and_a_clear_ends_it_for_all(server):
    status, made = _cypher(server, "CREATE (:Scratch {n: 1})")
    assert status == 200, made
    assert made["session_changed"] is True
    for _ in range(8):
        status, read = _cypher(server, "MATCH (s:Scratch) RETURN s.n AS n")
        assert status == 200, read
        assert [r["n"] if isinstance(r, dict) else r[0] for r in read["rows"]] == [1]
        assert "session_changed" not in read
    status, held = _call(server, "GET", "/data/cypher/session")
    assert status == 200, held
    assert held == {
        "labels": [{"label": "Scratch", "nodes": 1, "tagged_model_nodes": 0}],
        "types": [],
        "empty": False,
    }
    assert _call(server, "DELETE", "/data/cypher/session") == (200, {"cleared": True})
    for _ in range(4):
        status, held = _call(server, "GET", "/data/cypher/session")
        assert (status, held) == (200, {"labels": [], "types": [], "empty": True})
