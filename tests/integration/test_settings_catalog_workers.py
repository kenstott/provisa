# Copyright (c) 2026 Kenneth Stott
# Canary: 91c4e0a7-3d58-4b2f-8e16-a7f05d2c6b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An operator setting saved through one worker is the deployment's (REQ-1913, REQ-1900).

Boots ``uvicorn --workers 4`` on a fresh control plane and asks EACH worker — on a connection
pinned to it — what it runs on after a setting is saved through one of them:

* a live setting is in force on every worker within the config reload interval (REQ-1914: a
  saved setting advances the platform plane's ``settings`` stamp, and each worker's config
  watcher reloads its settings when the stored stamp differs from the one it loaded);
* a restart setting keeps its booted value on every worker, reports a pending restart, and is in
  force after the launch is restarted;
* the stored value takes precedence over the environment, and clearing it returns to the
  environment;
* a bad value is refused naming the field, and nothing is stored;
* a secret is never returned by any worker.

The test instance only: its own database, org, data directory and leased ports
(``tests/integration/worker_boot_harness.py``)."""

# Requirements: REQ-1913, REQ-1900, REQ-1914

from __future__ import annotations

import json
import os
import time

import pytest

from provisa.core import config_watch, settings_registry
from tests.integration.cross_worker_evidence import Worker
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_WORKERS = 4
CATALOG = "/admin/settings/catalog"
# How long a worker may take to see another worker's write: one config reload interval (the
# launch runs on the setting's default) plus slack for the request itself. Not a tuning knob — a
# worker still on the old value after this is the defect.
_REACHES_EVERY_WORKER_S = settings_registry.setting(config_watch.INTERVAL_SETTING).default + 5.0
# The harness launches with these in the environment (see WorkerBoot.start / `env=` below).
_ENV_MCP_MAX_ROWS = 50
_ENV_REDIRECT_SECRET = "minioadmin"


def _get(worker: Worker) -> tuple[dict[str, dict], dict, str]:
    worker.conn.request("GET", CATALOG)
    resp = worker.conn.getresponse()
    text = resp.read().decode()
    assert resp.status == 200, text
    payload = json.loads(text)
    by_key = {s["key"]: s for card in payload["cards"] for s in card["settings"]}
    return by_key, payload, text


def _put(worker: Worker, values: dict, confirm: list[str] | None = None) -> tuple[int, dict, str]:
    body = json.dumps({"values": values, "confirm": confirm or []})
    worker.conn.request("PUT", CATALOG, body=body, headers={"Content-Type": "application/json"})
    resp = worker.conn.getresponse()
    text = resp.read().decode()
    return resp.status, json.loads(text), text


def _every_worker(workers: list[Worker], key: str, expect: dict) -> None:
    """Wait until every worker's catalog entry for ``key`` carries ``expect``."""
    deadline = time.monotonic() + _REACHES_EVERY_WORKER_S
    seen: list[dict] = []
    while time.monotonic() < deadline:
        seen = [_get(w)[0][key] for w in workers]
        if all(all(entry.get(k) == v for k, v in expect.items()) for entry in seen):
            return
        time.sleep(0.25)
    shown = [{k: entry.get(k) for k in expect} for entry in seen]
    raise AssertionError(f"{key}: not every worker reports {expect} — {shown}")


def _one_connection_per_worker(port: int, workers: int) -> list[Worker]:
    """One keep-alive connection pinned to each worker process.

    Connections are opened in simultaneous bursts and all held until every worker is found: the
    workers share one listening socket, and the kernel hands a lone connection to whichever
    worker it woke last — opened one at a time, hundreds can land on the same few workers.
    """
    from concurrent.futures import ThreadPoolExecutor

    seen: dict[str, Worker] = {}
    spare: list[Worker] = []
    try:
        for _ in range(25):
            with ThreadPoolExecutor(max_workers=32) as pool:
                burst = list(pool.map(lambda _i: Worker(port), range(32)))
            for w in burst:
                if w.boot_id in seen:
                    spare.append(w)
                else:
                    seen[w.boot_id] = w
            if len(seen) == workers:
                break
    finally:
        for w in spare:
            w.conn.close()
    return list(seen.values())


@pytest.fixture(scope="module")
def launch():
    boot = WorkerBoot(
        _WORKERS,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        env={"PROVISA_MCP_MAX_ROWS": str(_ENV_MCP_MAX_ROWS)},
    )
    boot.create_database()
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


def _rows(worker: Worker) -> int:
    body = worker.data("{ s__orders { id } }")
    assert "data" in body and "errors" not in body, body
    return len(body["data"]["s__orders"])


def test_a_live_setting_is_in_force_on_every_worker(workers):
    author = workers[0]
    assert all(_rows(w) == 2 for w in workers)
    status, saved, _ = _put(author, {"limits.default_row_limit": 1})
    assert (status, saved["updated"]) == (200, ["limits.default_row_limit"])
    assert saved["pending_restart"] == []
    _every_worker(
        workers,
        "limits.default_row_limit",
        {"value": 1, "source": "stored", "pending_restart": False, "restart_required": False},
    )
    # In force, not only reported: every worker now caps the query at the stored limit.
    assert [_rows(w) for w in workers] == [1] * _WORKERS
    status, _, _ = _put(author, {"limits.default_row_limit": None})
    assert status == 200
    _every_worker(workers, "limits.default_row_limit", {"value": 100, "source": "default"})
    assert [_rows(w) for w in workers] == [2] * _WORKERS


def test_the_stored_value_takes_precedence_over_the_environment(workers):
    author = workers[1]
    _every_worker(workers, "mcp.max_rows", {"value": _ENV_MCP_MAX_ROWS, "source": "env"})
    assert _put(author, {"mcp.max_rows": 7})[0] == 200
    _every_worker(workers, "mcp.max_rows", {"value": 7, "source": "stored", "stored": 7})
    assert _put(author, {"mcp.max_rows": None})[0] == 200
    _every_worker(
        workers, "mcp.max_rows", {"value": _ENV_MCP_MAX_ROWS, "source": "env", "stored": None}
    )


def test_a_bad_value_is_refused_naming_the_field_and_nothing_is_stored(workers):
    status, body, _ = _put(
        workers[2], {"limits.engine_query_timeout": 300, "limits.default_row_limit": 0}
    )
    assert status == 400
    assert body["code"] == "settings.invalid_value"
    assert body["params"]["field"] == "limits.default_row_limit"
    assert body["params"]["reason"] == "below_min"
    assert body["params"]["min"] == 1
    time.sleep(_REACHES_EVERY_WORKER_S)
    for w in workers:
        by_key = _get(w)[0]
        assert by_key["limits.engine_query_timeout"]["source"] == "default"
        assert by_key["limits.default_row_limit"]["source"] == "default"


def test_a_secret_is_never_returned_by_any_worker(workers):
    # The launch's environment carries the object-store secret; the deployment has no auth
    # provider, so the caller is anonymous and a guarded setting is refused for it.
    for w in workers:
        by_key, _, text = _get(w)
        entry = by_key["redirect.secret_key"]
        assert entry["type"] == "secret" and entry["set"] is True and entry["source"] == "env"
        assert "value" not in entry and "stored" not in entry
        assert _ENV_REDIRECT_SECRET not in text
    status, body, text = _put(
        workers[3], {"redirect.secret_key": "s3cr3t-value"}, confirm=["redirect.secret_key"]
    )
    assert status == 403
    assert body["code"] == "settings.auth_required"
    assert body["params"] == {"field": "redirect.secret_key", "reason": "auth_required"}
    assert "s3cr3t-value" not in text


def test_a_live_setting_a_worker_holds_as_state_is_applied_by_every_worker(workers):
    """The request-thread bound is the size of each worker's own pool: a stored change has to be
    APPLIED by every worker, not only read. Each worker reports the value its pool was last built
    from (``applied``)."""
    key = "concurrency.request_threads"
    _every_worker(workers, key, {"value": 4, "applied": 4, "restart_required": False})
    status, saved, _ = _put(workers[2], {key: 7})
    assert (status, saved["pending_restart"]) == (200, [])
    _every_worker(workers, key, {"value": 7, "source": "stored", "applied": 7})
    assert _put(workers[2], {key: None})[0] == 200
    _every_worker(workers, key, {"value": 4, "source": "default", "applied": 4})
    # The workers still serve on the pools they resized.
    assert [_rows(w) for w in workers] == [2] * _WORKERS


def test_a_restart_setting_is_pending_on_every_worker_and_in_force_after_the_restart(
    launch, workers
):
    key = "concurrency.grpc_max_concurrent_rpcs"
    _every_worker(workers, key, {"value": 200, "source": "default", "restart_required": True})
    status, saved, _ = _put(workers[0], {key: 37})
    assert (status, saved["pending_restart"]) == (200, [key])
    _every_worker(
        workers, key, {"value": 200, "stored": 37, "source": "stored", "pending_restart": True}
    )
    for w in workers:
        assert _get(w)[1]["pending_restart"] == [key]
        w.conn.close()

    # Restart the launch on the SAME control plane: a new launch id, new worker processes.
    launch.stop()
    relaunched = WorkerBoot(
        _WORKERS,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        database=launch.database,
        data_dir=launch.data_dir,
    )
    try:
        relaunched.start()
        relaunched.wait_all_ready(timeout=300)
        after = _one_connection_per_worker(relaunched.ports["http"], _WORKERS)
        assert len(after) == _WORKERS
        for w in after:
            by_key, payload, _ = _get(w)
            entry = by_key[key]
            assert (entry["value"], entry["stored"], entry["source"]) == (37, 37, "stored")
            assert entry["pending_restart"] is False
            assert payload["pending_restart"] == []
            w.conn.close()
        errors = [
            ln for ln in relaunched.log_text().splitlines() if "ERROR" in ln or "Traceback" in ln
        ]
        assert errors == []
    finally:
        relaunched.stop()
