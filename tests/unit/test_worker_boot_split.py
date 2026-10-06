# Copyright (c) 2026 Kenneth Stott
# Canary: 7b0d2e95-1a4c-4d36-8e7f-3c9a5b1d6f02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The per-worker half of the boot writes nothing shared (REQ-1900).

A worker whose launch has already done the once-per-launch work connects, reads and builds; it
does not re-issue engine catalogs (on Trino that is DROP CATALOG + CREATE CATALOG under the
workers already serving) or re-apply the config."""

# Requirements: REQ-1900

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from provisa.core import boot_lock
from provisa.core.boot_lock import boot_generation, expected_workers, launch_id


def test_a_process_outside_a_launch_has_no_launch_id_and_is_the_only_worker(monkeypatch):
    monkeypatch.delenv("PROVISA_LAUNCH_ID", raising=False)
    monkeypatch.setenv("PROVISA_WORKERS", "16")
    assert launch_id() is None
    assert expected_workers() == 1


def test_a_launch_names_its_id_and_its_worker_count(monkeypatch):
    monkeypatch.setenv("PROVISA_LAUNCH_ID", "abc")
    monkeypatch.setenv("PROVISA_WORKERS", "16")
    assert launch_id() == "abc"
    assert expected_workers() == 16


def test_a_launch_without_a_worker_count_is_refused(monkeypatch):
    monkeypatch.setenv("PROVISA_LAUNCH_ID", "abc")
    monkeypatch.delenv("PROVISA_WORKERS", raising=False)
    with pytest.raises(KeyError):
        expected_workers()


def test_the_generation_changes_with_the_launch_and_with_every_input():
    base = boot_generation("l1", config={"a": 1}, schema="s", engine="duckdb")
    assert base == boot_generation("l1", engine="duckdb", schema="s", config={"a": 1})
    assert base != boot_generation("l2", config={"a": 1}, schema="s", engine="duckdb")
    assert base != boot_generation("l1", config={"a": 2}, schema="s", engine="duckdb")
    assert base != boot_generation("l1", config={"a": 1}, schema="s2", engine="duckdb")
    assert base != boot_generation("l1", config={"a": 1}, schema="s", engine="trino")


def test_a_dead_workers_roll_call_row_is_recognised():
    import os
    import subprocess
    import sys

    assert boot_lock._pid_alive(os.getpid())
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait(timeout=30)
    assert not boot_lock._pid_alive(child.pid)


def test_connecting_a_trino_terminal_registers_and_seeds_nothing():
    from provisa.federation import trino_lifecycle

    state = SimpleNamespace(federation_engine=object(), tenant_engine=object(), org_id="default")
    with (
        patch.object(trino_lifecycle, "terminal_conn_kwargs", return_value={"host": "h"}),
        patch.object(trino_lifecycle.trino.dbapi, "connect", return_value="conn") as connect,
        patch("provisa.core.trino_system_catalogs.register_system_catalogs") as register,
        patch("provisa.observability.ops_trino.seed_ops_trino") as seed,
        patch("provisa.compiler.schema_service.init") as schema_init,
    ):
        trino_lifecycle.connect_terminal(state)

    connect.assert_called_once_with(host="h")
    assert state.engine_conn == "conn"
    schema_init.assert_called_once_with(state.federation_engine)
    register.assert_not_called()
    seed.assert_not_called()


def test_a_worker_that_does_not_apply_seeds_nothing_and_reads_the_store():
    """REQ-1900, REQ-1919: a worker whose launch another worker set up (``apply=False``) writes
    nothing to the control plane: it neither seeds nor issues catalogs, and it reads the model the
    process runs from the store, as every worker does."""
    import inspect

    from provisa.api import app as app_module

    src = inspect.getsource(app_module._load_and_build)
    seed_at = src.index("seed_config(")
    guard = src.rindex("if apply and not await is_seeded(conn):", 0, seed_at)
    assert src.index("_seed_file", guard) < seed_at
    assert "if apply and not engine_deferred:" in src
    assert src.index("attach_store_sources(") > src.index("if apply and not engine_deferred:")
    # Every worker reads its configuration from the store.
    assert "config = await store_config(raw_config, conn)" in src
    assert "adopt_loaded_config" not in src
