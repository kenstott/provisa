# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7f4d19-8c3e-4b65-9f02-d1e6a5c7b384
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An in-memory materialization store creates no files (REQ-1901).

The store broker serializes a FILE store across processes with a sentinel lock file and a
generation file beside it. Given the in-memory store path it did the same thing literally — and
wrote ``:memory:.lock`` and ``:memory:.gen`` into whatever directory the process was started in,
while attaching a brand-new empty database on every call, so nothing written was ever read back."""

# Requirements: REQ-1901

from __future__ import annotations

import os

import pytest

from provisa.federation import materialize_broker
from provisa.federation.materialize_broker import get_broker


@pytest.fixture
def in_empty_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(materialize_broker, "_memory_store", None)
    return tmp_path


def test_an_in_memory_store_writes_no_files(in_empty_cwd):
    broker = get_broker(":memory:")
    broker.reconcile("mat", "t", [("id", "INTEGER"), ("name", "VARCHAR")])
    broker.execute("INSERT INTO mat_store.mat.t VALUES (1, 'a')")
    broker.canary()
    assert os.listdir(in_empty_cwd) == []


def test_an_in_memory_store_keeps_what_was_written(in_empty_cwd):
    broker = get_broker(":memory:")
    broker.reconcile("mat", "t", [("id", "INTEGER"), ("name", "VARCHAR")])
    broker.execute("INSERT INTO mat_store.mat.t VALUES (1, 'a')")
    # A second handle is the same process-wide store, as two handles on one file are one file.
    assert get_broker(":memory:").execute("SELECT id, name FROM mat_store.mat.t") == [(1, "a")]


def test_an_in_memory_store_generation_advances_on_writes_only(in_empty_cwd):
    broker = get_broker(":memory:")
    start = broker.canary()
    broker.reconcile("mat", "t", [("id", "INTEGER")])
    after_write = broker.canary()
    assert after_write > start
    table, generation = broker.fetch_arrow("mat", "t")
    assert table.num_rows == 0
    assert generation == after_write == broker.canary()


def test_a_file_store_still_locks_and_counts_beside_the_file(tmp_path):
    path = str(tmp_path / "store.duckdb")
    broker = get_broker(path)
    broker.reconcile("mat", "t", [("id", "INTEGER")])
    assert os.path.exists(path + ".lock")
    assert broker.canary() == 1
