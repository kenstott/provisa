# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's catalog is built once per model generation and answers every catalog statement of it
(REQ-2167): DBeaver sends dozens on connect, and building the catalog for each cost each one."""

from __future__ import annotations

import contextvars
import threading
from unittest.mock import MagicMock

import pytest

from provisa.pgwire import catalog_populate
from provisa.pgwire.catalog import answer


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"analyst": ctx, "auditor": MagicMock(tables={})}
    state.schema_build_cache = {"column_types": {}}
    state.roles = {}
    return state


@pytest.fixture
def builds(monkeypatch):
    built: list[str] = []
    build = catalog_populate._build_catalog_db

    def counting(role_id, state):
        built.append(role_id)
        return build(role_id, state)

    monkeypatch.setattr(catalog_populate, "_build_catalog_db", counting)
    return built


def test_a_roles_catalog_is_built_once_for_its_statements(builds):
    state = _state()
    for sql in ("SELECT nspname FROM pg_namespace", "SELECT relname FROM pg_class"):
        answer(sql, "analyst", state)
    assert builds == ["analyst"]


def test_each_role_has_its_own_catalog(builds):
    state = _state()
    answer("SELECT rolname FROM pg_roles", "analyst", state)
    answer("SELECT rolname FROM pg_roles", "auditor", state)
    assert builds == ["analyst", "auditor"]


def test_a_new_model_generation_builds_the_catalog_again(builds):
    state = _state()
    answer("SELECT nspname FROM pg_namespace", "analyst", state)
    state.contexts = {**state.contexts, "analyst": MagicMock(tables={})}  # a rebuilt role context
    answer("SELECT nspname FROM pg_namespace", "analyst", state)
    state.roles = {"analyst": {"id": "analyst"}}  # a new role registry
    answer("SELECT nspname FROM pg_namespace", "analyst", state)
    state.roles["meta:analyst+auditor"] = {"id": "meta:analyst+auditor"}  # added in place
    answer("SELECT nspname FROM pg_namespace", "analyst", state)
    assert builds == ["analyst", "analyst", "analyst", "analyst"]


def test_concurrent_statements_share_the_catalog(builds):
    state = _state()
    answer("SELECT 1 FROM pg_namespace LIMIT 1", "analyst", state)
    results: list[list] = []

    def ask():
        results.append(answer("SELECT nspname FROM pg_namespace ORDER BY 1", "analyst", state).rows)

    # Each thread asks in the request's context (its bound org), as a connection thread does.
    threads = [
        threading.Thread(target=contextvars.copy_context().run, args=(ask,)) for _ in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert builds == ["analyst"]
    assert len(results) == 8 and all(r == results[0] and r for r in results)
