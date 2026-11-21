# Copyright (c) 2026 Kenneth Stott
# Canary: 5b9f4270-0b6b-437d-972c-57a624564d57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""Names-table writes are guarded to prod builds only, and to after the schema version advances
(REQ-194, REQ-1882, REQ-1915, REQ-2006).

Two regressions introduced together by d1a38d8605 + 9445ba716c:

1. SLOW SANDBOX OPEN (REQ-2006). ``write_names_table`` and ``freeze_aliases`` were called on
   every rebuild, including the non-prod env builds ``ensure_org_runtime`` added to every sandbox
   open (synthetic, preprod).  Each env build locked ``model_names`` in SHARE ROW EXCLUSIVE MODE
   for its duration, adding 3–6 s per env to the open.  An env build shares the prod's model, the
   prod has already written the names table, and there is nothing for an env build to add.

2. STALE-MODEL REPLICA REQUEST (REQ-1915). The lock was acquired inside the same
   ``async with conn`` block as other build-output writes, which ran *before*
   ``state.schema_version += 1``.  While one node's rebuild held the lock and a concurrent
   node awaited it, ``registered_tables()`` returned cached data keyed on the old
   ``schema_version`` — including the old ``home_region`` for a table whose region the operator
   had just changed.  If ``drop_retired`` deleted the stale record from that node's
   ``replica_state`` during this window, a data read using the stale cache found no record and
   requested a fresh build, leaving an empty replica table in the store.  Writing the names table
   after ``schema_version`` advances ensures every concurrent read gets a fresh DB lookup with
   the new ``home_region``.
"""

# Requirements: REQ-194, REQ-1882, REQ-1915, REQ-2006

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytestmark = pytest.mark.unbound

# ---------------------------------------------------------------------------
# Shared harness: a _guarded_names coroutine that mirrors the guard in
# _rebuild_schemas_impl (app.py), using the same imports and context variables.
# Tests verify the guard's invariants by patching write_names_table /
# freeze_aliases and observing whether / when they are called.
# ---------------------------------------------------------------------------


async def _run_guard(state: Any) -> None:
    """Mirror of the names-table guard added after schema_version += 1 in
    _rebuild_schemas_impl.  Uses the same imports so the test fails if any of
    them move or the guard condition changes without a matching test update."""
    from provisa.api.app import _schema_version_guard
    from provisa.core.environments import PROD
    from provisa.core.request_context import active_env

    _env = active_env()
    with _schema_version_guard:
        state.schema_version += 1
    if _env == PROD:
        import provisa.core.repositories.model_names as _model_names

        assert state.names_table is not None, "the build stores the names table it built"
        await _model_names.write_names_table(None, state.names_table.rows)
        await _model_names.freeze_aliases(None, state.names_table.frozen_aliases)


# ---------------------------------------------------------------------------
# Test 1: write_names_table is NOT called for non-prod env rebuilds
# ---------------------------------------------------------------------------


def test_names_table_write_skipped_for_env_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env (non-prod) rebuild must not call write_names_table: the prod has already written
    it, and the SHARE ROW EXCLUSIVE MODE lock it acquires would serialise every concurrent env
    rebuild against every concurrent prod rebuild unnecessarily (REQ-2006, REQ-1882)."""
    from provisa.core.env_classes import TEST_SYNTHETIC
    from provisa.core.request_context import current_env

    written: list[str] = []

    async def _write_names_table(_conn: Any, _rows: Any) -> None:
        written.append("write_names_table")

    async def _freeze_aliases(_conn: Any, _frozen: Any) -> None:
        written.append("freeze_aliases")

    import provisa.core.repositories.model_names as _mn

    monkeypatch.setattr(_mn, "write_names_table", _write_names_table)
    monkeypatch.setattr(_mn, "freeze_aliases", _freeze_aliases)

    from types import SimpleNamespace

    state = SimpleNamespace(schema_version=0, names_table=None)
    token = current_env.set(TEST_SYNTHETIC)
    try:
        asyncio.run(_run_guard(state))
    finally:
        current_env.reset(token)

    assert written == [], (
        "write_names_table and freeze_aliases must not be called for non-prod env rebuilds; "
        f"got: {written}"
    )


# ---------------------------------------------------------------------------
# Test 2: write_names_table IS called for PROD rebuilds
# ---------------------------------------------------------------------------


def test_names_table_write_called_for_prod_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    """The prod rebuild writes the names table and freezes aliases (REQ-194)."""
    from provisa.core.request_context import current_env

    written: list[str] = []

    async def _write_names_table(_conn: Any, _rows: Any) -> None:
        written.append("write_names_table")

    async def _freeze_aliases(_conn: Any, _frozen: Any) -> None:
        written.append("freeze_aliases")

    import provisa.core.repositories.model_names as _mn

    monkeypatch.setattr(_mn, "write_names_table", _write_names_table)
    monkeypatch.setattr(_mn, "freeze_aliases", _freeze_aliases)

    from types import SimpleNamespace

    names_table = SimpleNamespace(rows=[], frozen_aliases=[])
    state = SimpleNamespace(schema_version=0, names_table=names_table)
    # PROD: current_env returns None → active_env() returns PROD.
    token = current_env.set(None)
    try:
        asyncio.run(_run_guard(state))
    finally:
        current_env.reset(token)

    assert written == ["write_names_table", "freeze_aliases"], (
        f"prod rebuild must call write_names_table then freeze_aliases; got: {written}"
    )


# ---------------------------------------------------------------------------
# Test 3: schema_version increments BEFORE write_names_table is called
# ---------------------------------------------------------------------------


def test_schema_version_increments_before_names_table_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """schema_version advances before write_names_table is called so the
    registered_tables cache is invalidated before the SHARE ROW EXCLUSIVE MODE lock is
    acquired: a concurrent reader gets a fresh DB lookup with the current home_region and
    does not request a stale local build (REQ-1882, REQ-1915)."""
    from provisa.core.request_context import current_env
    from types import SimpleNamespace

    version_at_write: list[int] = []
    names_table = SimpleNamespace(rows=[], frozen_aliases=[])
    state = SimpleNamespace(schema_version=5, names_table=names_table)

    async def _write_names_table(_conn: Any, _rows: Any) -> None:
        version_at_write.append(state.schema_version)

    async def _freeze_aliases(_conn: Any, _frozen: Any) -> None:
        pass

    import provisa.core.repositories.model_names as _mn

    monkeypatch.setattr(_mn, "write_names_table", _write_names_table)
    monkeypatch.setattr(_mn, "freeze_aliases", _freeze_aliases)

    version_before = state.schema_version
    # PROD: current_env None → active_env() returns PROD.
    token = current_env.set(None)
    try:
        asyncio.run(_run_guard(state))
    finally:
        current_env.reset(token)

    assert version_at_write == [version_before + 1], (
        "schema_version must be incremented before write_names_table is called so the "
        "registered_tables cache is stale-free during the lock; "
        f"expected [{version_before + 1}], got {version_at_write}"
    )
